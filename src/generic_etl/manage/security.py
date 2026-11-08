"""Subsystem 32 – Security System.

Role-based access to staging and presentation tables, column-level
protection and encryption:

* declarative **role policies** rendered to Unity Catalog ``GRANT`` /
  ``REVOKE`` statements (optionally executed)
* **column masks** and **row filters** (UC SQL functions) and DataFrame
  masking helpers (hash, redact, partial)
* **encryption** of sensitive columns with AES-GCM (``aes_encrypt``), keys
  read from Databricks secret scopes
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..core.context import EtlContext

PRIVILEGES = {"SELECT", "MODIFY", "READ_VOLUME", "WRITE_VOLUME", "USE_SCHEMA", "USE_CATALOG", "CREATE_TABLE",
              "ALL_PRIVILEGES", "EXECUTE", "APPLY_TAG", "MANAGE"}


@dataclass
class RolePolicy:
    """Grant ``privileges`` on ``securables`` (``TABLE x``, ``SCHEMA y``...) to ``principal``."""

    principal: str
    privileges: Sequence[str]
    securables: Sequence[str]


def mask_column(col: Column, how: str = "redact", keep: int = 4, salt: str = "") -> Column:
    """``hash`` (SHA-256), ``redact`` (``****``), ``partial`` (keep last ``keep`` chars), ``null``."""
    s = col.cast("string")
    if how == "hash":
        return F.sha2(F.concat(F.lit(salt), s), 256)
    if how == "redact":
        return F.when(col.isNull(), None).otherwise(F.lit("****"))
    if how == "partial":
        return F.regexp_replace(s, f".(?=.{{{int(keep)}}})", "*")
    if how == "email":
        return F.regexp_replace(s, r"(^.)[^@]*(@.*$)", "$1***$2")
    if how == "null":
        return F.lit(None).cast("string")
    raise ValueError(f"unknown masking method {how}")


class SecurityManager:
    def __init__(self, ctx: EtlContext, secret_scope: Optional[str] = None) -> None:
        self.ctx = ctx
        self.secret_scope = secret_scope

    # ------------------------------------------------------------ RBAC
    @staticmethod
    def grant_statements(policies: Sequence[RolePolicy], revoke: bool = False) -> List[str]:
        stmts = []
        for p in policies:
            privs = [x.upper().replace(" ", "_") for x in p.privileges]
            bad = set(privs) - PRIVILEGES
            if bad:
                raise ValueError(f"unknown privileges {bad}")
            priv_sql = ", ".join(x.replace("_", " ") for x in privs)
            for sec in p.securables:
                if revoke:
                    stmts.append(f"REVOKE {priv_sql} ON {sec} FROM `{p.principal}`")
                else:
                    stmts.append(f"GRANT {priv_sql} ON {sec} TO `{p.principal}`")
        return stmts

    def apply_policies(self, policies: Sequence[RolePolicy], revoke: bool = False, dry_run: bool = False) -> List[str]:
        stmts = self.grant_statements(policies, revoke)
        if not dry_run:
            for s in stmts:
                self.ctx.spark.sql(s)
        return stmts

    @staticmethod
    def column_mask_statements(table: str, column: str, function_name: str, allowed_group: str,
                               method: str = "redact") -> List[str]:
        """Unity Catalog column mask: only ``allowed_group`` sees clear values."""
        masked = {"redact": "'****'", "hash": "sha2(CAST(val AS STRING), 256)", "null": "NULL"}[method]
        return [
            f"CREATE OR REPLACE FUNCTION {function_name}(val STRING) RETURN "
            f"CASE WHEN is_account_group_member('{allowed_group}') THEN val ELSE {masked} END",
            f"ALTER TABLE {table} ALTER COLUMN `{column}` SET MASK {function_name}",
        ]

    @staticmethod
    def row_filter_statements(table: str, column: str, function_name: str,
                              group_values: Dict[str, Sequence[str]], admin_group: Optional[str] = None) -> List[str]:
        """Unity Catalog row filter: each group sees only its ``column`` values."""
        cases = []
        if admin_group:
            cases.append(f"WHEN is_account_group_member('{admin_group}') THEN TRUE")
        for grp, vals in group_values.items():
            in_list = ", ".join(f"'{v}'" for v in vals)
            cases.append(f"WHEN is_account_group_member('{grp}') THEN val IN ({in_list})")
        body = "CASE " + " ".join(cases) + " ELSE FALSE END"
        return [f"CREATE OR REPLACE FUNCTION {function_name}(val STRING) RETURN {body}",
                f"ALTER TABLE {table} SET ROW FILTER {function_name} ON (`{column}`)"]

    # --------------------------------------------------- DataFrame level
    @staticmethod
    def mask(df: DataFrame, rules: Dict[str, str], salt: str = "") -> DataFrame:
        for c, how in rules.items():
            df = df.withColumn(c, mask_column(F.col(c), how, salt=salt))
        return df

    def get_key(self, key_name: str) -> str:
        """Fetch an encryption key from a Databricks secret scope (or env var)."""
        import os

        if self.secret_scope:
            try:
                from pyspark.dbutils import DBUtils  # type: ignore

                return DBUtils(self.ctx.spark).secrets.get(self.secret_scope, key_name)
            except ImportError:
                pass
        env = os.environ.get(key_name)
        if env:
            return env
        raise KeyError(f"encryption key {key_name} not found")

    @staticmethod
    def encrypt(df: DataFrame, columns: Sequence[str], key: str) -> DataFrame:
        """AES-GCM encrypt (base64 output). Key must be 16, 24 or 32 bytes."""
        if len(key.encode()) not in (16, 24, 32):
            raise ValueError("AES key must be 16, 24 or 32 bytes")
        for c in columns:
            df = df.withColumn(c, F.base64(F.expr(f"aes_encrypt(CAST(`{c}` AS STRING), '{key}', 'GCM')")))
        return df

    @staticmethod
    def decrypt(df: DataFrame, columns: Sequence[str], key: str) -> DataFrame:
        for c in columns:
            df = df.withColumn(c, F.expr(f"CAST(aes_decrypt(unbase64(`{c}`), '{key}', 'GCM') AS STRING)"))
        return df
