"""Category 3 – Delivering (subsystems 9-21)."""
from .aggregates import AggregateBuilder, AggregateDefinition, DimensionRollup, choose_aggregate
from .bridge import BridgeTableBuilder
from .dimension_manager import DimensionManager
from .fact_provider import FactLoadResult, FactTableProvider, StaleDimensionError
from .facts import AccumulatingSnapshotFactBuilder, PeriodicSnapshotFactBuilder, TransactionFactBuilder
from .hierarchy import HierarchyCycleError, HierarchyManager
from .key_pipeline import DimensionLookup, KeyPipelineResult, ReferentialIntegrityError, SurrogateKeyPipeline
from .late_arriving import LateArrivingDimensionHandler, LateArrivingFactHandler
from .olap import CubeBuilder, CubeDimension, CubeMeasure, CubeModel
from .propagation import DataPropagationManager, ExportSpec, dataset_checksum
from .scd import SCDConfig, SCDManager, SCDResult
from .special_dims import (
    Band,
    JunkDimensionBuilder,
    MiniDimensionBuilder,
    build_date_dimension,
    build_shrunken_dimension,
    build_time_dimension,
)
from .surrogate_keys import (
    LATE_ARRIVING_KEY,
    NOT_APPLICABLE_KEY,
    SPECIAL_KEYS,
    UNKNOWN_KEY,
    SurrogateKeyGenerator,
    default_rows,
    ensure_default_rows,
)

__all__ = [
    "SCDConfig", "SCDManager", "SCDResult",
    "SurrogateKeyGenerator", "default_rows", "ensure_default_rows",
    "UNKNOWN_KEY", "NOT_APPLICABLE_KEY", "LATE_ARRIVING_KEY", "SPECIAL_KEYS",
    "HierarchyManager", "HierarchyCycleError",
    "build_date_dimension", "build_time_dimension", "JunkDimensionBuilder", "MiniDimensionBuilder", "Band",
    "build_shrunken_dimension",
    "TransactionFactBuilder", "PeriodicSnapshotFactBuilder", "AccumulatingSnapshotFactBuilder",
    "SurrogateKeyPipeline", "DimensionLookup", "KeyPipelineResult", "ReferentialIntegrityError",
    "BridgeTableBuilder",
    "LateArrivingDimensionHandler", "LateArrivingFactHandler",
    "DimensionManager",
    "FactTableProvider", "FactLoadResult", "StaleDimensionError",
    "AggregateBuilder", "AggregateDefinition", "DimensionRollup", "choose_aggregate",
    "CubeBuilder", "CubeModel", "CubeDimension", "CubeMeasure",
    "DataPropagationManager", "ExportSpec", "dataset_checksum",
]
