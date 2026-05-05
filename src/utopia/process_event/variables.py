from dataclasses import dataclass
from enum import Enum


class Env(Enum):
    LOCAL = "local"
    PROD = "prod"


@dataclass(frozen=True)
class PipelineConfig:
    dedup_key: str = "detection_oid"
    item_key: str = "item_name"
    location_oid_key: str = "geographical_location_oid"
    location_dim_oid_key: str = "geographical_location_oid"
    location_name_key: str = "geographical_location"
    output_location_col: str = "geographical_location"
    output_rank_col: str = "item_rank"
    output_item_col: str = "item_name"


DEFAULT_CONFIG = PipelineConfig()
