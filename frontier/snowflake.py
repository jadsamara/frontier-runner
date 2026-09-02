"""Snowflake warehouse adapter.

Core frontier logic must import `WarehouseAdapter` from `frontier.warehouse`
instead of this module or `snowflake.connector`.
"""

from frontier.adapters.snowflake import (
    SnowflakeConnectConfig,
    SnowflakeWarehouse,
    describe_connection,
    load_snowflake_config,
    open_warehouse,
)
from frontier.warehouse import FakeWarehouse, WarehouseAdapter as Warehouse

__all__ = [
    "FakeWarehouse",
    "SnowflakeConnectConfig",
    "SnowflakeWarehouse",
    "Warehouse",
    "describe_connection",
    "load_snowflake_config",
    "open_warehouse",
]
