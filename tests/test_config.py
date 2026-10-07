import pytest

from tpch_marketing.config import (
    ALLOWED_MARKET_SEGMENTS,
    SOURCE_TABLES,
    LayerNames,
    sql_in_list,
    validate_source,
)


def test_layer_names():
    n = LayerNames("workspace", "dev")
    assert n.bronze == "workspace.dev_tpch_mkt_bronze"
    assert n.gold == "workspace.dev_tpch_mkt_gold"


def test_rejects_injection():
    with pytest.raises(ValueError):
        LayerNames("workspace; DROP", "dev")
    with pytest.raises(ValueError):
        validate_source("samples.tpch; --")


def test_unknown_layer():
    with pytest.raises(ValueError):
        LayerNames("c", "dev").schema("platinum")


def test_sql_in_list_escapes():
    assert sql_in_list(("A", "B'C")) == "'A', 'B''C'"


def test_segments_and_tables():
    assert len(ALLOWED_MARKET_SEGMENTS) == 5
    assert len(SOURCE_TABLES) == 8
    # parents are loaded before children
    assert SOURCE_TABLES.index("customer") < SOURCE_TABLES.index("orders")
    assert SOURCE_TABLES.index("partsupp") < SOURCE_TABLES.index("lineitem")
