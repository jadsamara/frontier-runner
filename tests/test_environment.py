from frontier.environment import ENVIRONMENT_MISMATCH, assess_artifact_environment


def test_shared_sample_sources_are_not_an_environment_mismatch() -> None:
    check = assess_artifact_environment(
        base_sql="select o_custkey as customer_id from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS",
        pr_sql="select o_custkey as customer_id from DATA_AGENT_DEV.FRONTIER_CDC.ORDERS",
        base_database="DATA_AGENT_DEV",
        pr_database="DATA_AGENT_DEV",
        profile_database="DATA_AGENT_DEV",
    )
    assert check.ok is True
    check = assess_artifact_environment(
        base_sql="select customer_id from FRONTIER_TEST.DBT_DEV.STG_ORDERS",
        pr_sql="select customer_id from FRONTIER_TEST.DBT_DEV.STG_ORDERS where status = 'F'",
        candidate_sql="select customer_id from FRONTIER_TEST.DBT_DEV.STG_ORDERS",
        base_database="FRONTIER_TEST",
        pr_database="FRONTIER_TEST",
        profile_database="FRONTIER_TEST",
    )
    assert check.ok is True
    assert check.code is None


def test_base_and_head_databases_disagree() -> None:
    check = assess_artifact_environment(
        base_sql="select 1 from FRONTIER_LAB.DBT_DEV.ORDERS",
        pr_sql="select 1 from FRONTIER_TEST.DBT_DEV.ORDERS",
        profile_database="FRONTIER_TEST",
    )
    assert check.ok is False
    assert check.code == ENVIRONMENT_MISMATCH
    assert "base and head" in (check.reason or "")


def test_artifact_database_disagrees_with_profile() -> None:
    check = assess_artifact_environment(
        base_sql="select 1 from FRONTIER_LAB.DBT_DEV.ORDERS",
        pr_sql="select 1 from FRONTIER_LAB.DBT_DEV.ORDERS",
        candidate_sql="select customer_id from FRONTIER_LAB.DBT_DEV.ORDERS",
        profile_database="FRONTIER_TEST",
    )
    assert check.ok is False
    assert check.code == ENVIRONMENT_MISMATCH
    assert "FRONTIER_LAB" in (check.reason or "")
    assert "FRONTIER_TEST" in (check.reason or "")
