import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 1, "replica": 1}, )
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 1, "replica": 2}, )
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 2, "replica": 1}, )
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 2, "replica": 2}, )
node5 = cluster.add_instance("node5", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 3, "replica": 1}, )
node6 = cluster.add_instance("node6", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 3, "replica": 2}, )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """
            CREATE TABLE t1 ON CLUSTER test_cluster (
                a String,
                b UInt64,
                c UInt64
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/t1/{shard}', '{replica}')
            ORDER BY a
            SETTINGS index_granularity = 1;
            """
        )

        node1.query(
            """
            CREATE TABLE t1_d ON CLUSTER test_cluster (
                a String,
                b UInt64,
                c UInt64
            )
            ENGINE = Distributed(test_cluster, default, t1, rand());
            """
        )


        node1.query(
            "INSERT INTO t1_d SELECT n % 2, n, n FROM (SELECT number as n FROM system.numbers limit 100)")

        node1.query("SYSTEM FLUSH DISTRIBUTED t1_d")
        node1.query("analyze table t1_d")

        yield cluster

    finally:
        cluster.shutdown()


def execute_and_compare(query_text, additional_settings=""):
    settings = " SETTINGS allow_experimental_query_coordination = 0"
    coordination_settings = " SETTINGS allow_experimental_query_coordination = 1"
    if len(additional_settings) != 0:
        settings = settings + ", " + additional_settings
        coordination_settings = coordination_settings + ", " + additional_settings
    result = node1.query(query_text + settings)
    coordination_result = node1.query(query_text + coordination_settings)
    assert result == coordination_result


def test_simple(started_cluster):
    execute_and_compare("""
        WITH 1 AS const_one
        SELECT a, const_one FROM t1_d ORDER BY a LIMIT 100
        """)

    execute_and_compare("""
        WITH toString(b) AS string_b
        SELECT string_b FROM t1_d ORDER BY string_b LIMIT 100
        """)

    execute_and_compare("""
        WITH cte_numbers AS (SELECT b FROM t1_d WHERE b > 3 LIMIT 10)
        SELECT count() FROM cte_numbers
        """)

    execute_and_compare("""
        WITH cte_numbers AS (SELECT b FROM t1_d WHERE b > 3 LIMIT 1000)
        SELECT count()
        FROM t1_d
        WHERE b IN (SELECT b FROM cte_numbers)
        """, additional_settings="distributed_product_mode='allow'")
