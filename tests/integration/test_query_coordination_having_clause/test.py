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
            "INSERT INTO t1_d SELECT n % 50, n % 50, n % 50 FROM (SELECT number as n FROM system.numbers limit 100)")

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
    execute_and_compare("SELECT a, sum(b) FROM t1_d GROUP BY a HAVING sum(b) > 10 ORDER BY a")
    execute_and_compare("SELECT a, sum(b) as sum_b FROM t1_d GROUP BY a HAVING sum_b > 10 ORDER BY a")
    execute_and_compare("SELECT a, sum(b) as sum_b FROM t1_d GROUP BY a HAVING sum_b - 100 > 10 ORDER BY a")


def test_subquery(started_cluster):
    execute_and_compare(
        "SELECT a, sum(b) FROM t1_d GROUP BY a HAVING a in (SELECT a FROM t1_d WHERE b < 10) ORDER BY a",
        additional_settings="distributed_product_mode='allow'")
