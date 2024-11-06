import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 1, "replica": 1},)
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 2, "replica": 1},)
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 3, "replica": 1},)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query("""
            CREATE TABLE t1 ON CLUSTER test_cluster
            (part_key UInt64, value UInt64, order UInt64)
            ENGINE = MergeTree ORDER BY value SETTINGS index_granularity=100;
        """)

        node1.query("""
            CREATE TABLE t1_d ON CLUSTER test_cluster
            (part_key UInt64, value UInt64, order UInt64)
            ENGINE = Distributed(test_cluster, default, t1, rand());
        """)

        yield cluster
    finally:
        cluster.shutdown()


def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text)
    test_result = node1.query(
        query_text + " SETTINGS allow_experimental_query_coordination = 1"
    )
    assert accurate_result == test_result


def test_simple(started_cluster):
    node1.query("""
        INSERT INTO t1_d
        SELECT part_key, value, order
        FROM generateRandom('part_key UInt8, value Int8, order Int8') LIMIT 200
    """)

    node1.query("SYSTEM FLUSH DISTRIBUTED t1_d")

    exec_query_compare_result("""
        SELECT part_key, value, order, groupArray(value) OVER (PARTITION BY part_key ORDER BY value)
        AS frame_values
        FROM t1_d
        ORDER BY part_key ASC, value ASC
    """)