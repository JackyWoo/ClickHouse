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
            """CREATE TABLE t1 ON CLUSTER test_cluster (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t1', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE t1_d ON CLUSTER test_cluster (id UInt32, val String, name String) ENGINE = Distributed(test_cluster, default, t1, rand());"""
        )

        node1.query(
            """CREATE TABLE t2 ON CLUSTER test_cluster (id UInt32, text String, scores UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t2', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE t2_d ON CLUSTER test_cluster (id UInt32, text String, scores UInt32) ENGINE = Distributed(test_cluster, default, t2, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text)
    test_result = node1.query(query_text + " SETTINGS allow_experimental_query_coordination = 1")

    print(accurate_result)
    print(test_result)
    assert accurate_result == test_result


def test_query(started_cluster):
    node1.query("INSERT INTO t1_d SELECT id,'123','test' FROM generateRandom('id Int16') LIMIT 18")
    node1.query("INSERT INTO t1_d SELECT id,'234','test1' FROM generateRandom('id Int16') LIMIT 20")

    node1.query("INSERT INTO t2_d SELECT id,'123',10 FROM generateRandom('id Int16') LIMIT 20")
    node1.query("INSERT INTO t2_d SELECT id,'234',12 FROM generateRandom('id Int16') LIMIT 25")

    node1.query("SYSTEM FLUSH DISTRIBUTED t1_d")
    node1.query("SYSTEM FLUSH DISTRIBUTED t2_d")

    exec_query_compare_result(
        "SELECT * FROM (SELECT id, val, name FROM t1_d UNION ALL SELECT id, text, toString(scores) FROM t2_d) ORDER BY id, val, name")

    exec_query_compare_result(
        "SELECT * FROM (SELECT id, val, name FROM t1_d UNION ALL SELECT id, text, toString(scores) FROM t2_d UNION ALL SELECT id, 'test_union', toString(scores) FROM t2_d) ORDER BY id, val, name")
