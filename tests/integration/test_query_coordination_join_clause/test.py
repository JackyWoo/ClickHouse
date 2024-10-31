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
            """CREATE TABLE t1 ON CLUSTER test_cluster (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t1', '{replica}') ORDER BY id SETTINGS index_granularity=2;"""
        )

        node1.query(
            """CREATE TABLE t1_d ON CLUSTER test_cluster (id UInt32, val String, name String) ENGINE = Distributed(test_cluster, default, t1, rand());"""
        )

        node1.query(
            """CREATE TABLE t2 ON CLUSTER test_cluster (id UInt32, text String, scores UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t2', '{replica}') ORDER BY id SETTINGS index_granularity=2;"""
        )

        node1.query(
            """CREATE TABLE t2_d ON CLUSTER test_cluster (id UInt32, text String, scores UInt32) ENGINE = Distributed(test_cluster, default, t2, rand());"""
        )

        insert_data()

        yield cluster

    finally:
        cluster.shutdown()


def insert_data():
    node1.query("INSERT INTO t1_d SELECT number % 2, toString(number % 2), toString(number % 2) FROM numbers(10)")
    node1.query("SYSTEM FLUSH DISTRIBUTED t1_d")

    node1.query("INSERT INTO t2_d SELECT number % 4, toString(number % 4), number % 4 FROM numbers(10)")
    node1.query("SYSTEM FLUSH DISTRIBUTED t2_d")


def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text + " SETTINGS distributed_product_mode = 'global'")
    test_result = node1.query(query_text + " SETTINGS allow_experimental_query_coordination = 1")

    print("accurate_result: " + accurate_result)
    print("test_result: " + test_result)

    assert accurate_result == test_result


# SELECT <expr_list>
# FROM <left_table>
# [GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
# (ON <expr_list>)|(USING <column_list>) ...

def test_self_join(started_cluster):
    exec_query_compare_result(
        "SELECT t1.name, t1.val FROM t1_d as t1 INNER JOIN t1_d as t2 ON t1.id = t2.id AND t1.name = '1' ORDER BY t1.name")


def test_inner_join(started_cluster):
    exec_query_compare_result(
        "SELECT name, text FROM t1_d INNER ANY JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d INNER ALL JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")


def test_outer_join(started_cluster):
    exec_query_compare_result(
        "SELECT name, text FROM t1_d RIGHT OUTER JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d RIGHT ANY JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d RIGHT ALL JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    # ANY FULL JOINs are not implemented
    exec_query_compare_result(
        "SELECT name, text FROM t1_d FULL OUTER JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d LEFT ANY JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d LEFT ALL JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")


def test_semi_and_anti(started_cluster):
    # SEMI|ANTI JOIN should be LEFT or RIGHT
    exec_query_compare_result(
        "SELECT name, text FROM t1_d LEFT OUTER JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d LEFT SEMI JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d LEFT ANTI JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d RIGHT SEMI JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d RIGHT ANTI JOIN t2_d ON t1_d.id = t2_d.id AND t2_d.text = '1' ORDER BY name, text")


def test_asof_join(started_cluster):
    # Only ASOF and LEFT ASOF joins are supported
    exec_query_compare_result(
        "SELECT name, text FROM t1_d INNER ASOF JOIN t2_d ON t1_d.id > t2_d.id AND t1_d.val = t2_d.text ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d LEFT ASOF JOIN t2_d ON t1_d.id > t2_d.id AND t1_d.val = t2_d.text ORDER BY name, text")


def test_cross_join(started_cluster):
    exec_query_compare_result("SELECT name, text FROM t1_d CROSS JOIN t2_d ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM t1_d CROSS JOIN t2_d WHERE t1_d.val = t2_d.text ORDER BY name, text")
