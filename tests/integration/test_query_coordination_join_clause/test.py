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
            """
            CREATE TABLE t2 ON CLUSTER test_cluster (
                a String,
                b UInt64,
                c UInt64
            ) 
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/t2/{shard}', '{replica}') 
            ORDER BY a 
            SETTINGS index_granularity = 1;
            """
        )

        node1.query(
            """
            CREATE TABLE t2_d ON CLUSTER test_cluster (
                a String,
                b UInt64,
                c UInt64
            )
            ENGINE = Distributed(test_cluster, default, t2, rand());
            """
        )

        node1.query(
            "INSERT INTO t1_d SELECT toString(n), n, n FROM (SELECT number as n FROM numbers(20))")
        node1.query(
            "INSERT INTO t2_d SELECT toString(n), n, n FROM (SELECT number as n FROM numbers(2, 20))")

        node1.query("SYSTEM FLUSH DISTRIBUTED t1_d")
        node1.query("SYSTEM FLUSH DISTRIBUTED t2_d")

        node1.query("analyze table t1_d")
        node1.query("analyze table t2_d")

        yield cluster

    finally:
        cluster.shutdown()



def execute_and_compare(query_text, additional_settings=""):
    settings = " SETTINGS allow_experimental_query_coordination = 0, distributed_product_mode = 'global'"
    coordination_settings = " SETTINGS allow_experimental_query_coordination = 1"
    if len(additional_settings) != 0:
        settings = settings + ", " + additional_settings
        coordination_settings = coordination_settings + ", " + additional_settings
    result = node1.query(query_text + settings)
    coordination_result = node1.query(query_text + coordination_settings)
    assert result == coordination_result



# SELECT <expr_list>
# FROM <left_table>
# [GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
# (ON <expr_list>)|(USING <column_list>) ...

def test_self_join(started_cluster):
    execute_and_compare(
        "SELECT t1.c, t1.b FROM t1_d as t1 INNER JOIN t1_d as t2 ON t1.a = t2.a AND t1.c = 1 ORDER BY t1.c, t1.b")


def test_inner_join(started_cluster):
    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d INNER ANY JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d INNER ALL JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")


def test_outer_join(started_cluster):
    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d LEFT OUTER JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d LEFT ANY JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d LEFT ALL JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    # The results of right and full join in original ck are wrong

    # execute_and_compare(
    #     "SELECT t1_d.c, t2_d.b FROM t1_d RIGHT OUTER JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    # execute_and_compare(
    #     "SELECT t1_d.c, t2_d.b FROM t1_d RIGHT ANY JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    # execute_and_compare(
    #     "SELECT t1_d.c, t2_d.b FROM t1_d RIGHT ALL JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    # execute_and_compare(
    #     "SELECT t1_d.c, t2_d.b FROM t1_d FULL OUTER JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    # ANY FULL JOINs are not implemented


def test_semi_and_anti(started_cluster):
    # SEMI|ANTI JOIN should be LEFT or RIGHT
    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d LEFT SEMI JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d LEFT ANTI JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d RIGHT SEMI JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")

    # The result of original ck is wrong
    # execute_and_compare(
    #     "SELECT t1_d.c, t2_d.b FROM t1_d RIGHT ANTI JOIN t2_d ON t1_d.a = t2_d.a ORDER BY t1_d.c, t2_d.b")


def test_asof_join(started_cluster):
    # Only ASOF and LEFT ASOF joins are supported
    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d INNER ASOF JOIN t2_d ON t1_d.a = t2_d.a AND t1_d.b > t2_d.b ORDER BY t1_d.c, t2_d.b")

    execute_and_compare(
        "SELECT t1_d.c, t2_d.b FROM t1_d LEFT ASOF JOIN t2_d ON t1_d.a = t2_d.a AND t1_d.b > t2_d.b ORDER BY t1_d.c, t2_d.b")


def test_cross_join(started_cluster):
    execute_and_compare("SELECT t1_d.c, t2_d.b FROM t1_d CROSS JOIN t2_d ORDER BY t1_d.c, t2_d.b")
