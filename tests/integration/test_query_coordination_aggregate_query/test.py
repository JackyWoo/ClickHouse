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
            """CREATE TABLE t2 ON CLUSTER test_cluster (id UInt32, val String, str String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t2', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE t2_d ON CLUSTER test_cluster (id UInt32, val String, str String) ENGINE = Distributed(test_cluster, default, t2, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_query(started_cluster):
    node1.query("INSERT INTO t1_d SELECT id,'113','bsada' FROM generateRandom('id Int') LIMIT 2133")

    node1.query("INSERT INTO t1_d SELECT id,'111','dsada' FROM generateRandom('id Int') LIMIT 213")
    node1.query("INSERT INTO t1_d SELECT id,'222','dqada' FROM generateRandom('id Int') LIMIT 253")
    node1.query("INSERT INTO t1_d SELECT id,'333','dwada' FROM generateRandom('id Int') LIMIT 309")
    node1.query("INSERT INTO t1_d SELECT id,'444','deada' FROM generateRandom('id Int') LIMIT 204")
    node1.query("INSERT INTO t1_d SELECT id,'555','drada' FROM generateRandom('id Int') LIMIT 273")
    node1.query("INSERT INTO t1_d SELECT id,'666','dtada' FROM generateRandom('id Int') LIMIT 293")
    node1.query("INSERT INTO t1_d SELECT id,'777','dyada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO t1_d SELECT id,'888','duada' FROM generateRandom('id Int') LIMIT 233")
    node1.query("INSERT INTO t1_d SELECT id,'999','diada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO t1_d SELECT id,'100','doada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'101','dpada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'102','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'103','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'104','dfada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'105','dgada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'106','dhada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'107','djada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'108','dkada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t1_d SELECT id,'109','dlada' FROM generateRandom('id Int') LIMIT 100")
    node1.query("INSERT INTO t1_d SELECT id,'110','dpada' FROM generateRandom('id Int') LIMIT 910")
    node1.query("INSERT INTO t1_d SELECT id,'211','dxada' FROM generateRandom('id Int') LIMIT 212")
    node1.query("INSERT INTO t1_d SELECT id,'212','dcada' FROM generateRandom('id Int') LIMIT 103")
    node1.query("INSERT INTO t1_d SELECT id,'213','dvada' FROM generateRandom('id Int') LIMIT 211")

    node1.query("INSERT INTO t2_d SELECT id,'913','bsada' FROM generateRandom('id Int') LIMIT 233")

    node1.query("INSERT INTO t2_d SELECT id,'911','doada' FROM generateRandom('id Int') LIMIT 213")
    node1.query("INSERT INTO t2_d SELECT id,'322','dpada' FROM generateRandom('id Int') LIMIT 253")
    node1.query("INSERT INTO t2_d SELECT id,'433','dwada' FROM generateRandom('id Int') LIMIT 309")
    node1.query("INSERT INTO t2_d SELECT id,'544','deada' FROM generateRandom('id Int') LIMIT 204")
    node1.query("INSERT INTO t2_d SELECT id,'655','dhada' FROM generateRandom('id Int') LIMIT 273")
    node1.query("INSERT INTO t2_d SELECT id,'766','dcada' FROM generateRandom('id Int') LIMIT 293")
    node1.query("INSERT INTO t2_d SELECT id,'877','dyada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO t2_d SELECT id,'888','dmada' FROM generateRandom('id Int') LIMIT 233")
    node1.query("INSERT INTO t2_d SELECT id,'999','dvada' FROM generateRandom('id Int') LIMIT 223")
    node1.query("INSERT INTO t2_d SELECT id,'900','doada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'901','dcada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'902','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'903','dsada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'904','dlada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'905','dgada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'106','dhtda' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'107','djada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'108','dkada' FROM generateRandom('id Int') LIMIT 203")
    node1.query("INSERT INTO t2_d SELECT id,'109','dlada' FROM generateRandom('id Int') LIMIT 100")
    node1.query("INSERT INTO t2_d SELECT id,'110','dpada' FROM generateRandom('id Int') LIMIT 90")
    node1.query("INSERT INTO t2_d SELECT id,'211','dzada' FROM generateRandom('id Int') LIMIT 22")
    node1.query("INSERT INTO t2_d SELECT id,'212','dcada' FROM generateRandom('id Int') LIMIT 103")
    node1.query("INSERT INTO t2_d SELECT id,'213','dvada' FROM generateRandom('id Int') LIMIT 21")

    node1.query("SYSTEM FLUSH DISTRIBUTED t1_d")
    node1.query("SYSTEM FLUSH DISTRIBUTED t2_d")

    node1.query("analyze table t1")
    node1.query("analyze table t2")
    node2.query("analyze table t1")
    node2.query("analyze table t2")
    node3.query("analyze table t1")
    node3.query("analyze table t2")
    node4.query("analyze table t1")
    node4.query("analyze table t2")
    node5.query("analyze table t1")
    node5.query("analyze table t2")
    node6.query("analyze table t1")
    node6.query("analyze table t2")

    r = node1.query(
        "EXPLAIN SELECT id,any(val) FROM t1_d GROUP BY id LIMIT 2 SETTINGS allow_experimental_query_coordination = 1, statistics_agg_full_cardinality_coefficient=0.01, group_by_two_level_threshold=100, group_by_two_level_threshold_bytes=1000")

    r = node1.query(
        "EXPLAIN pipeline SELECT id,any(val) FROM t1_d GROUP BY id ORDER BY id SETTINGS allow_experimental_query_coordination = 1")

    rr = node1.query(
        "SELECT id,any(val) FROM t1_d GROUP BY id ORDER BY id SETTINGS allow_experimental_query_coordination = 1, group_by_two_level_threshold=100, group_by_two_level_threshold_bytes=1000")

    r = node1.query(
        "SELECT sum(id),val FROM t1_d GROUP BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),val FROM t1_d GROUP BY val")

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val")

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1, group_by_two_level_threshold=1, group_by_two_level_threshold_bytes=1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY val")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY name,val")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val  with totals ORDER BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val  with totals ORDER BY name,val")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val with rollup ORDER BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val with rollup ORDER BY name,val")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val with cube ORDER BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val with cube ORDER BY name,val")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY GROUPING SETS((name,val),(name),(val),()) ORDER BY name,val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY GROUPING SETS((name,val),(name),(val),()) ORDER BY name,val")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val with totals ORDER BY name,val SETTINGS extremes=1,allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val with totals ORDER BY name,val SETTINGS extremes=1")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY name,val LIMIT 10 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY name,val LIMIT 10 ")

    assert r == rr

    r = node1.query(
        "SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY name,val LIMIT 11,3 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT sum(id),name,val FROM t1_d GROUP BY name,val ORDER BY name,val LIMIT 11,3 ")

    assert r == rr

    r = node1.query(
        "SELECT sum(id) ids,name,val FROM t1_d GROUP BY name,val HAVING ids > 999900000 ORDER BY name,val LIMIT 10 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "SELECT sum(id) ids,name,val FROM t1_d GROUP BY name,val HAVING ids > 999900000 ORDER BY name,val LIMIT 10 ")

    assert r == rr

    r = node1.query(
        "SELECT uniq(id),val FROM t1_d GROUP BY val SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query("SELECT uniq(id),val FROM t1_d GROUP BY val")

    r = node1.query(
        "SELECT * FROM t1_d a join t2_d b on a.name=b.str order by a.id,a.val,a.name,b.id,b.val,b.str limit 30 SETTINGS allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "SELECT * FROM t1_d a GLOBAL join t2_d b on a.name=b.str order by a.id,a.val,a.name,b.id,b.val,b.str limit 30")

    assert r == rr

    # IN subquery

    r = node1.query(
        "select * from t1_d where val in (select val from t2_d where str like '%d%') order by id limit 13 SETTINGS use_index_for_in_with_subqueries=0, allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "select * from t1_d where val GLOBAL in (select val from t2_d where str like '%d%') order by id limit 13 ")

    assert r == rr

    r = node1.query(
        "select * from t1_d where val in (select val from t2_d where str like '%d%') or val in (select val from t2_d where str like '%v%') order by id limit 13  SETTINGS use_index_for_in_with_subqueries=0, allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "select * from t1_d where val GLOBAL in (select val from t2_d where str like '%d%') or val GLOBAL in (select val from t2_d where str like '%v%') order by id limit 13 ")

    assert r == rr

    r = node1.query(
        "select * from t1_d where val in (select val from t2_d where str like '%d%') or val in (select val from t2_d where str like '%v%') order by id limit 13 SETTINGS use_index_for_in_with_subqueries=0, allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "select * from t1_d where val GLOBAL in (select val from t2_d where str like '%d%') or val GLOBAL in (select val from t2_d where str like '%v%') order by id limit 13 ")

    assert r == rr

    r = node1.query(
        "select * from t1_d where val in (select val from t2_d where str like '%d%' or val in (select val from t1_d)) or val in (select val from t2_d where str like '%v%') order by id limit 13 SETTINGS use_index_for_in_with_subqueries=0, allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "select * from t1_d where val GLOBAL in (select val from t2_d where str like '%d%' or val GLOBAL in (select val from t1_d)) or val GLOBAL in (select val from t2_d where str like '%v%') order by id limit 13")

    assert r == rr

    r = node1.query(
        "select uniq(id) ids,name from (select sum(id) as id,name,val from t1_d group by name,val order by id limit 6) where val in (select val from t2_d where str like '%d%' or val in (select val from t1_d where name like '%s%')) and val in (select val from t2_d where str like '%a%') group by name order by ids,name limit 4 settings use_index_for_in_with_subqueries=0,allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "select uniq(id) ids,name from (select sum(id) as id,name,val from t1_d group by name,val order by id limit 6) where val GLOBAL in (select val from t2_d where str like '%d%' or val GLOBAL in (select val from t1_d where name like '%s%')) and val GLOBAL in (select val from t2_d where str like '%a%') group by name order by ids,name limit 4")

    assert r == rr

    r = node1.query(
        "select uniq(id) ids,name from (select a.id as id,a.val as val,a.name as name from t1_d a join t2_d b on a.name=b.str order by val,name limit 100000) where val in (select val from t2_d where str not like '%d%' or val in (select val from t1_d where name not like '%s%')) or val in (select val from t2_d where str not like '%a%') group by name order by ids,name limit 21 settings use_index_for_in_with_subqueries=0,allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")

    rr = node1.query(
        "select uniq(id) ids,name from (select t1_d.id as id,t1_d.val as val,t1_d.name as name from t1_d GLOBAL join t2_d on t1_d.name=t2_d.str order by val,name limit 100000) where val GLOBAL in (select val from t2_d where str not like '%d%' or val GLOBAL in (select val from t1_d where name not like '%s%')) or val GLOBAL in (select val from t2_d where str not like '%a%') group by name order by ids,name limit 21")

    assert r == rr

    # print("local table select:")
    # r = node1.query("select uniq(id) ids,name from (select t1_d.id as id,t1_d.val as val,t1_d.name as name from t1_d join t2_d on t1_d.name=t2_d.str order by val,name limit 100000) where val in (select val from t2_d where str not like '%d%' or val in (select val from t1_d where name not like '%s%')) or val in (select val from t2_d where str not like '%a%') group by name order by ids,name limit 21 settings use_index_for_in_with_subqueries=0,allow_experimental_query_coordination = 1, allow_experimental_analyzer = 1")
    # print(r)
    #
    # print("distribute table select:")
    # rr = node1.query("select uniq(id) ids,name from (select t1_d.id as id,t1_d.val as val,t1_d.name as name from t1_d GLOBAL join t2_d on t1_d.name=t2_d.str order by val,name limit 100000) where val GLOBAL in (select val from t2_d where str not like '%d%' or val GLOBAL in (select val from t1_d where name not like '%s%')) or val GLOBAL in (select val from t2_d where str not like '%a%') group by name order by ids,name limit 21")
    # print(rr)
    #
    # assert r == rr
