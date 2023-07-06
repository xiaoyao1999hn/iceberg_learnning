package org.xiaoyao.iceberg.spark.leanning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Copyright (C) 2020~2099 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * @author chengjie
 * @date 2023/6/8 16:36
 * @desc
 **/
public class IcebergDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Iceberg spark example")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                //指定catalog 类型
                .config("spark.sql.catalog.local.warehouse", "iceberg_warehouse")
                .getOrCreate();
//        spark.sql("create database iceberg_db");
//        spark.sql("CREATE TABLE local.iceberg_db.table_2 (id bigint, data string) USING iceberg PARTITIONED BY (data)\n");
//        spark.sql("INSERT INTO local.iceberg_db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')");
//        spark.sql("INSERT INTO local.iceberg_db.table VALUES (4, 'd'), (5, 'e'), (6, 'f')");
//        spark.sql("INSERT INTO local.iceberg_db.table VALUES (7, 'g'), (8, 'h'), (9, 'i')");

//        StringBuffer data = new StringBuffer();
//        for (int i = 0; i < 100000; i++) {
//            if (i % 2 == 0) {
//                continue;
//            }
//            data.append("(").append(i).append(",'k'),");
//        }
//        data.deleteCharAt(data.lastIndexOf(","));
//        spark.sql("INSERT INTO local.iceberg_db.table_2 VALUES " + data.toString());

//  spark.sql("INSERT INTO local.iceberg_db.table VALUES (10, 'i'), (11, 'i'), (12, 'i')");
//        spark.sql("DELETE FROM local.iceberg_db.table WHERE id=1 and data='x'");

         spark.sql("MERGE INTO local.iceberg_db.table t USING (SELECT * from local.iceberg_db.table_2) s ON t.id=s.id "
                 + "WHEN MATCHED AND t.id > 5000 THEN DELETE \n"
                 + "WHEN MATCHED AND t.id<100 THEN UPDATE SET t.data = 'i' \n");

        //查询表的快照版本信息
//        Dataset<Row> result = spark.sql("CALL local.system.ancestors_of('local.iceberg_db.table')");
        //查询表变更历史信息
//        Dataset<Row> result = spark.sql("SELECT * FROM local.iceberg_db.table.history");
        //查询指定快照的数据（时间旅行必备）
//        Dataset<Row> result = spark.sql("select * from local.iceberg_db.table VERSION AS OF 7152491129159022541");
        //查询表分区信息
//        Dataset<Row> result = spark.sql("SELECT * FROM local.iceberg_db.table.partitions");
        //数据探查
//        Dataset<Row> result = spark.sql("SELECT * FROM local.iceberg_db.table.all_data_files");
        //查询表数据历史记录
//        spark.sql("CALL local.system.create_changelog_view(table => 'local.iceberg_db.table',options => map('start-snapshot-id','4760019567106322226','end-snapshot-id', '7152491129159022541'))");
        //查询表数据变更具体明细
//        Dataset<Row> result = spark.sql("SELECT * FROM table_changes");
        //创建布隆过滤器索引
//        Dataset<Row> result = spark.sql("create index nameIndex on local.iceberg_db.table using bloomfilter (id)");
        //查询表索引信息
//        Dataset<Row> result = spark.sql("SHOW INDEX ON local.iceberg_db.table");
        //数据打tag
//        Dataset<Row> result = spark.sql("ALTER TABLE local.iceberg_db.table CREATE TAG test AS OF VERSION 4760019567106322226 RETAIN 7 DAYS");
//        Dataset<Row> result = spark.sql("SELECT * FROM local.iceberg_db.table");
//        Dataset<Row> result = spark.sql(
//                "CALL local.system.remove_orphan_files(table => 'iceberg_db.table',dry_run => true)");
//        result.show();
    }
}
