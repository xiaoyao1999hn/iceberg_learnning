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
public class SparkDemo {

    public static void main(String[] args) {
//        SparkSession spark = SparkSession
//                .builder()
//                .master("local")
//                .appName("Iceberg spark example")
//                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
//                .config("spark.sql.catalog.local.type", "hadoop") //指定catalog 类型
//                .config("spark.sql.catalog.local.warehouse", "iceberg_warehouse")
//                .getOrCreate();
//
//        spark.sql("create database iceberg_db");
//        spark.sql("CREATE TABLE local.iceberg_db.table (id bigint, data string) USING iceberg\n");
//        spark.sql("INSERT INTO local.iceberg_db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')");
//        spark.sql("INSERT INTO local.iceberg_db.table VALUES (4, 'd'), (5, 'e'), (6, 'f')");
//        spark.sql("INSERT INTO local.iceberg_db.table VALUES (7, 'g'), (8, 'h'), (9, 'i')");
//        Dataset<Row> result = spark.sql("select * from local.iceberg_db.table");
//        result.show();
//
//        ali.oss.endpoint=http://oss-cn-hangzhou.aliyuncs.com/
//        ali.oss.access-key-id=LTAIN6kxH2XYKBHE
//        ali.oss.access-key-secret=hkHaDmW0tvnhWZvFjLbdu9FtMdiX0r
//        ali.oss.bucket-name=tob-test-oss


//        'spark.sql.catalog.demo', 'org.apache.iceberg.spark.SparkCatalog').config(
//                'spark.sql.catalog.demo.type', 'hadoop').config(
//                'spark.sql.catalog.demo.warehouse', 's3a://bucket/').config(
//                'spark.sql.catalog.demo.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000').config(
//                'spark.sql.catalog.demo.hadoop.fs.s3a.access.key', 'minioadmin').config(
//                'spark.sql.catalog.demo.hadoop.fs.s3a.secret.key', 'minioadmin').getOrCreate()


//        --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
//        --conf spark.sql.catalog.nessie.warehouse=s3://spark-demo1 \
//        --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
//        --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
//        --conf spark.sql.catalog.nessie.uri=http://localhost:19120/api/v1 \
//        --conf spark.sql.catalog.nessie.ref=main \
//        --conf spark.sql.catalog.nessie.cache-enabled=false


//              .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//                .config("hadoop.fs.s3a.committer.name", "directory")
//                .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
//                .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
//                .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
//                .config("spark.hadoop.fs.s3a.path.style.access", "true")
//                .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
//                .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
//                .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
//                .config("spark.hadoop.fs.s3a.fast.upload", value = true)
//                .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
//                .config("spark.sql.parquet.mergeSchema", value = false)
//                .config("spark.sql.parquet.filterPushdown", value = true)
//                .config("spark.sql.shuffle.partitions", 10)
//                .config("spark.default.parallelism", 8)
//                .config("spark.sql.files.maxPartitionBytes", "1g")
//                .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
//                .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table")
//                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
//                .config("spark.sql.catalog.iceberg.type", "hadoop")
//                .config("spark.sql.catalog.iceberg.warehouse", "s3://ccf-datalake-contest/datalake_table")

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Iceberg spark example")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.demo.type", "hadoop")
                .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aliyun.oss.OSSFileIO")
                .config("spark.sql.catalog.demo.hadoop.fs.oss.warehouse", "oss://iceberg_warehouse")

                .config("spark.sql.catalog.demo.hadoop.fs.oss.endpoint", "http://oss-cn-hangzhou.aliyuncs.com/")
                .config("spark.sql.catalog.demo.hadoop.fs.oss.access.key.id", "LTAIN6kxH2XYKBHE")
                .config("spark.sql.catalog.demo.hadoop.fs.oss.access.key.secret", "hkHaDmW0tvnhWZvFjLbdu9FtMdiX0r")
                .getOrCreate();

        spark.sql("create database iceberg_db");
        spark.sql("CREATE TABLE demo.iceberg_db.table (id bigint, data string) USING iceberg\n");
        spark.sql("INSERT INTO demo.iceberg_db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        spark.sql("INSERT INTO demo.iceberg_db.table VALUES (4, 'd'), (5, 'e'), (6, 'f')");
        spark.sql("INSERT INTO demo.iceberg_db.table VALUES (7, 'g'), (8, 'h'), (9, 'i')");
        Dataset<Row> result = spark.sql("select * from demo.iceberg_db.table");
        result.show();

    }
}
