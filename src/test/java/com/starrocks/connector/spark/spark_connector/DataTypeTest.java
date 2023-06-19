package com.starrocks.connector.spark.spark_connector;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import com.alibaba.fastjson2.JSON;
import scala.Serializable;
import java.math.BigDecimal;

import org.apache.spark.sql.SparkSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;
import java.util.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.apache.commons.configuration.Configuration;  
import org.apache.commons.configuration.ConfigurationException;  
import org.apache.commons.configuration.PropertiesConfiguration;  

public class DataTypeTest {
//    private static final Logger LOG = LogManager.getLogger(DataTypeTest.class);

    static String FE_IP = null;
    static String FE_QUERYPORT = null;
    static String FE_HTTPPORT = null;
    static String USER = null;
    static String PASS = null;
    static String JDBC_DRIVER = null;
    static String DB_URL = null;
    static String DATABASE = null;
    static String KAFKA_IP_PORT = null;
    static String SQL_FILE_PATH = null;
    static Connection conn = null;
    static Statement stmt = null;
    static SQLContext sqlContext = null;
    static SparkSession spark = null;

    @BeforeClass
    public static void prepare() {

        try {
            Configuration config = new PropertiesConfiguration("src/test/java/com/starrocks/connector/spark/spark_connector/conf/conf.properties");     
             FE_IP = config.getString("fe_host");
             FE_QUERYPORT = config.getString("fe_query_port");
             FE_HTTPPORT = config.getString("fe_http_port");
             USER = config.getString("sr_user");
             PASS = config.getString("sr_password");
             JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
             DB_URL = String.format("jdbc:mysql://%s:%s/information_schema", FE_IP, FE_QUERYPORT);
             DATABASE = "spark_connector_datatype" + UUID.randomUUID().toString().replace("-", "").toLowerCase();
             KAFKA_IP_PORT = "172.26.194.239:9092";
             SQL_FILE_PATH = "src/test/java/com/starrocks/connector/spark/spark_connector/sql/";


            } catch (Exception e) {
                e.printStackTrace();
            }
        // create database
        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);
            // open db_url
            System.out.println("connect database");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // create database
        executeSql(stmt,String.format("create database %s", DATABASE),"", false, false);
        executeSql(stmt,String.format("use %s", DATABASE),"", false, false);

        // init sparksession and sqlContext
        spark = SparkSession
            .builder()
            .master("local")
            .appName("Application")
            .config("spark.url.config", "some-value")
            .getOrCreate();
        sqlContext = new SQLContext(spark);
    }

    @AfterClass
    public static void drop_object() {
        // drop database
        executeSql(stmt,String.format("drop database %s", DATABASE),"", false, false);
        // close
        try {
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void supportDataBasicTypeCsvKafkaBatch(){
        /*
         * 测试从kafka中导入基础数据类型的csv格式数据到starrocks中, batch
         */
        try {
            // create table 
            String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
            for (String tableName : tableNames) {
                String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
                executeSql(stmt, createTableSql, "", false, false);

                Dataset<Row> df = sqlContext.read()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
                        .option("subscribe", "topic_kafka_spark_csv")
                        .load();
                df.show();
                df.selectExpr("CAST(value AS string)").show();

                Map<String, String> options = new HashMap<>();

                options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
                options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
                options.put("spark.starrocks.database", DATABASE);
                options.put("spark.starrocks.table", tableName);
                options.put("spark.starrocks.username", USER);
                options.put("spark.starrocks.password", PASS);
                options.put("spark.starrocks.write.properties.format", "csv");
                options.put("spark.starrocks.write.ctl.enable-transaction", "false");
                options.put("spark.starrocks.infer.columns", "value");
                options.put("spark.starrocks.infer.column.value.type", "string");

                df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

                executeSql(stmt, "select count(1) from " + tableName, "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
                "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
                "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
                "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
                true,
                false);

                executeSql(stmt, "drop table " + tableName, "", false, false);

                }
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }

    @Test
    public void supportDataBasicTypeCsvKafkaStream(){
        /*
         * 测试从kafka中导入基础数据类型的csv格式数据到starrocks中, stream
         */
        SparkSession sparkSession = SparkSession
        .builder()
        .master("local")
        .appName("Application")
        .getOrCreate();

        sparkSession.conf().set("spark.sql.streaming.checkpointLocation", "/home/disk4/huoyue//tmp/spark/");
        sparkSession.conf().set("spark.default.parallelism", 2);
        try {
            // create table 
            String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
            for (String tableName : tableNames) {
                String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
                executeSql(stmt, createTableSql, "", false, false);

                Dataset<Row> df = sqlContext.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
                        .option("subscribe", "topic_kafka_spark_csv")
                        .option("startingOffsets", "earliest")
                        .option("checkpointLocation", "/home/disk1/xiaolingfeng/tmp/spark/")
                        .option("maxOffsetsPerTrigger", 1000000)
                        .load();

                Map<String, String> options = new HashMap<>();

                options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
                options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
                options.put("spark.starrocks.database", DATABASE);
                options.put("spark.starrocks.table", tableName);
                options.put("spark.starrocks.username", USER);
                options.put("spark.starrocks.password", PASS);
                options.put("spark.starrocks.write.properties.format", "csv");
                options.put("spark.starrocks.write.ctl.enable-transaction", "false");
                options.put("spark.starrocks.infer.columns", "value");
                options.put("spark.starrocks.infer.column.value.type", "string");

                df.selectExpr("CAST(value AS string)")
                .writeStream()
                .trigger(Trigger.ProcessingTime(50000))
                .format("starrocks_writer")
                .outputMode(OutputMode.Append())
                .options(options)
                .start().awaitTermination(20000);

                executeSql(stmt, "select count(1) from " + tableName, "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
                "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
                "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
                "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
                true,
                false);

                executeSql(stmt, "drop table " + tableName, "", false, false);

                }
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }


    @Test
    public void starrocksWritePropertiesUpsertTest(){
        /*
         * 测试从kafka中导入数据，进行updert操作
         */

        // create table
        String tableName = "duplicate_table_upsert_test";
        String createTableSql = "CREATE TABLE `duplicate_table_upsert_test` ( " +
        " `k1`  date,  " +
        " `k2`  datetime,  " +
        " `k3`  string,  " +
        " `k4`  string,  " +
        " `k5`  boolean,  " +
        " `k6`  tinyint,  " +
        " `k7`  smallint,  " +
        " `k8`  int,  " +
        " `k9`  bigint,  " +
        " `k10` largeint,  " +
        " `k11` float,  " +
        " `k12` double,  " +
        " `k13` decimal(27,9)  " +
        " )  " +
        " PRIMARY KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
        " DISTRIBUTED BY HASH(`k1`)  " +
        " BUCKETS 3  " +
        " PROPERTIES ( \"replication_num\" = \"3\")" +
        "; ";
    executeSql(stmt, createTableSql, "", false, false);

    // get data from kafka
    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "data-for-basic-types")
    .load();
        
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", tableName);
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    options.put("spark.starrocks.write.properties.where", "k1 < '2020-06-25'");

    df.selectExpr("cast(value as string)").show();
    try {
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from " + tableName), "2048", true, false);
        executeSql(stmt, String.format("SELECT max(k1), min(k2), sum(k6) FROM %s order by 1",tableName), 
        "2020-06-24,2020-06-23T00:00,-1024",
        true, 
        false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        options.put("spark.starrocks.write.properties.columns", "k1, k2, k3, k4, k5 ,temp, k6 = temp + 1, k7, k8, k9, k10, k11, k12, k13, __op=k5 ");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from " + tableName), "1024", true, false);
        executeSql(stmt, String.format("SELECT sum(k6) FROM %s order by 1", tableName), 
        "0",
        true, 
        false);
        executeSql(stmt, String.format("drop table " + tableName), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    }

    @Test
    public void starrocksWritePropertiesPartialUpdateTest(){
    /*
     * 测试partial update操作
     */
        String createTableSql = "CREATE TABLE `tbl_partial_update_test` ( \n" +
            "k0 int,  \n" +
            "v1 date,  \n" +
            "v2 datetime,  \n" +
            "v3 char(20),  \n" +
            "v4 varchar(20),  \n" +
            "v5 string,  \n" +
            "v6 boolean,  \n" +
            "v7 tinyint,  \n" +
            "v8 smallint,  \n" +
            "v9 int,  \n" +
            "v10 bigint,  \n" +
            "v11 largeint,  \n" +
            "v12 float,  \n" +
            "v13 double,  \n" +
            "v14 decimal(27,9),  \n" +
            "v15 decimal(9,5),  \n" +
            "v16 decimal(18,10),  \n" +
            "v17 decimal(38,18), \n" +
            "v18 datetime,  \n" +
            "v19 ARRAY<int>)   \n" +
            "primary KEY(k0)  \n" +
            "DISTRIBUTED BY HASH(k0)  \n" +
            "BUCKETS 3  \n" +
            "PROPERTIES ( \"replication_num\" = \"3\"); \n" ;
        executeSql(stmt, createTableSql, "", false, false);

        executeSql(stmt, "insert into tbl_partial_update_test values (2,'2022-12-31','2021-12-31 23:59:59','beijingaergertte','haidiansdvgerwwge','tcafvergtrwhtwrht','1','-128','-32768','-2147483648','-9223372036854775808','-170141183460469231731687303715884105728','-3.115','-3.14159','111111111111111111.111111111','1111.11111','11111111.1111111111','11111111111111111111.111111111111111111', '2021-12-31 23:59:59', [1]),(3,'2023-12-31','2021-12-31 23:59:59','beijingaergertte','haidiansdvgerwwge','tcafvergtrwhtwrht','1','-128','-32768','-2147483648','-9223372036854775808','-170141183460469231731687303715884105728','-3.115','-3.14159','111111111111111111.111111111','1111.11111','11111111.1111111111','11111111111111111111.111111111111111111', '2021-12-31 23:59:59', [1])", "", false, false);
    
        Dataset<Row> df = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "topic-partial-update")
        .load();

        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "write");
        options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
        options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
        options.put("spark.starrocks.database", DATABASE);
        options.put("spark.starrocks.table", "tbl_partial_update_test");
        options.put("spark.starrocks.username", USER);
        options.put("spark.starrocks.password", PASS);
        options.put("spark.starrocks.write.properties.format", "csv");
        options.put("spark.starrocks.infer.columns", "value");
        options.put("spark.starrocks.infer.column.value.type", "string");
        
        df.selectExpr("cast(value as string)").show();
        try {
            // properties.columns 与表完全一致
            options.put("spark.starrocks.write.properties.columns", "k0,v1,v2,v3,v4,v5,v6,v7,v8,v9");
            options.put("spark.starrocks.write.properties.column_separator", ",");
            options.put("spark.starrocks.write.properties.partial_update", "true");
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, String.format("select count(1) from tbl_partial_update_test"), "2", true, false);
            executeSql(stmt, "SELECT * FROM tbl_partial_update_test order by 2", 
            "2,2022-12-31,2021-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111,2021-12-31T23:59:59,[1]\n" +
            "3,2121-12-31,2121-12-31T23:59:59,'beiaaaaa','haiaaaaaaa','tcafvaaaaaaa',false,10,100,1000,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111,2021-12-31T23:59:59,[1]",
            true, 
            false);
            executeSql(stmt, String.format("drop table tbl_partial_update_test"), "", false, false);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }


    @Test
    public void starrocksWritePropertiesConditionUpdateTest(){
        /*
         * condition update 测试
         */
        String createTableSql = " CREATE TABLE `table_condition_update_test` ( " +
           " `k1` date NOT NULL,  " +
           " `k2` datetime NOT NULL,  " +
           " `k3` string NOT NULL,  " +
           " `k4` string NOT NULL,  " +
           " `k5` boolean NOT NULL,  " +
           " `v1` tinyint NOT NULL,  " +
           " `v2` smallint NOT NULL,  " +
           " `v3` int NOT NULL,  " +
           " `v4` bigint NOT NULL,  " +
           " `v5` largeint NOT NULL,  " +
           " `v6` float NOT NULL,  " +
           " `v7` double NOT NULL,  " +
           " `v8` decimal(27,9) NOT NULL  " +
           " )  " +
           " PRIMARY KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
           " DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
           " BUCKETS 3 PROPERTIES  " +
           " ( " +
           "  \"replication_num\" = \"3\" " +
           ");";
        executeSql(stmt, createTableSql, "", false, false);

        Dataset<Row> df = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "data-for-basic-types")
        .load();

        // spark.starrocks.write.properties.row_delimiter = \n row_delimiter = \t
        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "write");
        options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
        options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
        options.put("spark.starrocks.database", DATABASE);
        options.put("spark.starrocks.table", "table_condition_update_test");
        options.put("spark.starrocks.username", USER);
        options.put("spark.starrocks.password", PASS);
        options.put("spark.starrocks.write.properties.format", "csv");
        options.put("spark.starrocks.infer.columns", "value");
        options.put("spark.starrocks.infer.column.value.type", "string");
        options.put("spark.starrocks.write.properties.where", "k1 is not null ");

        df.selectExpr("cast(value as string)").show();
        try {
            // properties.columns 与表完全一致

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select * from table_condition_update_test where k1='2020-08-25' and k2='2020-08-25 18:12:14' and k3='guangzhou' and k4='tianhe' and k5=1", 
            "2020-08-25,2020-08-25T18:12:14,guangzhou,tianhe,true,126,32766,-2147418114,-9223372036854710274,-18446744073709486082,6550.3,652.2,62.393000000",
            true, 
            false);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }

        try {
            Dataset<Row> df2 = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "basic_types_data_condition")
            .load();

            options.put("spark.starrocks.write.properties.merge_condition", "v1");
            df2.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select * from table_condition_update_test where k1='2020-08-25' and k2='2020-08-25 18:12:14' and k3='guangzhou' and k4='tianhe' and k5=1", 
            "2020-08-25,2020-08-25T18:12:14,guangzhou,tianhe,true,127,32767,-2147418113,-9223372036854710273,-18446744073709486081,6550.4,652.21,62.394000000",
            true, 
            false);
            executeSql(stmt, String.format("drop table table_condition_update_test"), "", false, false);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

    }

    @Test
    public void supportDataBasicTypeJsonKafkaArray(){
        /*
         * 从kafka中导入json格式的数据文件，数据文件中含有array 类型
         */
        try {
            
            SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .appName("Application")
            .getOrCreate();

            sparkSession.conf().set("spark.sql.streaming.checkpointLocation", "/home/disk4/huoyue/tmp/spark/");
            sparkSession.conf().set("spark.default.parallelism", 2);

            SQLContext sqlContext = new SQLContext(sparkSession);
    
            String createTableSql = "create table dupl_table_types_array( " +
                "c0 bigint, " +
                "c1 ARRAY<bigint> not null, " +
                "c2 array<bigint> not null, " +
                "c3 ARRAY<ARRAY<bigint>> not null, " +
                "c4 array<array<bigint>> not null, " +
                "c5 ARRAY<ARRAY<ARRAY<ARRAY<ARRAY<bigint>>>>> not null, " +
                "c6 array<array<array<array<array<bigint>>>>> not null) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 properties('replication_num'='1');";

            executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
                    .option("subscribe", "data-array-ndjson")
                    .option("startingOffsets", "earliest")
                    .option("checkpointLocation", "/home/disk4/huoyue/tmp/spark/")
                    .option("maxOffsetsPerTrigger", 1000000)
                    .load();

            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            //options.put("spark.starrocks.write.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.http", "172.26.93.108:8030");
            //options.put("spark.starrocks.write.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.fe.urls.jdbc", "jdbc:mysql://172.26.93.108:9030");
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "dupl_table_types_array");
            options.put("spark.starrocks.username", "root");
            options.put("spark.starrocks.password", "");
            options.put("spark.starrocks.write.properties.format", "json");
            options.put("spark.starrocks.write.properties.strip_outer_array", "true");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

            df.map(new KafkaMap(), Encoders.bean(KafkaMessage.class))
                    .writeStream()
                    .trigger(Trigger.ProcessingTime(5000))
                    .format("starrocks_writer")
                    .outputMode(OutputMode.Append())
                    .options(options)
                    .start().awaitTermination(20000);

            executeSql(stmt, "select count(1) from dupl_table_types_array ", "2", true, false);
            executeSql(stmt, String.format("select * from dupl_table_types_array order by 1"), 
            "1,[1],[1],[[1]],[[1]],[[[[[1]]]]],[[[[[1]]]]]\n" +
            "9223372036854775807,[9223372036854775807],[9223372036854775807],[[9223372036854775807]],[[9223372036854775807]],[[[[[9223372036854775807]]]]],[[[[[9223372036854775807]]]]]\n", 
            true,
            false);

            //executeSql(stmt, "drop table dupl_table_types_array ", "", false, false);
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }

    @Test
    public void supportDataBasicTypeMismatch(){
    /*
     * 测试数据类spark和starrocks的数据型不匹配的场景
     */
        try {
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

            // create table 
            String createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_tinyint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint , v8 tinyint , v9 tinyint , v10 tinyint , v11 tinyint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_tinyint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_tinyint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_tinyint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,null,null,null,null,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,null,null,null,null,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,null,null,null,null,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_smallint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 smallint , v8 smallint , v9 smallint , v10 smallint , v11 smallint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_smallint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_smallint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_smallint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,null,null,null,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,null,null,null,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,null,null,null,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_int` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 int , v8 int , v9 int , v10 int , v11 int , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_int");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_int", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_int order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,null,null,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,null,null,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,null,null,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_bigint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 bigint , v8 bigint , v9 bigint , v10 bigint , v11 bigint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_bigint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_bigint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_bigint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,null,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,null,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,null,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_largeint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 largeint , v8 largeint , v9 largeint , v10 largeint , v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_largeint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_largeint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_largeint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_float` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 float , v8 float , v9 float , v10 float , v11 float , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_float");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_float", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_float order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128.0,-32768.0,-2.14748365E9,-9.223372E18,-1.7014118E38,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127.0,32767.0,2.14748365E9,9.223372E18,1.7014118E38,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124.0,-32764.0,-2.14748365E9,-9.223372E18,-1.8446744E19,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_double` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 double , v8 double , v9 double , v10 double , v11 double , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_double");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_double", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_double order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128.0,-32768.0,-2.147483648E9,-9.223372036854776E18,-1.7014118346046923E38,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127.0,32767.0,2.147483647E9,9.223372036854776E18,1.7014118346046923E38,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124.0,-32764.0,-2.147483644E9,-9.223372036854776E18,-1.8446744073709552E19,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_decimal` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 decimal(18,8) , v8 decimal(18,8) , v9 decimal(18,8) , v10 decimal(18,8) , v11 decimal(18,8) , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_decimal");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_decimal", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_decimal order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128.00000000,-32768.00000000,-2147483648.00000000,null,null,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127.00000000,32767.00000000,2147483647.00000000,null,null,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124.00000000,-32764.00000000,-2147483644.00000000,null,null,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_dec_to_tinyint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 tinyint , v13 tinyint, v14 tinyint, v15 tinyint, v16 tinyint, v17 tinyint) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_dec_to_tinyint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_dec_to_tinyint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_dec_to_tinyint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3,-3,null,null,null,null\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3,-3,null,null,null,null\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2,-3,null,123,null,null\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_dec_to_smallint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 smallint , v13 smallint, v14 smallint, v15 smallint, v16 smallint, v17 smallint) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_dec_to_smallint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_dec_to_smallint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_dec_to_smallint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3,-3,null,1111,null,null\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3,-3,null,-1111,null,null\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2,-3,12345,123,1234,null\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_dec_to_int` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 int , v13 int, v14 int, v15 int, v16 int, v17 int) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_dec_to_int");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_dec_to_int", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_dec_to_int order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3,-3,null,1111,11111111,null\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3,-3,null,-1111,-11111111,null\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2,-3,12345,123,1234,123456\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_dec_to_bigint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 bigint , v13 bigint, v14 bigint, v15 bigint, v16 bigint, v17 bigint) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_dec_to_bigint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_dec_to_bigint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_dec_to_bigint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3,-3,111111111111111111,1111,11111111,null\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3,-3,-111111111111111111,-1111,-11111111,null\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2,-3,12345,123,1234,123456\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_dec_to_largeint` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 largeint , v13 largeint, v14 largeint, v15 largeint, v16 largeint, v17 largeint) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_dec_to_largeint");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_dec_to_largeint", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_dec_to_largeint order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3,-3,111111111111111111,1111,11111111,-7335632962598440505\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3,-3,-111111111111111111,-1111,-11111111,7335632962598440505\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2,-3,12345,123,1234,123456\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_character_to_char` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 char(20), v5 char(20), v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_character_to_char");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_character_to_char", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_character_to_char order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_character_to_varchar` ( k0 int, v1 date, v2 datetime, v3 varchar(20), v4 varchar(20), v5 varchar(20), v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_character_to_varchar");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_character_to_varchar", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_character_to_varchar order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_character_to_string` ( k0 int, v1 date, v2 datetime, v3 string, v4 string, v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_character_to_string");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_character_to_string", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_character_to_string order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_datetime_to_date` ( k0 int, v1 date, v2 date, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_datetime_to_date");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_datetime_to_date", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_datetime_to_date order by 1"), 
            "1,9999-12-31,9999-12-31,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_date_to_datetime` ( k0 int, v1 datetime, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint ,v8 smallint ,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_date_to_datetime");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_date_to_datetime", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_date_to_datetime order by 1"), 
            "1,9999-12-31T00:00,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01T00:00,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23T00:00,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_integer_to_string` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 char(20) ,v8 varchar(20) ,string int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_integer_to_string");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_integer_to_string", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_integer_to_string order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_character_to_int` ( k0 int, v1 date, v2 datetime, v3 int, v4 int, v5 int, v6 boolean, v7 tinyint ,v8 smallint,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();

            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_character_to_int");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_character_to_int", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_character_to_int order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,null,null,null,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,null,null,null,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,null,null,null,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

            // create table 
            createTableSql = "CREATE TABLE `tbl_data_type_mismatch_date_to_string` ( k0 int, v1 char(20), v2 varchar(20), v3 int, v4 int, v5 int, v6 boolean, v7 tinyint ,v8 smallint,v9 int ,v10 bigint ,v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(9,5), v16 decimal64(18,10), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();

            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_mismatch_date_to_string");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_mismatch_date_to_string", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_mismatch_date_to_string order by 1"), 
            "1,9999-12-31,9999-12-31 23:59:59,null,null,null,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01 00:00:01,null,null,null,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23 00:00:04,null,null,null,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);

        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }

    @Test
    public void supportDataBasicTypeOutOfRange(){
    /*
     * 测试deimal类型导入数据超出数据范围
     */
        try {
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

            // create table 
            String createTableSql = "CREATE TABLE `tbl_data_type_decimal_out_of_range` ( k0 int, v1 date, v2 datetime, v3 char(20), v4 varchar(20), v5 string, v6 boolean, v7 tinyint , v8 smallint , v9 int , v10 bigint , v11 largeint , v12 float , v13 double , v14 decimal(27,9) , v15 decimal32(8,5), v16 decimal64(18,9), v17 decimal128(38,18)) DUPLICATE KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\" )";
            executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            options.put("spark.starrocks.table", "tbl_data_type_decimal_out_of_range");

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_data_type_decimal_out_of_range", "4", true, false);
            executeSql(stmt, String.format("select * from tbl_data_type_decimal_out_of_range order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,null,11111111.111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,null,-11111111.111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.123450000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
            true,
            false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        }

    @Test
    public void supportDataTypeArrayCsvKafka(){
    /*
     * 测试导入csv格式数据，数据类型为array
     */
        try {
            // create array type table
            executeSql(stmt, "CREATE TABLE `tbl_array_type` (\n" +
            "    `k0` int,\n" +
            "    `v1` array<int>,\n" +
            "    `v2` array<array<int>>,\n" +
            "    `v3` array<array<array<array<array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>>>>>\n" +
            ") ENGINE=OLAP\n" +
            "duplicate KEY(`k0`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`k0`) BUCKETS 3\n" +
            "PROPERTIES (\n" +
            "    \"replication_num\" = \"3\" \n" +
            ");", "", false, false);
            String tableName = "tbl_array_type";

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_array_csv")
            .load();
            df.selectExpr("cast(value as string)").show();
            //df.show();
            
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", tableName);
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

            df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,[2,1],[[1,2]],[[[[[[[[[[[[[[1,2]]]]]]]]]]]]]]\n" +
                "2,[],[[]],[[[[[[[[[[[[[[]]]]]]]]]]]]]]\n" +
                "3,null,null,null",
                true,
                    false);

            executeSql(stmt, "drop table " + tableName, "", true, false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

    }
       
    @Test
    public void supportDataTypeJsonCsvKafka(){
    /*
     * 测试导入csv 格式的数据文件，数据类型为json
     */
        try {
            // create array type table
            executeSql(stmt, "CREATE TABLE `tbl_array_type` (\n" +
            "    `k0` int,\n" +
            "    `v1` JSON,\n" +
            "    `v2` JSON\n" +
            ") ENGINE=OLAP\n" +
            "duplicate KEY(`k0`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`k0`) BUCKETS 3\n" +
            "PROPERTIES (\n" +
            "    \"replication_num\" = \"3\" \n" +
            ");", "", false, false);
            String tableName = "tbl_array_type";

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_json_csv")
            .load();
            df.selectExpr("cast(value as string)").show();
            //df.show();
            
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", tableName);
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

            df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,{\"aaa\": 1},{\"a1\": \"ccc\", \"a2\": 3}\n" +
            "2,{\"aaa\": null},{\"a1\": null, \"a2\": null}\n" +
            "3,\"null\",\"null\"\n" +
            "4,\"\",\"\"",
            true,
            false);

            executeSql(stmt, "drop table " + tableName, "", false, false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }


    public Statement getJdbcConn() {

        Connection conn;
        Statement stmt = null;
        try {
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.createStatement();
        } catch (SQLException se) {
            // 处理 JDBC 错误
            se.printStackTrace();
        } catch (Exception e) {
            // 处理 Class.forName 错误
            e.printStackTrace();
        }
        return stmt;
    }

    public static void executeSql(Statement stmt, String sql, String expected, boolean compare_res, boolean pattern) {
        try {
            //boolean return_res = false;
            System.out.println("RUN:" + sql);
            //ResultSet rs = stmt.executeQuery(caseSql);
            boolean isSelect = stmt.execute(sql);
            StringBuilder result = new StringBuilder();
            if (isSelect) {
                ResultSet rs = stmt.getResultSet();
                // get column num
                int column_count = stmt.getResultSet().getMetaData().getColumnCount();

                if (compare_res){
                    while (rs.next()) {
                        for (int i = 1; i <= column_count; i++){
                            if(i < column_count)
                                result.append(rs.getObject(i)).append(',');
                            else
                                result.append(rs.getObject(i));
                        }
                        result.append('\n');
                    }

                    System.out.println("expected is ---------\n" + expected);
                    System.out.println("result   is ---------\n" + result);
                }
                System.out.println("result equal expected");
                System.out.println(expected.trim().equals(result.toString().trim()));
                System.out.println("------------");
                // Determine whether to perform fuzzy matching on the result
                if (pattern) {
                    if(result.toString().trim().contains(expected.trim()) || Pattern.compile(expected.trim()).matcher(result.toString().trim()).matches()) {
                        Assert.assertTrue(true);
                    }
                }
                else {
                    Assert.assertEquals(expected.trim(), result.toString().trim());
                }

                /*
                if(expected.trim().equals(result.trim())){
                    return true;
                } else {
                    return false;
                }
                */
            }
            else {
                System.out.println("非查询语句");
                Assert.assertTrue(true);
            }
        }
        catch (Exception ex) {
            System.out.println("the sql execute failed");
            if (pattern) {
                if(ex.toString().trim().contains(expected.trim()) || Pattern.compile(expected.trim()).matcher(ex.toString().trim()).matches()) {
                    Assert.assertTrue(true);
                }
            }
            else {
                System.out.println(" expected is " + expected);
                System.out.println(" result is " + ex.toString());
                Assert.assertEquals(expected.trim(), ex.toString().trim());
            }
            ex.printStackTrace();
        }
    }

    public String getSqlFromFile(String filePath, String sqlFile) throws IOException {
        if (filePath.isEmpty())
            return null;
        FileInputStream fileInputStream = null;
        StringBuilder createSql = new StringBuilder();
        try {
            fileInputStream = new FileInputStream(filePath + sqlFile);
            byte[] bytes = new byte[4];//每一次读取四个字节
            int readCount;
            while ((readCount = fileInputStream.read(bytes)) != -1) {
                System.out.print(new String(bytes,0,readCount));//将字节数组转换为字符串
                createSql.append(new String(bytes, 0, readCount));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            assert fileInputStream != null;
            fileInputStream.close();

        }
        return createSql.toString();
    }

public static class KafkaMap implements MapFunction<Row, KafkaMessage> {

    @Override
    public KafkaMessage call(Row row) throws Exception {
        int i = row.fieldIndex("value");
        return JSON.parseObject((byte[]) row.get(i), KafkaMessage.class);
    }
}

 
public static class KafkaMessage implements Serializable {
    private Long c0;
    private Long[] c1;
    private Long[] c2;
    private Long[][] c3;
    private Long[][] c4;
    private Long[][][][][] c5;
    private Long[][][][][] c6;

    public Long getC0() {
        return c0;
    }

    public void setC0(Long c0) {
        this.c0 = c0;
    }

    public Long[] getC1() {
        return c1;
    }

    public void setC1(Long[] c1) {
        this.c1 = c1;
    }

    public Long[] getC2() {
        return c2;
    }

    public void setC2(Long[] c2) {
        this.c2 = c2;
    }

    public Long[][] getC3() {
        return c3;
    }

    public void setC3(Long[][] c3) {
        this.c3 = c3;
    }

    public Long[][] getC4() {
        return c4;
    }

    public void setC4(Long[][] c4) {
        this.c4 = c4;
    }

    public Long[][][][][] getC5() {
        return c5;
    }

    public void setC5(Long[][][][][] c5) {
        this.c5 = c5;
    }

    public Long[][][][][] getC6() {
        return c6;
    }

    public void setC6(Long[][][][][] c6) {
        this.c6 = c6;
    }

}
}

/* 
public static class KafkaMessage implements Serializable {
    private Integer k0;
    private String v1;
    private String v2;
    private String v3;
    private String v4;
    private String v5;
    private Boolean v6;
    private Integer v7;
    private Integer v8;
    private Integer v9;
    private Long v10;
    private BigDecimal v11;
    private Float v12;
    private Double v13;
    private BigDecimal v14;
    private BigDecimal v15;
    private BigDecimal v16;
    private BigDecimal v17;

    private Long c0;
    private Long[] c1;
    private Long[] c2;
    private Long[][] c3;
    private Long[][] c4;
    private Long[][][][][] c5;
    private Long[][][][][] c6;

    public Long getC0() {
        return c0;
    }

    public void setC0(Long c0) {
        this.c0 = c0;
    }

    public Long[] getC1() {
        return c1;
    }

    public void setC1(Long[] c1) {
        this.c1 = c1;
    }

    public Long[] getC2() {
        return c2;
    }

    public void setC2(Long[] c2) {
        this.c2 = c2;
    }

    public Long[][] getC3() {
        return c3;
    }

    public void setC3(Long[][] c3) {
        this.c3 = c3;
    }

    public Long[][] getC4() {
        return c4;
    }

    public void setC4(Long[][] c4) {
        this.c4 = c4;
    }

    public Long[][][][][] getC5() {
        return c5;
    }

    public void setC5(Long[][][][][] c5) {
        this.c5 = c5;
    }

    public Long[][][][][] getC6() {
        return c6;
    }

    public void setC6(Long[][][][][] c6) {
        this.c6 = c6;
    }

    public Integer getK0() {
        return k0;
    }

    public void setK0(Integer k0) {
        this.k0 = k0;
    }

    public String getV1() {
        return v1;
    }

    public void setV1(String v1) {
        this.v1 = v1;
    }

    public String getV2() {
        return v2;
    }

    public void setV2(String v2) {
        this.v2 = v2;
    }

    public String getV3() {
        return v3;
    }

    public void setV3(String v3) {
        this.v3 = v3;
    }

    public String getV4() {
        return v4;
    }

    public void setV4(String v4) {
        this.v4 = v4;
    }

    public String getV5() {
        return v5;
    }

    public void setV5(String v5) {
        this.v5 = v5;
    }

    public Boolean getV6() {
        return v6;
    }

    public void setV6(Boolean v6) {
        this.v6 = v6;
    }

    public Integer getV7() {
        return v7;
    }

    public void setV7(Integer v7) {
        this.v7 = v7;
    }

    public Integer getV8() {
        return v8;
    }

    public void setV8(Integer v8) {
        this.v8 = v8;
    }

    public Integer getV9() {
        return v9;
    }

    public void setV9(Integer v9) {
        this.v9 = v9;
    }

    public Long getV10() {
        return v10;
    }

    public void setV10(Long v10) {
        this.v10 = v10;
    }

    public BigDecimal getV11() {
        return v11;
    }

    public void setV11(BigDecimal v11) {
        this.v11 = v11;
    }

    public Float getV12() {
        return v12;
    }

    public void setV12(Float v12) {
        this.v12 = v12;
    }

    public Double getV13() {
        return v13;
    }

    public void setV13(Double v13) {
        this.v13 = v13;
    }

    public BigDecimal getV14() {
        return v14;
    }

    public void setV14(BigDecimal v14) {
        this.v14 = v14;
    }

    public BigDecimal getV15() {
        return v15;
    }

    public void setV15(BigDecimal v15) {
        this.v15 = v15;
    }

    public BigDecimal getV16() {
        return v16;
    }

    public void setV16(BigDecimal v16) {
        this.v16 = v16;
    }

    public BigDecimal getV17() {
        return v17;
    }

    public void setV17(BigDecimal v17) {
        this.v17 = v17;
    }
}

}
*/
