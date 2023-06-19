package com.starrocks.connector.spark.spark_connector;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.spark.sql.SparkSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.apache.commons.configuration.Configuration;  
import org.apache.commons.configuration.ConfigurationException;  
import org.apache.commons.configuration.PropertiesConfiguration; 

public class ParamaterTest {
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
    static ArrayList<String> userNameList = new ArrayList<>();
    

    @BeforeClass
    public static void prepareBeforeClass() {
        try {
            Configuration config = new PropertiesConfiguration("src/test/java/com/starrocks/connector/spark/spark_connector/conf/conf.properties");     
             FE_IP = config.getString("fe_host");
             FE_QUERYPORT = config.getString("fe_query_port");
             FE_HTTPPORT = config.getString("fe_http_port");
             USER = config.getString("sr_user");
             PASS = config.getString("sr_password");
             JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
             DB_URL = String.format("jdbc:mysql://%s:%s/information_schema", FE_IP, FE_QUERYPORT);
             
             KAFKA_IP_PORT = "172.26.194.239:9092";
             SQL_FILE_PATH = "src/test/java/com/starrocks/connector/spark/spark_connector/sql/";

            } catch (Exception e) {
                e.printStackTrace();
            }


        // init sparksession and sqlContext
        spark = SparkSession
            .builder()
            .master("local")
            .appName("Application")
            .config("spark.url.config", "some-value")
            .getOrCreate();
        sqlContext = new SQLContext(spark);
    }

    @Before
    public void prepareBefore() {
        DATABASE = "spark_connector_datatype" + UUID.randomUUID().toString().replace("-", "").toLowerCase();
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
        
    }

    @After
    public void downAfter() {
        ;
    }

    @AfterClass
    public static void downAfterClass() {
        
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
    public void starrocksConfTest(){

        try {
            // create table
            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
               "k0 int, \n" +
               "v1 date, \n" +
               "v2 datetime, \n" +
               "v3 char(20), \n" +
               "v4 varchar(20), \n" +
               "v5 string, \n" +
               "v6 boolean, \n" +
               "v7 tinyint , \n" +
               "v8 smallint , \n" +
               "v9 int , \n" +
               "v10 bigint , \n" +
               "v11 largeint , \n" +
               "v12 float , \n" +
               "v13 double , \n" +
               "v14 decimal(27,9) , \n" +
               "v15 decimal32(9,5), \n" +
               "v16 decimal64(18,10), \n" +
               "v17 decimal128(38,18) \n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(k0) \n" +
               "COMMENT \"OLAP\" \n" +
               "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
               "PROPERTIES ( \n" +
               "    \"replication_num\" = \"3\" \n" +
               ") \n";
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
            options.put("spark.starrocks.table", "tbl_para_test");
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

            options.put("spark.starrocks.conf", "WRITE");
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);
            executeSql(stmt, "select * from tbl_para_test order by 1 limit 1", "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111", true, false);
            executeSql(stmt, "truncate table tbl_para_test ", "", false, false);

            options.put("spark.starrocks.conf", "WRIte");
            System.out.println("-------------spark.starrocks.conf is " + options.get("spark.starrocks.conf"));
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);
            executeSql(stmt, "select * from tbl_para_test order by 1 limit 1", "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111", true, false);
            executeSql(stmt, "truncate table tbl_para_test ", "", false, false);

            options.put("spark.starrocks.conf", "aaaa");
            System.out.println("-------------spark.starrocks.conf is " + options.get("spark.starrocks.conf"));
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);
            executeSql(stmt, "select * from tbl_para_test order by 1 limit 1", "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111", true, false);
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }

    @Test
    public void starrocksConfTest2(){

        try {
            // create table
            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
               "k0 int, \n" +
               "v1 date, \n" +
               "v2 datetime, \n" +
               "v3 char(20), \n" +
               "v4 varchar(20), \n" +
               "v5 string, \n" +
               "v6 boolean, \n" +
               "v7 tinyint , \n" +
               "v8 smallint , \n" +
               "v9 int , \n" +
               "v10 bigint , \n" +
               "v11 largeint , \n" +
               "v12 float , \n" +
               "v13 double , \n" +
               "v14 decimal(27,9) , \n" +
               "v15 decimal32(9,5), \n" +
               "v16 decimal64(18,10), \n" +
               "v17 decimal128(38,18) \n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(k0) \n" +
               "COMMENT \"OLAP\" \n" +
               "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
               "PROPERTIES ( \n" +
               "    \"replication_num\" = \"3\" \n" +
               ") \n";
           executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "aaaa");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "tbl_para_test");
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

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);
            System.out.println("-------------spark.starrocks.conf is " + options.get("spark.starrocks.conf"));
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }

    /* 
    @Test
    public void starrocksWriteFeUrlsHttpJDBCTest(){

        try {
            // create table
            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
               "k0 int, \n" +
               "v1 date, \n" +
               "v2 datetime, \n" +
               "v3 char(20), \n" +
               "v4 varchar(20), \n" +
               "v5 string, \n" +
               "v6 boolean, \n" +
               "v7 tinyint , \n" +
               "v8 smallint , \n" +
               "v9 int , \n" +
               "v10 bigint , \n" +
               "v11 largeint , \n" +
               "v12 float , \n" +
               "v13 double , \n" +
               "v14 decimal(27,9) , \n" +
               "v15 decimal32(9,5), \n" +
               "v16 decimal64(18,10), \n" +
               "v17 decimal128(38,18) \n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(k0) \n" +
               "COMMENT \"OLAP\" \n" +
               "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
               "PROPERTIES ( \n" +
               "    \"replication_num\" = \"3\" \n" +
               ") \n";
           executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "WRITE");
            options.put("spark.starrocks.fe.urls.http", "172.26.92.200:8030,172.26.95.205:8030,172.26.92.202:8030");
            options.put("spark.starrocks.fe.urls.jdbc", "jdbc:mysql://172.26.92.200:9030;172.26.95.205:9030;172.26.92.202:9030");
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "tbl_para_test");
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

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);

            options.put("spark.starrocks.fe.urls.http", "172.26.92.200:8030");
            options.put("spark.starrocks.fe.urls.jdbc", "172.26.92.200:8030");
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "8", true, false);

            options.put("spark.starrocks.fe.urls.http", "172.26.92.200:8030,172.26.95.205:8030");
            options.put("spark.starrocks.fe.urls.jdbc", "172.26.92.200:8030;172.26.95.205:9030");
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "12", true, false);
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    */

    @Test
    public void starrocksWriteFeUrlsHttpJDBCErrorTest(){

            // create table
            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
               "k0 int, \n" +
               "v1 date, \n" +
               "v2 datetime, \n" +
               "v3 char(20), \n" +
               "v4 varchar(20), \n" +
               "v5 string, \n" +
               "v6 boolean, \n" +
               "v7 tinyint , \n" +
               "v8 smallint , \n" +
               "v9 int , \n" +
               "v10 bigint , \n" +
               "v11 largeint , \n" +
               "v12 float , \n" +
               "v13 double , \n" +
               "v14 decimal(27,9) , \n" +
               "v15 decimal32(9,5), \n" +
               "v16 decimal64(18,10), \n" +
               "v17 decimal128(38,18) \n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(k0) \n" +
               "COMMENT \"OLAP\" \n" +
               "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
               "PROPERTIES ( \n" +
               "    \"replication_num\" = \"3\" \n" +
               ") \n";
           executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "WRITE");
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "tbl_para_test");
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");
        try {
            // ip 错误
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", "192.168.0.1", FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("-------------expection is " + e.getMessage().toString());
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
            
        }

        try {
            // port 错误
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, "0001"));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "12", true, false);            
        } catch (Exception e) {
            e.printStackTrace();
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
            
        }

        try {
            // ip 错误
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", "192.168.0.1", FE_QUERYPORT));
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);            
        } catch (Exception e) {
            e.printStackTrace();
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
            
        }

        try {
            // port 错误
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, "0001"));
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "8", true, false);            
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
        } catch (Exception e) {
            e.printStackTrace();
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
        }
    }


    @Test
    public void starrocksWriteFeUrlsHttpJDBCError2Test(){

                    // create table
            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
               "k0 int, \n" +
               "v1 date, \n" +
               "v2 datetime, \n" +
               "v3 char(20), \n" +
               "v4 varchar(20), \n" +
               "v5 string, \n" +
               "v6 boolean, \n" +
               "v7 tinyint , \n" +
               "v8 smallint , \n" +
               "v9 int , \n" +
               "v10 bigint , \n" +
               "v11 largeint , \n" +
               "v12 float , \n" +
               "v13 double , \n" +
               "v14 decimal(27,9) , \n" +
               "v15 decimal32(9,5), \n" +
               "v16 decimal64(18,10), \n" +
               "v17 decimal128(38,18) \n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(k0) \n" +
               "COMMENT \"OLAP\" \n" +
               "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
               "PROPERTIES ( \n" +
               "    \"replication_num\" = \"3\" \n" +
               ") \n";
           executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "WRITE");
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "tbl_para_test");
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");

        try {
            // port 错误
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", "192.168.0.0", "0001"));
            System.out.println("-------------spark.starrocks.fe.urls.http is " + options.get("spark.starrocks.fe.urls.http"));
            System.out.println("-------------spark.starrocks.fe.urls.jdbc is " + options.get("spark.starrocks.fe.urls.jdbc"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, "select count(1) from tbl_para_test", "4", true, false);            
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
        } catch (Exception e) {
            e.printStackTrace();
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
        }
    }

    @Test
    public void starrocksWriteDatabaseTest(){
            // create table
            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
               "k0 int, \n" +
               "v1 date, \n" +
               "v2 datetime, \n" +
               "v3 char(20), \n" +
               "v4 varchar(20), \n" +
               "v5 string, \n" +
               "v6 boolean, \n" +
               "v7 tinyint , \n" +
               "v8 smallint , \n" +
               "v9 int , \n" +
               "v10 bigint , \n" +
               "v11 largeint , \n" +
               "v12 float , \n" +
               "v13 double , \n" +
               "v14 decimal(27,9) , \n" +
               "v15 decimal32(9,5), \n" +
               "v16 decimal64(18,10), \n" +
               "v17 decimal128(38,18) \n" +
               ") ENGINE=OLAP \n" +
               "DUPLICATE KEY(k0) \n" +
               "COMMENT \"OLAP\" \n" +
               "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
               "PROPERTIES ( \n" +
               "    \"replication_num\" = \"3\" \n" +
               ") \n";
           executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "WRITE");
            options.put("spark.starrocks.database", DATABASE + "a");
            options.put("spark.starrocks.table", "tbl_para_test");
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");
        try {
            // database 不存在
            options.put("spark.starrocks.database", DATABASE + "a");
            System.out.println("-------------spark.starrocks.database is " + options.get("spark.starrocks.database"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("-------------expection is " + e.getMessage().toString());
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
            
        }

        try {
            executeSql(stmt, "create database " + DATABASE + "aaaaaa", "", false, false);
            executeSql(stmt, "use " + DATABASE + "aaaaaa", "", false, false);
            createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
                "k0 int, \n" +
                "v1 date, \n" +
                "v2 datetime, \n" +
                "v3 char(20), \n" +
                "v4 varchar(20), \n" +
                "v5 string, \n" +
                "v6 boolean, \n" +
                "v7 tinyint , \n" +
                "v8 smallint , \n" +
                "v9 int , \n" +
                "v10 bigint , \n" +
                "v11 largeint , \n" +
                "v12 float , \n" +
                "v13 double , \n" +
                "v14 decimal(27,9) , \n" +
                "v15 decimal32(9,5), \n" +
                "v16 decimal64(18,10), \n" +
                "v17 decimal128(38,18) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(k0) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                "PROPERTIES ( \n" +
                "    \"replication_num\" = \"3\" \n" +
                ") \n";
            executeSql(stmt, createTableSql, "", false, false);
            executeSql(stmt, "create database " + DATABASE + "AAAAAA", "", false, false);
            executeSql(stmt, "use " + DATABASE + "AAAAAA", "", false, false);
            createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
                "k0 int, \n" +
                "v1 date, \n" +
                "v2 datetime, \n" +
                "v3 char(20), \n" +
                "v4 varchar(20), \n" +
                "v5 string, \n" +
                "v6 boolean, \n" +
                "v7 tinyint , \n" +
                "v8 smallint , \n" +
                "v9 int , \n" +
                "v10 bigint , \n" +
                "v11 largeint , \n" +
                "v12 float , \n" +
                "v13 double , \n" +
                "v14 decimal(27,9) , \n" +
                "v15 decimal32(9,5), \n" +
                "v16 decimal64(18,10), \n" +
                "v17 decimal128(38,18) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(k0) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                "PROPERTIES ( \n" +
                "    \"replication_num\" = \"3\" \n" +
                ") \n";
            executeSql(stmt, createTableSql, "", false, false);

            options.put("spark.starrocks.database",  DATABASE + "aaaaaa");
            System.out.println("-------------spark.starrocks.database is " + options.get("spark.starrocks.database"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, String.format("select count(1) from %s.tbl_para_test", "`" + DATABASE + "aaaaaa`"), "4", true, false);
            executeSql(stmt, String.format("select count(1) from %s.tbl_para_test", "`" + DATABASE + "AAAAAA`"), "0", true, false);
            
        } catch (Exception e) {

                Assert.assertTrue(false);
           
        }

    }

    @Test
    public void starrocksWriteTableTest(){
        String createTableSql = "";
            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();

            
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "WRITE");
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "tbl_para_test");
            options.put("spark.starrocks.username", USER);
            options.put("spark.starrocks.password", PASS);
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");
        try {
            // table 不存在
            options.put("spark.starrocks.table", "aaaaaa");
            System.out.println("-------------spark.starrocks.database is " + options.get("spark.starrocks.database"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("-------------expection is " + e.getMessage().toString());
            if (e.toString().contains("Writing job aborted")) {
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
            
        }

        try {
            // table 大小写
            createTableSql = "CREATE TABLE `TBL_para_test` (\n" +
                "k0 int, \n" +
                "v1 date, \n" +
                "v2 datetime, \n" +
                "v3 char(20), \n" +
                "v4 varchar(20), \n" +
                "v5 string, \n" +
                "v6 boolean, \n" +
                "v7 tinyint , \n" +
                "v8 smallint , \n" +
                "v9 int , \n" +
                "v10 bigint , \n" +
                "v11 largeint , \n" +
                "v12 float , \n" +
                "v13 double , \n" +
                "v14 decimal(27,9) , \n" +
                "v15 decimal32(9,5), \n" +
                "v16 decimal64(18,10), \n" +
                "v17 decimal128(38,18) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(k0) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                "PROPERTIES ( \n" +
                "    \"replication_num\" = \"3\" \n" +
                ") \n";
            executeSql(stmt, createTableSql, "", false, false);
            createTableSql = "CREATE TABLE `TBL_PARA_TEST` (\n" +
                "k0 int, \n" +
                "v1 date, \n" +
                "v2 datetime, \n" +
                "v3 char(20), \n" +
                "v4 varchar(20), \n" +
                "v5 string, \n" +
                "v6 boolean, \n" +
                "v7 tinyint , \n" +
                "v8 smallint , \n" +
                "v9 int , \n" +
                "v10 bigint , \n" +
                "v11 largeint , \n" +
                "v12 float , \n" +
                "v13 double , \n" +
                "v14 decimal(27,9) , \n" +
                "v15 decimal32(9,5), \n" +
                "v16 decimal64(18,10), \n" +
                "v17 decimal128(38,18) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(k0) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                "PROPERTIES ( \n" +
                "    \"replication_num\" = \"3\" \n" +
                ") \n";
            executeSql(stmt, createTableSql, "", false, false);

            options.put("spark.starrocks.table", "TBL_para_test");
            System.out.println("-------------spark.starrocks.table is " + options.get("spark.starrocks.table"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, String.format("select count(1) from `TBL_para_test`"), "4", true, false);
            executeSql(stmt, String.format("select count(1) from `TBL_PARA_TEST`"), "0", true, false);

            options.put("spark.starrocks.table", "TBL_PARA_TEST");
            System.out.println("-------------spark.starrocks.table is " + options.get("spark.starrocks.table"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, String.format("select count(1) from `TBL_PARA_TEST`"), "4", true, false);
            executeSql(stmt, "drop table `TBL_para_test` ", "", false, false);
            executeSql(stmt, "drop table `TBL_PARA_TEST` ", "", false, false);
        } catch (Exception e) {

                Assert.assertTrue(false);
           
        }

        try {
            // 表名称含有特殊字符
            createTableSql = "CREATE TABLE `tbl_para_test_汉字¥%&*%` (\n" +
                "k0 int, \n" +
                "v1 date, \n" +
                "v2 datetime, \n" +
                "v3 char(20), \n" +
                "v4 varchar(20), \n" +
                "v5 string, \n" +
                "v6 boolean, \n" +
                "v7 tinyint , \n" +
                "v8 smallint , \n" +
                "v9 int , \n" +
                "v10 bigint , \n" +
                "v11 largeint , \n" +
                "v12 float , \n" +
                "v13 double , \n" +
                "v14 decimal(27,9) , \n" +
                "v15 decimal32(9,5), \n" +
                "v16 decimal64(18,10), \n" +
                "v17 decimal128(38,18) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(k0) \n" +
                "COMMENT \"OLAP\" \n" +
                "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                "PROPERTIES ( \n" +
                "    \"replication_num\" = \"3\" \n" +
                ") \n";
            executeSql(stmt, createTableSql, "", false, false);

            options.put("spark.starrocks.table", "tbl_para_test_汉字¥%&*%");
            System.out.println("-------------spark.starrocks.table is " + options.get("spark.starrocks.table"));

            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();
            executeSql(stmt, String.format("select count(1) from `tbl_para_test_汉字¥%&*%`"), "4", true, false);
            executeSql(stmt, "drop table `tbl_para_test_汉字¥%&*%` ", "", false, false);
        } catch (Exception e) {
            Assert.assertTrue(false);
           
        }
    }
        @Test
        public void starrocksWriteUsernameTest(){
            
            String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
            executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
            userNameList.add(userName);

            String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
            "k0 int, \n" +
            "v1 date, \n" +
            "v2 datetime, \n" +
            "v3 char(20), \n" +
            "v4 varchar(20), \n" +
            "v5 string, \n" +
            "v6 boolean, \n" +
            "v7 tinyint , \n" +
            "v8 smallint , \n" +
            "v9 int , \n" +
            "v10 bigint , \n" +
            "v11 largeint , \n" +
            "v12 float , \n" +
            "v13 double , \n" +
            "v14 decimal(27,9) , \n" +
            "v15 decimal32(9,5), \n" +
            "v16 decimal64(18,10), \n" +
            "v17 decimal128(38,18) \n" +
            ") ENGINE=OLAP \n" +
            "DUPLICATE KEY(k0) \n" +
            "COMMENT \"OLAP\" \n" +
            "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
            "PROPERTIES ( \n" +
            "    \"replication_num\" = \"3\" \n" +
            ") \n";
            executeSql(stmt, createTableSql, "", false, false);

            Dataset<Row> df = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_csv")
            .load();
            df.show();
            df.selectExpr("CAST(value AS string)").show();
    
                
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "WRITE");
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", "tbl_para_test");
            options.put("spark.starrocks.username", userName);
            options.put("spark.starrocks.password", "123456");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.write.properties.format", "csv");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            options.put("spark.starrocks.infer.columns", "value");
            options.put("spark.starrocks.infer.column.value.type", "string");
            try {
                // 用户没有权限
                df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();
    
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("-------------expection is " + e.getMessage().toString());
                if (e.toString().contains("Writing job aborted")) {
                    Assert.assertTrue(true);
                }
                else {
                    Assert.assertTrue(false);
                }
            }

            try {
                options.put("spark.starrocks.username", userName+'a');
                // 用户不存在
                df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();
    
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("-------------expection is " + e.getMessage().toString());
                if (e.toString().contains("Writing job aborted")) {
                    Assert.assertTrue(true);
                }
                else {
                    Assert.assertTrue(false);
                }
            }

            try {
                // 密码不对
                options.put("spark.starrocks.username", userName);
                options.put("spark.starrocks.password", "123456");
                
                df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();
    
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("-------------expection is " + e.getMessage().toString());
                if (e.toString().contains("Writing job aborted")) {
                    Assert.assertTrue(true);
                }
                else {
                    Assert.assertTrue(false);
                }
            }

            try {
                // 用户有权限
                executeSql(stmt, "grant insert on tbl_para_test to user " + userName, "", false, false);

                df.selectExpr("CAST(value AS string)")
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();

                executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
                executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            } catch (Exception e) {
                    Assert.assertTrue(false);
            }
    
    }

    @Test
    public void starrocksWriteCTLEnableTransactionTest(){
        
        String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
        executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
        userNameList.add(userName);

        String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
        "k0 int, \n" +
        "v1 date, \n" +
        "v2 datetime, \n" +
        "v3 char(20), \n" +
        "v4 varchar(20), \n" +
        "v5 string, \n" +
        "v6 boolean, \n" +
        "v7 tinyint , \n" +
        "v8 smallint , \n" +
        "v9 int , \n" +
        "v10 bigint , \n" +
        "v11 largeint , \n" +
        "v12 float , \n" +
        "v13 double , \n" +
        "v14 decimal(27,9) , \n" +
        "v15 decimal32(9,5), \n" +
        "v16 decimal64(18,10), \n" +
        "v17 decimal128(38,18) \n" +
        ") ENGINE=OLAP \n" +
        "DUPLICATE KEY(k0) \n" +
        "COMMENT \"OLAP\" \n" +
        "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
        "PROPERTIES ( \n" +
        "    \"replication_num\" = \"3\" \n" +
        ") \n";
        executeSql(stmt, createTableSql, "", false, false);

        Dataset<Row> df = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "topic_kafka_spark_csv")
        .load();
        df.show();
        df.selectExpr("CAST(value AS string)").show();

            
        // enable-transaction = True
        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "WRITE");
        options.put("spark.starrocks.database", DATABASE);
        options.put("spark.starrocks.table", "tbl_para_test");
        options.put("spark.starrocks.username", USER);
        options.put("spark.starrocks.password", "");
        options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
        options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
        options.put("spark.starrocks.write.properties.format", "csv");
        options.put("spark.starrocks.infer.columns", "value");
        options.put("spark.starrocks.infer.column.value.type", "string");
        try {
            // 设置参数 ctl.enable-transaction = true
            options.put("spark.starrocks.write.ctl.enable-transaction", "true");
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();
            
            executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

        try {
            // 设置参数 ctl.enable-transaction = true
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();
            
            executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

        try {
            // 设置参数 ctl.enable-transaction 为非法值
            options.put("spark.starrocks.write.ctl.enable-transaction", "aaa");
            df.selectExpr("CAST(value AS string)")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();
        } catch (Exception e) {
            System.out.println("-------------expection is " + e.getMessage().toString());
            System.out.println("-------------expection is " + e.toString().contains("Writing job aborted"));
            if (e.toString().contains("aaa is not a boolean string")) {
                executeSql(stmt, "drop table tbl_para_test ", "", false, false);
                Assert.assertTrue(true);
            }
            else {
                Assert.assertTrue(false);
            }
        }

}

@Test
public void starrocksWriteCTLCacheMaxBytesTest(){
    
    String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
    executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
    userNameList.add(userName);

    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
    df.show();
    df.selectExpr("CAST(value AS string)").show();

        
    // enable-transaction = True
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "WRITE");
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", "");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.write.ctl.enable-transaction", "true");
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.cacheMaxBytes", "1024");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.cacheMaxBytes", "102400");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction 为非法值
        options.put("spark.starrocks.write.ctl.cacheMaxBytes", "-1");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        if (e.toString().contains("Writing job aborted")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}

@Test
public void starrocksWriteCTLExpectDelayTimeTest(){
    
    String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
    executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
    userNameList.add(userName);

    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
    df.show();
    df.selectExpr("CAST(value AS string)").show();

        
    // enable-transaction = True
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "WRITE");
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", "");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.write.ctl.enable-transaction", "true");
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.expectDelayTime", "50");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.expectDelayTime", "30000000000");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction 为非法值
        options.put("spark.starrocks.write.ctl.expectDelayTime", "0");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        if (e.toString().contains("Writing job aborted")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}


@Test
public void starrocksWriteCTLConnectTimeoutTest(){
    
    String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
    executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
    userNameList.add(userName);

    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
    df.show();
    df.selectExpr("CAST(value AS string)").show();

        
    // enable-transaction = True
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "WRITE");
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", "");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.write.ctl.enable-transaction", "true");
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.connectTimeout", "100");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.connectTimeout", "60000");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction 为非法值
        options.put("spark.starrocks.write.ctl.connectTimeout", "1000000000000000");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        if (e.toString().contains("For input string: \"1000000000000000\"")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}


@Test
public void starrocksWriteCTLIOThreadCount(){
    
    String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
    executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
    userNameList.add(userName);

    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
    df.show();
    df.selectExpr("CAST(value AS string)").show();

        
    // enable-transaction = True
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "WRITE");
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", "");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.write.ctl.enable-transaction", "true");
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.ioThreadCount", "1");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction = true
        options.put("spark.starrocks.write.ctl.ioThreadCount", "60000");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.enable-transaction 为非法值
        options.put("spark.starrocks.write.ctl.ioThreadCount", "0.01");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();

        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        if (e.toString().contains("For input string: \"0.01\"")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}

@Test
public void starrocksWriteCTLlabelPrefixTest(){
    
    String userName = "spark_conn_" +  UUID.randomUUID().toString().replace("-", "").toLowerCase();
    executeSql(stmt, String.format("create user %s identified by '123456' ", userName), "", false, false);
    userNameList.add(userName);

    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
    df.show();
    df.selectExpr("CAST(value AS string)").show();

        
    // enable-transaction = True
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "WRITE");
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", "");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.write.ctl.enable-transaction", "true");
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    try {
        // 设置参数 ctl.labelPrefix 以字母开头
        options.put("spark.starrocks.write.ctl.labelPrefix", "abdefsf");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.labelPrefix 以数字开头
        options.put("spark.starrocks.write.ctl.labelPrefix", "123abdef");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "8", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.labelPrefix 以数字开头
        options.put("spark.starrocks.write.ctl.labelPrefix", "_abdef");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "12", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // 设置参数 ctl.labelPrefix 以下划线开头
        options.put("spark.starrocks.write.ctl.ioThreadCount", "asdgethtrwhergfdghbkdfsjbghopewruighldfhjgboeirfughoiwterhjgoretwjgoerigjeoarihgjoertiuhgbueorhpuoerhp");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();

        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "12", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        if (e.toString().contains("For input string: \"asdgethtrwhergfdghbkdfsjbghopewruighldfhjgboeirfughoiwterhjgoretwjgoerigjeoarihgjoertiuhgbueorhpuoerhp\"")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}

@Test
public void starrocksWritePropertiesFormatTest(){
    
    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
    df.show();
    df.selectExpr("CAST(value AS string)").show();

        
    // spark.starrocks.write.properties.format 使用默认值
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "WRITE");
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", "");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");

    try {
        // properties.format 为默认值
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.format 为非法字符
        options.put("spark.starrocks.write.properties.format", "avro");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "12", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        if (e.toString().contains("UnSupport format avro")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }
}

@Test
public void starrocksWritePropertiesDelimiterTest(){
    
    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
        
    // spark.starrocks.write.properties.row_delimiter = \n row_delimiter = \t
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.write.ctl.enable-transaction", "false");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    
    df.selectExpr("cast(value as string)").show();
    try {

        // properties.format 为默认值
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, String.format("SELECT * FROM tbl_para_test order by 1 "), 
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
        true,
            false);

        executeSql(stmt, String.format("truncate table tbl_para_test"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.row_delimiter 错误
        options.put("spark.starrocks.write.properties.row_delimiter", "aaa");
        options.put("spark.starrocks.write.properties.column_separator", "\t");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();

        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, String.format("SELECT * FROM tbl_para_test order by 1 "), 
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
        true,
            false);

    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.column_separator 错误
        options.put("spark.starrocks.write.properties.row_delimiter", "\n");
        options.put("spark.starrocks.write.properties.column_separator", ",");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();

        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "65546", true, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        System.out.println("----------------------------------------------------------------------- ");
        if (e.toString().contains("Writing job aborted")) {
            executeSql(stmt, "drop table tbl_para_test ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }
}


@Test
public void starrocksWritePropertiesDelimiter2Test(){
    
    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
        
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.write.ctl.enable-transaction", "false");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    
    df.selectExpr("cast(value as string)").show();

    try {
        // properties.row_delimiter 错误
        options.put("spark.starrocks.write.properties.row_delimiter", "aaa");
        options.put("spark.starrocks.write.properties.column_separator", "\t");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();

        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, String.format("SELECT * FROM tbl_para_test order by 1 "), 
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
        true,
            false);
        executeSql(stmt, "drop table tbl_para_test ", "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

}

@Test
public void starrocksWritePropertiesColumnTest(){
    
    String createTableSql = "CREATE TABLE `duplicate_table_with_null` ( " +
        " `k1`  date,  " +
        " `k2`  datetime,  " +
        " `k3`  char(20),  " +
        " `k4`  varchar(20),  " +
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
        " DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
        " DISTRIBUTED BY HASH(`k1`, `k2`, `k3`)  " +
        " BUCKETS 3  " +
        " PROPERTIES ( \"replication_num\" = \"3\")" +
        "; ";
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
    options.put("spark.starrocks.table", "duplicate_table_with_null");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    

    df.selectExpr("cast(value as string)").show();
    try {
        // properties.columns 与表完全一致
        options.put("spark.starrocks.infer.properties.columns", "k1, k2, k3, k4, k5, k6, k7, k8 ,k9 ,k10, k11, k12, k13");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.columns 与表顺序不一致
        options.put("spark.starrocks.infer.properties.columns", "k1, k2, k3, k4, k5, k7, k6, k8 ,k9 ,k10, k11, k12, k13");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.columns 比sr中的列多
        options.put("spark.starrocks.infer.properties.columns", "k1, k2, k3, k4, k5, k6, k7, k8 ,k9 ,k10, k11, k12, k13, k14");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.columns 比sr中的列少
        options.put("spark.starrocks.infer.properties.columns", "k1, k2, k3, k4, k5, k6, k7, k9 ,k10, k11, k12, k13");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.columns 与sr中的部分列名称不一样
        options.put("spark.starrocks.infer.properties.columns", "k1, k2, k3, k4, k5, k6, k7, v8, v9 ,v10, k11, k12, k13");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("drop table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

}

@Test
public void starrocksWritePropertiesColumn2Test(){
    
    String createTableSql = "CREATE TABLE `tbl_para_test` (\n" +
    "k0 int, \n" +
    "v1 date, \n" +
    "v2 datetime, \n" +
    "v3 char(20), \n" +
    "v4 varchar(20), \n" +
    "v5 string, \n" +
    "v6 boolean, \n" +
    "v7 tinyint , \n" +
    "v8 smallint , \n" +
    "v9 int , \n" +
    "v10 bigint , \n" +
    "v11 largeint , \n" +
    "v12 float , \n" +
    "v13 double , \n" +
    "v14 decimal(27,9) , \n" +
    "v15 decimal32(9,5), \n" +
    "v16 decimal64(18,10), \n" +
    "v17 decimal128(38,18) \n" +
    ") ENGINE=OLAP \n" +
    "DUPLICATE KEY(k0) \n" +
    "COMMENT \"OLAP\" \n" +
    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
    "PROPERTIES ( \n" +
    "    \"replication_num\" = \"3\" \n" +
    ") \n";
    executeSql(stmt, createTableSql, "", false, false);

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "topic_kafka_spark_csv")
    .load();
        
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "tbl_para_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.write.ctl.enable-transaction", "false");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    

    df.selectExpr("cast(value as string)").show();
    try {
        // properties.columns 与表完全一致
        options.put("spark.starrocks.write.properties.columns", "k0, v1, v2, v3, v4, v5, v6, v7, v8 ,v9 ,v10, v11, v12, v13, v14, v15, v16, v17");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, "SELECT * FROM tbl_para_test order by 1 ",
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" + 
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table tbl_para_test"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.columns 与表顺序不一致
        options.put("spark.starrocks.write.properties.columns", "k0, v1, v2, v13, v4, v5, v16, v7, v8 ,v9 ,v10, v11, v12, v3, v14, v15, v6, v17");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, "SELECT * FROM tbl_para_test order by 1 ",
        "1,9999-12-31,9999-12-31T23:59:59,-3.14159,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,null,111111111111111111.111111111,1111.11111,1.0000000000,11111111111111111111.111111111111111111\n" +
        "2,0001-01-01,0001-01-01T00:00:01,-3.14159,haidian,asfarewgeragergre,true,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,null,-111111111111111111.111111111,-1111.11111,0E-10,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,-3.1,'','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,null,12345.123456789,123.12300,1.0000000000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n",
        true, 
        false);
        executeSql(stmt, String.format("truncate table tbl_para_test"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.columns 比sr中的列多
        options.put("spark.starrocks.write.properties.columns", "k1, k2, k3, k4, k5, k6, k7, k8 ,k9 ,k10, k11, k12, k13, k14");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
    
    } catch (Exception e) {
        if (e.toString().contains("Writing job aborted")) {
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

    try {
        // properties.columns 比sr中的列少
        options.put("spark.starrocks.write.properties.columns", "k1, k2, k3, k4, k5, k6, k7, k9 ,k10, k11, k12, k13");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, "SELECT * FROM tbl_para_test order by 1 ",
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" + 
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table tbl_para_test"), "", false, false);
    } catch (Exception e) {
        if (e.toString().contains("Writing job aborted")) {
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

    try {
        // properties.columns 与sr中的部分列名称不一样
        options.put("spark.starrocks.write.properties.columns", "k0, v1, v2, v3, v4, v5, v6, v7, v8 ,v9 ,v10, v111, v122, v13, v14, v15, v16, v17");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from tbl_para_test"), "4", true, false);
        executeSql(stmt, "SELECT * FROM tbl_para_test order by 1 ",
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,null,null,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,null,null,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,'','','',true,-124,-32764,-2147483644,-9223372036854775804,null,null,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n",
        true, 
        false);
        executeSql(stmt, "drop table tbl_para_test ", "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

}

@Test
public void starrocksWritePropertiesWhereTest(){
    
    String createTableSql = "CREATE TABLE `duplicate_table_with_null` ( " +
        " `k1`  date,  " +
        " `k2`  datetime,  " +
        " `k3`  char(20),  " +
        " `k4`  varchar(20),  " +
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
        " DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
        " DISTRIBUTED BY HASH(`k1`, `k2`, `k3`)  " +
        " BUCKETS 3  " +
        " PROPERTIES ( \"replication_num\" = \"3\")" +
        "; ";
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
    options.put("spark.starrocks.table", "duplicate_table_with_null");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");

    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    
    df.selectExpr("cast(value as string)").show();
    try {
        // properties.where 使用不同副词
        options.put("spark.starrocks.write.properties.where", " (k1 <= 20200825) or (k11 >= -3.1) or (k5 between 0 and 1) or k6 < 128 or k7 > -32769 or k5 <> 2 or k11 != 7000");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65536", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.where 使用不同副词
        options.put("spark.starrocks.write.properties.where", " k5 in (1,0) and k5 not in (2,3) or k3 like 'bei%' or k3 not like 'shenzhe?' or k5 is null or k7 is not null or ( not (k7 > -32768))");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("drop table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }
}

@Test
public void starrocksWritePropertiesMaxFilterRatioTest(){
    
    String createTableSql = "CREATE TABLE `duplicate_table_with_null` ( " +
        " `k1`  date,  " +
        " `k2`  datetime,  " +
        " `k3`  char(20),  " +
        " `k4`  varchar(20),  " +
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
        " DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
        " DISTRIBUTED BY HASH(`k1`, `k2`, `k3`)  " +
        " BUCKETS 3  " +
        " PROPERTIES ( \"replication_num\" = \"3\")" +
        "; ";
    executeSql(stmt, createTableSql, "", false, false);

    // properties.max_filter_ratio = 0.01
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "duplicate_table_with_null");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.write.properties.max_filter_ratio", "0.01");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    
    try {
        // test data error ratio < max_filter_ratio
        Dataset<Row> df = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "max-filter-ratio-01-topic")
        .load();

        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65536", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "5728,-30902816,-2059436296736,-9223372036854774785,-18446744073709550593,3.4,-2.49,-2490.523000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // test data error ratio > max_filter_ratio
        Dataset<Row> df2 = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "max-filter-ratio-02-topic")
        .load();

        // properties.max_filter_ratio = 0.01
        options.put("spark.starrocks.write.properties.max_filter_ratio", "0.01");
        df2.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65536", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "null,null,null,null,null,null,null,null",
        true, 
        false);
        executeSql(stmt, String.format("drop table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }
}

@Test
public void starrocksWritePropertiesMaxFilterRatio2Test(){
    
    String createTableSql = "CREATE TABLE `duplicate_table_without_null` ( `k1`  date NOT NULL, `k2`  datetime NOT NULL, `k3`  char(20) NOT NULL, `k4`  varchar(20) NOT NULL, `k5`  boolean NOT NULL, `k6`  tinyint NOT NULL, `k7`  smallint NOT NULL, `k8`  int NOT NULL, `k9`  bigint NOT NULL, `k10` largeint NOT NULL, `k11` float NOT NULL, `k12` double NOT NULL, `k13` decimal(27,9) NOT NULL ) DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3 PROPERTIES ( \"replication_num\" = \"3\");";
    executeSql(stmt, createTableSql, "", false, false);

    // properties.max_filter_ratio = 0.01
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "duplicate_table_without_null");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");
    
    try {
        // test data error ratio > max_filter_ratio
        Dataset<Row> df2 = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "max-filter-ratio-02-topic")
        .load();

        // properties.max_filter_ratio = 0.01
        options.put("spark.starrocks.write.properties.max_filter_ratio", "0.5");
        df2.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_without_null"), "64226", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_without_null", 
        "-28723,42035917,-137922138182963,-9223372036854710273,-18446744073709486081,127.9,9.96,1944859.619000000",
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_without_null"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // test data error ratio > max_filter_ratio
        Dataset<Row> df2 = sqlContext.read()
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
        .option("subscribe", "max-filter-ratio-02-topic")
        .load();

        // properties.max_filter_ratio = 0.01
        options.put("spark.starrocks.write.properties.max_filter_ratio", "0.01");
        df2.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();

        executeSql(stmt, String.format("select count(1) from duplicate_table_without_null"), "65536", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_without_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "null,null,null,null,null,null,null,null",
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_without_null"), "", false, false);
    } catch (Exception e) {
        if (e.toString().contains("Writing job aborted")) {
            executeSql(stmt, "drop table duplicate_table_without_null ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}

@Test
public void starrocksWritePropertiesPartitionsTest(){
    
    String createTableSql = "CREATE TABLE `duplicate_table_with_null_partition` " +
    "  (  " +
    "      `k1` date,  " +
    "      `k2` datetime,  " +
    "      `k3` char(20),  " +
    "      `k4` varchar(20),  " +
    "      `k5` boolean,  " +
    "      `k6` tinyint,  " +
    "      `k7` smallint,  " +
    "      `k8` int,  " +
    "      `k9` bigint,  " +
    "      `k10` largeint,  " +
    "      `k11` float,  " +
    "      `k12` double,  " +
    "      `k13` decimal(27,9)  " +
    " ) DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
    " PARTITION BY RANGE(`k1`)  " +
    " (  " +
    "     PARTITION `p202006` VALUES LESS THAN (\"2020-07-01\"),  " +
    "     PARTITION `p202007` VALUES LESS THAN (\"2020-08-01\"),  " +
    "     PARTITION `p202008` VALUES LESS THAN (\"2020-09-01\")  " +
    " )  " +
    " DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
    " BUCKETS 3  " +
    " PROPERTIES ( " +
    "  \"replication_num\" = \"3\" " +
    "  ); ";
    
    executeSql(stmt, createTableSql, "", false, false);

    // properties.partitions = p202006
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "duplicate_table_with_null_partition");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.write.properties.partitions", "p202006");
    options.put("spark.starrocks.write.properties.where", "k1 < '2020-07-01' ");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "data-for-basic-types")
    .load();

    try {
        // properties.partitions = p202006
        options.put("spark.starrocks.write.properties.partitions", "p202006");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null_partition"), "8192", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null_partition WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null_partition"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // properties.partitions error
        options.put("spark.starrocks.write.properties.partitions", "p202009");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null_partition"), "8192", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null_partition WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table duplicate_table_with_null_partition"), "", false, false);
        executeSql(stmt, "drop table duplicate_table_with_null_partition ", "", false, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        System.out.println("----------------------------------------------------------------------- ");
        if (e.toString().contains("Writing job aborted")) {
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}

@Test
public void starrocksWritePropertiesTimeoutTest(){
    
    String createTableSql = "CREATE TABLE `duplicate_table_with_null` ( " +
        " `k1`  date,  " +
        " `k2`  datetime,  " +
        " `k3`  char(20),  " +
        " `k4`  varchar(20),  " +
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
        " DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)  " +
        " DISTRIBUTED BY HASH(`k1`, `k2`, `k3`)  " +
        " BUCKETS 3  " +
        " PROPERTIES ( \"replication_num\" = \"3\")" +
        "; ";
    executeSql(stmt, createTableSql, "", false, false);
    
    // properties.partitions = p202006
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "duplicate_table_with_null");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "data-for-basic-types")
    .load();

    try {
        // test data error ratio < max_filter_ratio
        options.put("spark.starrocks.write.properties.timeout", "198");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from duplicate_table_with_null"), "65546", true, false);
        executeSql(stmt, "SELECT SUM(CAST(k6 AS int)), SUM(k7), SUM(k8), MAX(k9), MAX(k10), MIN(k11), MIN(k12), SUM(k13) FROM duplicate_table_with_null WHERE k1 = '2020-06-23' AND k2 <= '2020-06-23 18:11:00'", 
        "-512,-33030656,-2199022731776,-9223372036854774785,-18446744073709550593,-3.1,-3.14,-2692.608000000", 
        true, 
        false);
        executeSql(stmt, String.format("drop table duplicate_table_with_null"), "", false, false);
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        System.out.println("----------------------------------------------------------------------- ");
        if (e.toString().contains("Writing job aborted")) {
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }

}

@Test
public void starrocksWritePropertiesStrictModeTest(){
    
    String createTableSql = "CREATE TABLE `abnormal_date` ( " +
     "   `k1` date, " +
     "   `k2` datetime " +
     " ) " +
     " AGGREGATE KEY(`k1`, `k2`) " +
     " DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3 " +
     " PROPERTIES ( " +
     "   \"replication_num\" = \"3\" " +
     " )";
    executeSql(stmt, createTableSql, "", false, false);
    
    // properties.partitions = p202006
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "abnormal_date");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "data-for-abnormal-date")
    .load();

    try {
        // test data error ratio < max_filter_ratio
        options.put("spark.starrocks.write.properties.strict_mode", "false");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from abnormal_date"), "72", true, false);
        executeSql(stmt, " select * from abnormal_date where k1 is not null and k2 is not null ", 
        "2020-06-21,2020-06-21T00:00", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table abnormal_date"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // test data error ratio < max_filter_ratio
        options.put("spark.starrocks.write.properties.strict_mode", "true");
        options.put("spark.starrocks.write.properties.columns", "k1, k2");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
    } catch (Exception e) {
        System.out.println("-------------expection is " + e.getMessage().toString());
        System.out.println("----------------------------------------------------------------------- ");
        if (e.toString().contains("Writing job aborted")) {
            executeSql(stmt, "drop table abnormal_date ", "", false, false);
            Assert.assertTrue(true);
        }
        else {
            Assert.assertTrue(false);
        }
    }


}

@Test
public void starrocksWritePropertiesTimezoneTest(){
    
    String createTableSql = "CREATE TABLE `table_timezone_test` ( " +
        " `k1`  datetime  " +
        " )  " +
        " DUPLICATE KEY(`k1`)  " +
        " DISTRIBUTED BY HASH(`k1`)  " +
        " BUCKETS 3  " +
        " PROPERTIES ( \"replication_num\" = \"3\")" +
        "; ";
    executeSql(stmt, createTableSql, "", false, false);
    
    // properties.partitions = p202006
    Map<String, String> options = new HashMap<>();
    options.put("spark.starrocks.conf", "write");
    options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
    options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
    options.put("spark.starrocks.database", DATABASE);
    options.put("spark.starrocks.table", "table_timezone_test");
    options.put("spark.starrocks.username", USER);
    options.put("spark.starrocks.password", PASS);
    options.put("spark.starrocks.write.properties.format", "csv");
    options.put("spark.starrocks.infer.columns", "value");
    options.put("spark.starrocks.infer.column.value.type", "string");

    Dataset<Row> df = sqlContext.read()
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
    .option("subscribe", "data-for-date-function")
    .load();

    try {
        // test data error ratio < max_filter_ratio
        options.put("spark.starrocks.write.properties.timezone", "America/New_York");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from table_timezone_test"), "1", true, false);
        executeSql(stmt, "SELECT * FROM table_timezone_test ", 
        "2020-06-29T14:20:54", 
        true, 
        false);
        executeSql(stmt, String.format("truncate table table_timezone_test"), "", false, false);
    } catch (Exception e) {
        Assert.assertTrue(false);
    }

    try {
        // test data error ratio < max_filter_ratio
        options.put("spark.starrocks.write.properties.timezone", "Asia/Shanghai");
        df.selectExpr("CAST(value AS string)")
        .write()
        .format("starrocks_writer")
        .mode(SaveMode.Append)
        .options(options)
        .save();
        
        executeSql(stmt, String.format("select count(1) from table_timezone_test"), "1", true, false);
        executeSql(stmt, "SELECT * FROM table_timezone_test ", 
        "2020-06-29T14:20:54", 
        true, 
        false);
        executeSql(stmt, String.format("drop table table_timezone_test"), "", false, false);
    } catch (Exception e) {
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

}
