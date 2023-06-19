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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;
import java.math.BigDecimal;
import org.slf4j.LoggerFactory;
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

public class SparkSqlTest {
    
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
    static String FE_URLS_HTTP = null;
    static String FE_URLS_JDBC = null;
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
             FE_URLS_HTTP = String.format("%s:%s", FE_IP, FE_HTTPPORT);
             FE_URLS_JDBC = String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT);

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
         
        ;
    }

    @Test
    public void BasicType(){
        /*
         * 测试插入基础数据类型
         */
        try {
            String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
            for (String tableName : tableNames) {
                String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
                executeSql(stmt, createTableSql, "", false, false);

                String spark_table_sql = "create table " + tableName + "_sink" + "( \n" +
                    "k0 int, \n" +
                    "v1 string, \n" +
                    "v2 string, \n" +
                    "v3 string, \n" +
                    "v4 string, \n" +
                    "v5 string, \n" +
                    "v6 int, \n" +
                    "v7 int , \n" +
                    "v8 int , \n" +
                    "v9 int , \n" +
                    "v10 long , \n" +
                    "v11 string , \n" +
                    "v12 float , \n" +
                    "v13 double , \n" +
                    "v14 decimal(27,9) , \n" +
                    "v15 decimal(9,5), \n" +
                    "v16 decimal(18,10), \n" +
                    "v17 decimal(38,18) \n" +
                    ") USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(spark_table_sql);

                spark.sql(String.format("insert into %s values(1,'9999-12-31','9999-12-31 23:59:59','beijingaergertte','haidiansdvgerwwge','tcafvergtrwhtwrht',1,-128,-32768,-2147483648,-9223372036854775808,'-170141183460469231731687303715884105728',-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111);", tableName + "_sink"));
                spark.sql(String.format("insert into %s values(2,'0001-01-01','0001-01-01 00:00:01','beijing','haidian','asfarewgeragergre',0,127,32767,2147483647,9223372036854775807,'170141183460469231731687303715884105727',-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111);", tableName + "_sink"));
                spark.sql(String.format("insert into %s values(3,'2020-06-23','2020-06-23 00:00:04','','','',1,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000);", tableName + "_sink"));
                spark.sql(String.format("insert into %s values(4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);", tableName + "_sink"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
                "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
                "3,2020-06-23,2020-06-23T00:00:04,,,,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
                "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
                true,
                    false);
                executeSql(stmt, String.format("drop table %s", tableName), "", false, false);
            }
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }


    @Test
    public void BasicTypeTempView(){
        /*  
         * 测试插入基础数据类型，使用temp view
         */
        try {
            String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
            for (String tableName : tableNames) {
                String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
                executeSql(stmt, createTableSql, "", false, false);

                String spark_table_sql = "create TEMPORARY view  " + tableName + "_temp_view" + "( \n" +
                    "k0 int, \n" +
                    "v1 string, \n" +
                    "v2 string, \n" +
                    "v3 string, \n" +
                    "v4 string, \n" +
                    "v5 string, \n" +
                    "v6 int, \n" +
                    "v7 int , \n" +
                    "v8 int , \n" +
                    "v9 int , \n" +
                    "v10 long , \n" +
                    "v11 string , \n" +
                    "v12 float , \n" +
                    "v13 double , \n" +
                    "v14 decimal(27,9) , \n" +
                    "v15 decimal(9,5), \n" +
                    "v16 decimal(18,10), \n" +
                    "v17 decimal(38,18) \n" +
                    ") USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(spark_table_sql);

                spark.sql(String.format("insert into %s values(1,'9999-12-31','9999-12-31 23:59:59','beijingaergertte','haidiansdvgerwwge','tcafvergtrwhtwrht',1,-128,-32768,-2147483648,-9223372036854775808,'-170141183460469231731687303715884105728',-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111);", tableName + "_temp_view"));
                spark.sql(String.format("insert into %s values(2,'0001-01-01','0001-01-01 00:00:01','beijing','haidian','asfarewgeragergre',0,127,32767,2147483647,9223372036854775807,'170141183460469231731687303715884105727',-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111);", tableName + "_temp_view"));
                spark.sql(String.format("insert into %s values(3,'2020-06-23','2020-06-23 00:00:04','','','',1,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000);", tableName + "_temp_view"));
                spark.sql(String.format("insert into %s values(4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);", tableName + "_temp_view"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
                "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
                "3,2020-06-23,2020-06-23T00:00:04,,,,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
                "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
                true,
                    false);
                executeSql(stmt, String.format("drop table %s", tableName), "", false, false);
            }
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }


    @Test
    public void JsonType(){
        /*
         * 插入json 数据类型
         */
        try {
                String tableName = "json_type_tbl";
                String srTableSql = "CREATE TABLE `json_type_tbl` ( \n" +
                    "k0 int, \n" +
                    "v2 json \n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(k0) \n" +
                    "COMMENT \"OLAP\" \n" +
                    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                    "PROPERTIES ( \n" +
                    "    \"replication_num\" = \"3\" \n" +
                    ");";
                executeSql(stmt, srTableSql, "", false, false);

                String sparkTableSql = "create table " + tableName + "_sink" + "( \n" +
                    "k0 int, \n" +
                    "v2 string \n" +
                    ") USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(sparkTableSql);


                spark.sql(String.format("insert into %s values(1,'{\"chemistry\":\"aaaa\"}');", tableName + "_sink"));
                spark.sql(String.format("insert into %s values(2,'{\"aaaa\":1}');", tableName + "_sink"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "2", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,{\"chemistry\": \"aaaa\"}\n" +
                "2,{\"aaaa\": 1}\n", 
                true,
                    false);

                // create temporay view
                sparkTableSql = "create TEMPORARY view " + tableName + "_temp_view" + "( \n" +
                    "k0 int, \n" +
                    "v2 string \n" +
                    ") USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(sparkTableSql);

                spark.sql(String.format("insert into %s values(3,'{\"bbb\":\"1111\"}');", tableName + "_temp_view"));
                spark.sql(String.format("insert into %s values(4,'{\"ccc\":null}');", tableName + "_temp_view"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,{\"chemistry\": \"aaaa\"}\n" +
                "2,{\"aaaa\": 1}\n" +
                "3,{\"bbb\": \"1111\"}\n" +
                "4,{\"ccc\": null}\n"
                , 
                true,
                    false);

                // create temporay view without column
                sparkTableSql = "create TEMPORARY view " + tableName + "_temp_view2" + "\n" +
                    " USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(sparkTableSql);

                spark.sql(String.format("insert into %s values(5,'{\"bbb\":\"2222\"}');", tableName + "_temp_view2"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "5", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,{\"chemistry\": \"aaaa\"}\n" +
                "2,{\"aaaa\": 1}\n" +
                "3,{\"bbb\": \"1111\"}\n" +
                "4,{\"ccc\": null}\n" +
                "5,{\"bbb\": \"2222\"}\n"
                , 
                true,
                    false);

        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }

    @Test
    public void ArrayType(){
        /*
         * 插入array 数据类型
         */
        try {
                String tableName = "array_type_tbl";
                String srTableSql = "CREATE TABLE `array_type_tbl` ( \n" +
                    "k0 int, \n" +
                    "v1 array<int> \n" +
                    ") ENGINE=OLAP \n" +
                    "DUPLICATE KEY(k0) \n" +
                    "COMMENT \"OLAP\" \n" +
                    "DISTRIBUTED BY HASH(k0) BUCKETS 3 \n" +
                    "PROPERTIES ( \n" +
                    "    \"replication_num\" = \"3\" \n" +
                    ");";
                executeSql(stmt, srTableSql, "", false, false);

                String sparkTableSql = "create table " + tableName + "_sink" + "( \n" +
                    "k0 int, \n" +
                    "v1 string \n" +
                    ") USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(sparkTableSql);


                spark.sql(String.format("insert into %s values(1,'[1]');", tableName + "_sink"));
                spark.sql(String.format("insert into %s values(2,'[2]');", tableName + "_sink"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "2", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,[1]\n" +
                "2,[2]\n", 
                true,
                    false);

                // create temporay view
                sparkTableSql = "create TEMPORARY view " + tableName + "_temp_view" + "( \n" +
                    "k0 int, \n" +
                    "v1 string \n" +
                    ") USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(sparkTableSql);

                spark.sql(String.format("insert into %s values(3,'[3]');", tableName + "_temp_view"));
                spark.sql(String.format("insert into %s values(4,'[4]');", tableName + "_temp_view"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,[1]\n" +
                "2,[2]\n" +
                "3,[3]\n" +
                "4,[4]\n", 
                true,
                    false);

                // create temporay view without column
                sparkTableSql = "create TEMPORARY view " + tableName + "_temp_view2" + "\n" +
                    " USING starrocks OPTIONS ( \n" +
                    "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
                    "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
                    "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
                    "  'spark.starrocks.table' = '" + tableName + "',  \n" +
                    "  'spark.starrocks.username' = '"+ USER +"',  \n" +
                    "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
                    "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
                    "  'spark.starrocks.write.ctl.enable-transaction' = 'false' \n" +
                    ");" ;
                spark.sql(sparkTableSql);

                spark.sql(String.format("insert into %s values(5,'[5]');", tableName + "_temp_view2"));

                executeSql(stmt, String.format("select count(1) from %s", tableName), "5", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,[1]\n" +
                "2,[2]\n" +
                "3,[3]\n" +
                "4,[4]\n" +
                "5,[5]\n"
                , 
                true,
                    false);

        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }


    @Test
    public void UpsertDeleteTest(){
    /*
    * 测试upsert delete
    */
        try {
            String tableName = "primary_table_upsert_delete_spark";
            String createTableSql = " CREATE TABLE `primary_table_upsert_delete_spark` (\n" +
              "  `k0` int(11) NOT NULL , \n" +
              "  `v1` date NULL , \n" +
              "  `v2` datetime NULL , \n" +
              "  `v3` char(20) NULL , \n" +
              "  `v4` varchar(20) NULL, \n" +
              "  `v5` varchar(65533) NULL \n" +
              ") ENGINE=OLAP \n" +
              "PRIMARY KEY(`k0`) \n" +
              "DISTRIBUTED BY HASH(`k0`) BUCKETS 3 \n" +
              "PROPERTIES ( \n" +
              "\"replication_num\" = \"3\" \n" +
              ");";
            executeSql(stmt, createTableSql, "", false, false);
            executeSql(stmt, "insert into primary_table_upsert_delete_spark values(1, '2020-01-01', '2020-01-01 01:01:01', 'aaaaaa', 'aaaaaa', 'aaaaaa')", "", false, false);
            executeSql(stmt, "insert into primary_table_upsert_delete_spark values(2, '2021-01-01', '2021-01-01 01:01:01', 'bbbbbb', 'bbbbbb', 'bbbbbb')", "", false, false);
            executeSql(stmt, "insert into primary_table_upsert_delete_spark values(3, '2022-01-01', '2022-01-01 01:01:01', 'bbbbbb', 'bbbbbb', 'bbbbbb')", "", false, false);

            String spark_table_sql = "create table primary_table_delete" + "( \n" +
            "k0 int, \n" +
            "v1 string, \n" +
            "v2 string, \n" +
            "v3 string, \n" +
            "v4 string, \n" +
            "v5 string, \n" +
            "__op int \n" +
            ") USING starrocks OPTIONS ( \n" +
            "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
            "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
            "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
            "  'spark.starrocks.table' = '" + tableName + "',  \n" +
            "  'spark.starrocks.username' = '"+ USER +"',  \n" +
            "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
            "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
            "  'spark.starrocks.write.ctl.enable-transaction' = 'false', \n" +
            "  'spark.starrocks.write.properties.columns' = 'k0,v1,v2,v3,v4,v5,__op', \n" +
            "  'spark.starrocks.infer.columns' = 'k0,v1,v2,v3,v4,v5,__op', \n" +
            "  'spark.starrocks.infer.column.k0.type' = 'int', \n" +
            "  'spark.starrocks.infer.column.v1.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v2.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v3.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v4.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v5.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.__op.type' = 'int' \n" +
            ");" ;
            spark.sql(spark_table_sql);

            spark.sql("insert into primary_table_delete values(1,'9999-12-31','9999-12-31 23:59:59','beijingaergertte','haidiansdvgerwwge','tcafvergtrwhtwrht',0);");
            spark.sql("insert into primary_table_delete values(2,'0001-01-01','0001-01-01 00:00:01','beijing','haidian','asfarewgeragergre',1);");
            spark.sql("insert into primary_table_delete values(3,'0001-01-01','0001-01-01 00:00:01','beijing','haidian','asfarewgeragergre',0);");

            executeSql(stmt, String.format("select count(1) from %s", tableName), "2", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht\n" +
            "3,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre\n",
            true,
                false);


            spark_table_sql = "create TEMPORARY view primary_table_delete_temp_view" + "( \n" +
            "k0 int, \n" +
            "v1 string, \n" +
            "v2 string, \n" +
            "v3 string, \n" +
            "v4 string, \n" +
            "v5 string, \n" +
            "__op int \n" +
            ") USING starrocks OPTIONS ( \n" +
            "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
            "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
            "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
            "  'spark.starrocks.table' = '" + tableName + "',  \n" +
            "  'spark.starrocks.username' = '"+ USER +"',  \n" +
            "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
            "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
            "  'spark.starrocks.write.ctl.enable-transaction' = 'false', \n" +
            "  'spark.starrocks.write.properties.columns' = 'k0,v1,v2,v3,v4,v5,__op', \n" +
            "  'spark.starrocks.infer.columns' = 'k0,v1,v2,v3,v4,v5,__op', \n" +
            "  'spark.starrocks.infer.column.k0.type' = 'int', \n" +
            "  'spark.starrocks.infer.column.v1.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v2.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v3.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v4.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v5.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.__op.type' = 'int' \n" +
            ");" ;
            spark.sql(spark_table_sql);

            spark.sql("insert into primary_table_delete_temp_view values(1,'2020-12-31','2020-12-31 23:59:59','aaa','aaa','aaa',0);");
            spark.sql("insert into primary_table_delete_temp_view values(3,'2020-01-01','2020-01-01 00:00:01','beijing','haidian','asfarewgeragergre',1);");

            executeSql(stmt, String.format("select count(1) from %s", tableName), "1", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,2020-12-31,2020-12-31T23:59:59,aaa,aaa,aaa\n",
            true,
                false);
 
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }



    @Test
    public void ParitalUpdateTest(){
    /*
    * 测试partial delete
    */
        try {
            String tableName = "primary_table_partial_update_spark";
            String createTableSql = " CREATE TABLE `primary_table_partial_update_spark` (\n" +
              "  `k0` int(11) NOT NULL , \n" +
              "  `v1` date NULL , \n" +
              "  `v2` datetime NULL , \n" +
              "  `v3` char(20) NULL , \n" +
              "  `v4` varchar(20) NULL, \n" +
              "  `v5` varchar(65533) NULL \n" +
              ") ENGINE=OLAP \n" +
              "PRIMARY KEY(`k0`) \n" +
              "DISTRIBUTED BY HASH(`k0`) BUCKETS 3 \n" +
              "PROPERTIES ( \n" +
              "\"replication_num\" = \"3\" \n" +
              ");";
            executeSql(stmt, createTableSql, "", false, false);
            executeSql(stmt, "insert into primary_table_partial_update_spark values(1, '2020-01-01', '2020-01-01 01:01:01', 'aaaaaa', 'aaaaaa', 'aaaaaa')", "", false, false);
            executeSql(stmt, "insert into primary_table_partial_update_spark values(2, '2021-01-01', '2021-01-01 01:01:01', 'bbbbbb', 'bbbbbb', 'bbbbbb')", "", false, false);
            executeSql(stmt, "insert into primary_table_partial_update_spark values(3, '2022-01-01', '2022-01-01 01:01:01', 'bbbbbb', 'bbbbbb', 'bbbbbb')", "", false, false);


            String spark_table_sql = "create table primary_table_partial_update" + "( \n" +
            "k0 int, \n" +
            "v1 string, \n" +
            "v2 string \n" +
            ") USING starrocks OPTIONS ( \n" +
            "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
            "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
            "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
            "  'spark.starrocks.table' = '" + tableName + "',  \n" +
            "  'spark.starrocks.username' = '"+ USER +"',  \n" +
            "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
            "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
            "  'spark.starrocks.write.ctl.enable-transaction' = 'false', \n" +
            "  'spark.starrocks.write.properties.partial_update' = 'true', \n" +
            "  'spark.starrocks.write.properties.columns' = 'k0,v1,v2', \n" +
            "  'spark.starrocks.infer.columns' = 'k0,v1,v2', \n" +
            "  'spark.starrocks.infer.column.k0.type' = 'int', \n" +
            "  'spark.starrocks.infer.column.v1.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v2.type' = 'string' \n" +
            ");" ;
            System.out.println(spark_table_sql);
            spark.sql(spark_table_sql);

            spark.sql("insert into primary_table_partial_update values(1,'9999-12-31','9999-12-31 23:59:59');");

            executeSql(stmt, String.format("select count(1) from %s", tableName), "3", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,9999-12-31,9999-12-31T23:59:59,aaaaaa,aaaaaa,aaaaaa\n" +
            "2,2021-01-01,2021-01-01T01:01:01,bbbbbb,bbbbbb,bbbbbb\n" +
            "3,2022-01-01,2022-01-01T01:01:01,bbbbbb,bbbbbb,bbbbbb\n",
            true,
                false);


            spark_table_sql = "create TEMPORARY view primary_table_partial_update_temp_view" + "( \n" +
            "k0 int, \n" +
            "v1 string, \n" +
            "v2 string \n" +
            ") USING starrocks OPTIONS ( \n" +
            "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
            "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
            "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
            "  'spark.starrocks.table' = '" + tableName + "',  \n" +
            "  'spark.starrocks.username' = '"+ USER +"',  \n" +
            "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
            "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
            "  'spark.starrocks.write.ctl.enable-transaction' = 'false', \n" +
            "  'spark.starrocks.write.properties.partial_update' = 'true', \n" +
            "  'spark.starrocks.write.properties.columns' = 'k0,v1,v2', \n" +
            "  'spark.starrocks.infer.columns' = 'k0,v1,v2', \n" +
            "  'spark.starrocks.infer.column.k0.type' = 'int', \n" +
            "  'spark.starrocks.infer.column.v1.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v2.type' = 'string' \n" +
            ");" ;
            spark.sql(spark_table_sql);

            spark.sql("insert into primary_table_partial_update_temp_view values(2,'9999-12-31','9999-12-31 23:59:59');");

            executeSql(stmt, String.format("select count(1) from %s", tableName), "3", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,9999-12-31,9999-12-31T23:59:59,aaaaaa,aaaaaa,aaaaaa\n" +
            "2,9999-12-31,9999-12-31T23:59:59,bbbbbb,bbbbbb,bbbbbb\n" +
            "3,2022-01-01,2022-01-01T01:01:01,bbbbbb,bbbbbb,bbbbbb\n",
            true,
                false);
         } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }


    @Test
    public void ConditionalUpdateTest(){
    /*
    * 测试condition delete
    */
        try {
            String tableName = "primary_table_conditional_update_spark";
            String createTableSql = " CREATE TABLE `primary_table_conditional_update_spark` (\n" +
              "  `k0` int(11) NOT NULL , \n" +
              "  `v1` date NULL , \n" +
              "  `v2` datetime NULL , \n" +
              "  `v3` char(20) NULL , \n" +
              "  `v4` varchar(20) NULL, \n" +
              "  `v5` varchar(65533) NULL \n" +
              ") ENGINE=OLAP \n" +
              "PRIMARY KEY(`k0`) \n" +
              "DISTRIBUTED BY HASH(`k0`) BUCKETS 3 \n" +
              "PROPERTIES ( \n" +
              "\"replication_num\" = \"3\" \n" +
              ");";
            executeSql(stmt, createTableSql, "", false, false);
            executeSql(stmt, "insert into primary_table_conditional_update_spark values(1, '2020-01-01', '2020-01-01 01:01:01', 'aaaaaa', 'aaaaaa', 'aaaaaa')", "", false, false);
            executeSql(stmt, "insert into primary_table_conditional_update_spark values(2, '2021-01-01', '2021-01-01 01:01:01', 'bbbbbb', 'bbbbbb', 'bbbbbb')", "", false, false);
            executeSql(stmt, "insert into primary_table_conditional_update_spark values(3, '2022-01-01', '2022-01-01 01:01:01', 'bbbbbb', 'bbbbbb', 'bbbbbb')", "", false, false);


            String spark_table_sql = "create table primary_table_conditional_update" + "( \n" +
            " k0 int, \n" +
            " v1 string, \n" +
            " v2 string, \n" +
            " v3 string, \n" +
            " v4 string, \n" +
            " v5 string \n" +
            ") USING starrocks OPTIONS ( \n" +
            "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
            "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
            "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
            "  'spark.starrocks.table' = '" + tableName + "',  \n" +
            "  'spark.starrocks.username' = '"+ USER +"',  \n" +
            "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
            "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
            "  'spark.starrocks.write.ctl.enable-transaction' = 'false', \n" +
            "  'spark.starrocks.write.properties.merge_condition' = 'v1', \n" +
            "  'spark.starrocks.write.properties.columns' = 'k0,v1,v2,v3,v4,v5', \n" +
            "  'spark.starrocks.infer.columns' = 'k0,v1,v2,v3,v4,v5', \n" +
            "  'spark.starrocks.infer.column.k0.type' = 'int', \n" +
            "  'spark.starrocks.infer.column.v1.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v2.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v3.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v4.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v5.type' = 'string' \n" +
            ");" ;
            System.out.println(spark_table_sql);
            spark.sql(spark_table_sql);

            spark.sql("insert into primary_table_conditional_update values(2,'2022-12-31','2022-12-31 23:59:59', 'beijing','haidian','asfarewgeragergre');");
            spark.sql("insert into primary_table_conditional_update values(3,'2001-07-31','2001-07-31 23:59:59', 'beijing','haidian','asfarewgeragergre');");

            executeSql(stmt, String.format("select count(1) from %s", tableName), "3", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,2020-01-01,2020-01-01T01:01:01,aaaaaa,aaaaaa,aaaaaa\n" +
            "2,2022-12-31,2022-12-31T23:59:59,beijing,haidian,asfarewgeragergre\n" +
            "3,2022-01-01,2022-01-01T01:01:01,bbbbbb,bbbbbb,bbbbbb\n",
            true,
                false);


            spark_table_sql = "create TEMPORARY view primary_table_conditional_update_temp_view" + "( \n" +
            " k0 int, \n" +
            " v1 string, \n" +
            " v2 string, \n" +
            " v3 string, \n" +
            " v4 string, \n" +
            " v5 string \n" +
            ") USING starrocks OPTIONS ( \n" +
            "  'spark.starrocks.fe.urls.http' = '"+ FE_URLS_HTTP + "',  \n"  +
            "  'spark.starrocks.fe.urls.jdbc' = '"+ FE_URLS_JDBC + "',  \n" +
            "  'spark.starrocks.database' = '"+ DATABASE +"',  \n" +
            "  'spark.starrocks.table' = '" + tableName + "',  \n" +
            "  'spark.starrocks.username' = '"+ USER +"',  \n" +
            "  'spark.starrocks.password' = '"+ PASS +"',  \n" +
            "  'spark.starrocks.write.properties.format' = 'csv',  \n" +
            "  'spark.starrocks.write.ctl.enable-transaction' = 'false', \n" +
            "  'spark.starrocks.write.properties.merge_condition' = 'v1', \n" +
            "  'spark.starrocks.write.properties.columns' = 'k0,v1,v2,v3,v4,v5', \n" +
            "  'spark.starrocks.infer.columns' = 'k0,v1,v2,v3,v4,v5', \n" +
            "  'spark.starrocks.infer.column.k0.type' = 'int', \n" +
            "  'spark.starrocks.infer.column.v1.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v2.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v3.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v4.type' = 'string', \n" +
            "  'spark.starrocks.infer.column.v5.type' = 'string' \n" +
            ");" ;
            System.out.println(spark_table_sql);
            spark.sql(spark_table_sql);

            spark.sql("insert into primary_table_conditional_update_temp_view values(2,'2023-12-31','2023-12-31 23:59:59', 'aaaaa','aaaaa','aaaaa');");
            spark.sql("insert into primary_table_conditional_update_temp_view values(3,'2001-07-31','2001-07-31 23:59:59', 'beijing','haidian','asfarewgeragergre');");

            executeSql(stmt, String.format("select count(1) from %s", tableName), "3", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,2020-01-01,2020-01-01T01:01:01,aaaaaa,aaaaaa,aaaaaa\n" +
            "2,2023-12-31,2023-12-31T23:59:59,aaaaa,aaaaa,aaaaa\n" +
            "3,2022-01-01,2022-01-01T01:01:01,bbbbbb,bbbbbb,bbbbbb\n",
            true,
                false);
 
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
    }
    }


    @Test
    public void SelectBasicTest(){
        /*
         * 测试spark 端查询
         */
        try {
            String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
            for (String tableName : tableNames) {
                String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
                executeSql(stmt, createTableSql, "", false, false);
                executeSql(stmt, String.format("insert into %s values(1,'9999-12-31','9999-12-31 23:59:59','beijingaergertte','haidiansdvgerwwge','tcafvergtrwhtwrht',1,-128,-32768,-2147483648,-9223372036854775808,'-170141183460469231731687303715884105728',-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111);", tableName), "", false, false);
                executeSql(stmt, String.format("insert into %s values(2,'0001-01-01','0001-01-01 00:00:01','beijing','haidian','asfarewgeragergre',0,127,32767,2147483647,9223372036854775807,'170141183460469231731687303715884105727',-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111);", tableName), "", false, false);
                executeSql(stmt, String.format("insert into %s values(3,'2020-06-23','2020-06-23 00:00:04','','','',1,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000);", tableName), "", false, false);
                executeSql(stmt, String.format("insert into %s values(4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);", tableName), "", false, false);

                executeSql(stmt, String.format("select count(1) from %s", tableName), "4", true, false);
                executeSql(stmt, String.format("select * from %s order by 1", tableName), 
                "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
                "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
                "3,2020-06-23,2020-06-23T00:00:04,,,,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
                "4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null\n", 
                true,
                    false);

                String spark_table_sql = "create TEMPORARY view " + tableName +"_temp_View \n" +
                " USING starrocks OPTIONS ( \n" +
                "  'starrocks.fenodes' = '"+ FE_URLS_HTTP + "',  \n"  +
                "  'starrocks.table.identifier' = '" + DATABASE + "." +tableName + "',  \n" +
                "  'user' = '"+ USER +"',  \n" +
                "  'password' = '"+ PASS +"' \n" +
                ");" ;
                System.out.println(spark_table_sql);
                spark.sql(spark_table_sql);

                Dataset<Row> dataset = spark.sql(String.format("select * from %s order by 1;", tableName +"_temp_View" ));
                List<Row> rowList = dataset.collectAsList();
                System.out.println(rowList.toString());

                String expected = "[[1,9999-12-31,9999-12-31 23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111], " +
                "[2,0001-01-01,0001-01-01 00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111], " +
                "[3,2020-06-23,2020-06-23 00:00:04,,,,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000], " +
                "[4,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]]";
                System.out.println(expected);
                Assert.assertEquals(expected.trim(), rowList.toString().trim());
            }
        } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
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
    
}