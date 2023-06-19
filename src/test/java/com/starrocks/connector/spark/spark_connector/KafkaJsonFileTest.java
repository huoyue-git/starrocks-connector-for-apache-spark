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

public class KafkaJsonFileTest {
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
    public static void drop_objcet() {
        
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
    public void supportDataBasicTypeJsonKafkaBatch(){
    /*
     * 从kafka中导入json文件格式的基础数据类型到starrocks中, batch
     */
    try {
        SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .appName("Application")
            .getOrCreate();

        sparkSession.conf().set("spark.sql.streaming.checkpointLocation", "/home/disk4/huoyue//tmp/spark/");
        sparkSession.conf().set("spark.default.parallelism", 2);

        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset<Row> readDf = sqlContext.read()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_json")
            .load();

        String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
        for (String tableName : tableNames) {
            String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
            executeSql(stmt, createTableSql, "", false, false);
        
            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", tableName);
            options.put("spark.starrocks.username", "root");
            options.put("spark.starrocks.password", "");
            options.put("spark.starrocks.write.properties.format", "json");
            options.put("spark.starrocks.write.properties.strip_outer_array", "true");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");

            readDf.map(new KafkaMap(), Encoders.bean(KafkaMessage.class))
                .write()
                .format("starrocks_writer")
                .mode(SaveMode.Append)
                .options(options)
                .save();
            executeSql(stmt, "select count(1) from " + tableName, "4", true, false);
            executeSql(stmt, String.format("select * from %s order by 1", tableName), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,-170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
            "3,2020-06-23,2020-06-23T00:00:04,,,,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
            "4,null,null,null,null,null,false,null,null,null,null,null,null,null,null,null,null,null\n", 
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
    public void supportDataBasicTypeJsonKafkaStream(){
    /*
     * 从kafka中导入json文件格式的基础数据类型到starrocks中, stream
     */
    try {
        SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .appName("Application")
            .getOrCreate();

        sparkSession.conf().set("spark.sql.streaming.checkpointLocation", "/home/disk4/huoyue//tmp/spark/");
        sparkSession.conf().set("spark.default.parallelism", 2);

        SQLContext sqlContext = new SQLContext(sparkSession);
        Dataset<Row> df = sqlContext.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_IP_PORT)
            .option("subscribe", "topic_kafka_spark_json")
            .option("startingOffsets", "earliest")
            .option("checkpointLocation", "/home/disk1/xiaolingfeng/tmp/spark/")
            .option("maxOffsetsPerTrigger", 1000000)
            .load();


        String[] tableNames = {"duplicate_table_decimal_v3_with_null_spark", "aggregate_table_decimal_v3_with_null_spark", "primary_table_decimal_v3_with_null_spark", "unique_table_decimal_v3_with_null_spark", "duplicate_table_decimal_v3_with_null_par_spark", "aggregate_table_decimal_v3_with_null_par_spark", "primary_table_decimal_v3_with_null_par_spark", "unique_table_decimal_v3_with_null_par_spark"};
        for (String tableName : tableNames) {
            String createTableSql = getSqlFromFile(SQL_FILE_PATH, tableName.concat(".sql"));
            executeSql(stmt, createTableSql, "", false, false);

            Map<String, String> options = new HashMap<>();
            options.put("spark.starrocks.conf", "write");
            options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
            options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
            options.put("spark.starrocks.database", DATABASE);
            options.put("spark.starrocks.table", tableName);
            options.put("spark.starrocks.username", "root");
            options.put("spark.starrocks.password", "");
            options.put("spark.starrocks.write.properties.format", "json");
            options.put("spark.starrocks.write.properties.strip_outer_array", "true");
            options.put("spark.starrocks.write.ctl.enable-transaction", "false");

        df.map(new KafkaMap(), Encoders.bean(KafkaMessage.class))
            .writeStream()
            .trigger(Trigger.ProcessingTime(50000))
            .format("starrocks_writer")
            .outputMode(OutputMode.Append())
            .options(options)
            .start().awaitTermination(20000);
        executeSql(stmt, "select count(1) from " + tableName, "4", true, false);
        executeSql(stmt, String.format("select * from %s order by 1", tableName), 
        "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht,true,-128,-32768,-2147483648,-9223372036854775808,-170141183460469231731687303715884105728,-3.115,-3.14159,111111111111111111.111111111,1111.11111,11111111.1111111111,11111111111111111111.111111111111111111\n" +
        "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre,false,127,32767,2147483647,9223372036854775807,-170141183460469231731687303715884105727,-3.115,-3.14159,-111111111111111111.111111111,-1111.11111,-11111111.1111111111,-11111111111111111111.111111111111111111\n" +
        "3,2020-06-23,2020-06-23T00:00:04,,,,true,-124,-32764,-2147483644,-9223372036854775804,-18446744073709551612,-2.7,-3.1,12345.123456789,123.12300,1234.1234500000,123456.123456789000000000\n" +
        "4,null,null,null,null,null,false,null,null,null,null,null,null,null,null,null,null,null\n", 
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
    public void supportDataBasicTypeJsonLocal() {
    /*
     * 从本地读取json 文件写入
     */
        executeSql(stmt, "CREATE TABLE `test_table` (\n" +
        "    `k0` int,\n" +
        "    `v1` date,\n" +
        "    `v2` datetime,\n" +
        "    `v3` char(20),\n" +
        "    `v4` varchar(20),\n" +
        "    `v5` string\n" +
        "--    `v6` boolean,\n" +
        "--    `v7` tinyint,\n" +
        "--    `v8` smallint,\n" +
        "--    `v9` int,\n" +
        "--    `v10` bigint,\n" +
        "--    `v11` largeint,\n" +
        "--    `v12` float,\n" +
        "--    `v13` double,\n" +
        "--    `v14` decimal(27,9)\n" +
        "--    `v15` decimal32(9,5),\n" +
        "--    `v16` decimal64(18,10),\n" +
        "--    `v17` decimal128(38,18)\n" +
        ") ENGINE=OLAP\n" +
        "duplicate KEY(`k0`)\n" +
        "COMMENT \"OLAP\"\n" +
        "DISTRIBUTED BY HASH(`k0`) BUCKETS 3\n" +
        "PROPERTIES (\n" +
        "    \"replication_num\" = \"3\" \n" +
        ");", "", false, false);
        String tableName = "test_table";
        // read json data
        Dataset<Row> df = spark.read().json("src/test/java/com/starrocks/connector/spark/spark_connector/data/json_data2.json");
        System.out.println("preprare show");
        df.show();
        System.out.println("show end");
        Map<String, String> options = new HashMap<>();
        options.put("spark.starrocks.conf", "write");
        options.put("spark.starrocks.fe.urls.http", String.format("%s:%s", FE_IP, FE_HTTPPORT));
        options.put("spark.starrocks.fe.urls.jdbc", String.format("jdbc:mysql://%s:%s", FE_IP, FE_QUERYPORT));
        options.put("spark.starrocks.database", DATABASE);
        options.put("spark.starrocks.table", tableName);
        options.put("spark.starrocks.username", USER);
        options.put("spark.starrocks.password", PASS);
        options.put("spark.starrocks.write.properties.format", "json");
         
        try {
        //    df.selectExpr("k0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8" ,"v9" ,"v10", "v12", "v13", "v14")
        // df.selectExpr("cast(k0 as int)","cast(v1 as date)","cast(v2 as timestamp)","cast(v3 as string)", "cast(v4 as string)", "cast(v5 as string)")
            df.selectExpr("k0","v1","v2","v3", "v4", "v5")
            .write()
            .format("starrocks_writer")
            .mode(SaveMode.Append)
            .options(options)
            .save();

            executeSql(stmt, String.format("select * from test_table order by 1"), 
            "1,9999-12-31,9999-12-31T23:59:59,beijingaergertte,haidiansdvgerwwge,tcafvergtrwhtwrht\n" +
            "2,0001-01-01,0001-01-01T00:00:01,beijing,haidian,asfarewgeragergre\n" +
            "3,2020-06-23,2020-06-23T00:00:04,,,\n" +
            "4,null,null,null,null,null",
                true,
                    false);

        } catch (Exception e) {
            e.printStackTrace();
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

    public static class SimpleData implements Serializable {
        private String dt;
        private Integer id;
        private String name;

        public SimpleData() {

        }

        public SimpleData(String dt, Integer id, String name) {
            this.dt = dt;
            this.id = id;
            this.name = name;
        }

        public String getDt() {
            return dt;
        }

        public void setDt(String dt) {
            this.dt = dt;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class JsonMap implements MapFunction<Row, KafkaMessage> {

        @Override
        public KafkaMessage call(Row row) throws Exception {
            String json = row.json();
            return JSON.parseObject(json, KafkaMessage.class);
        }
    }

    public static class KafkaMap implements MapFunction<Row, KafkaMessage> {

        @Override
        public KafkaMessage call(Row row) throws Exception {
            int i = row.fieldIndex("value");
            byte[] bytes = (byte[]) row.get(i);
            System.out.println(new String(bytes));
            return JSON.parseObject((byte[]) row.get(i), KafkaMessage.class);
        }
    }

    public static class KafkaMessage implements Serializable {
        private Integer k0;
        private String v1;
        private String v2;
        private String v3;
        private String v4;
        private String v5;
        private Integer v6;
        private Integer v7;
        private Integer v8;
        private Integer v9;
        private Long v10;
        private String v11;
        private Float v12;
        private Double v13;
        private BigDecimal v14;
        private BigDecimal v15;
        private BigDecimal v16;
        private BigDecimal v17;

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

        public Integer getV6() {
            return v6;
        }

        public void setV6(Integer v6) {
            this.v6 = v6;
        }

        public void setV6(Boolean v6) {
            this.v6 = v6 == null || !v6 ? 0 : 1;
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

        public String getV11() {
            return v11;
        }

        public void setV11(String v11) {
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

