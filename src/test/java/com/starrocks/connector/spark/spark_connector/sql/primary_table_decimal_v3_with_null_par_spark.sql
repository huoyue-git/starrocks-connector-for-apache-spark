CREATE TABLE `primary_table_decimal_v3_with_null_par_spark` (
k0 int,
v1 date,
v2 datetime,
v3 char(20),
v4 varchar(20),
v5 string,
v6 boolean,
v7 tinyint ,
v8 smallint ,
v9 int ,
v10 bigint ,
v11 largeint ,
v12 float ,
v13 double ,
v14 decimal(27,9) ,
v15 decimal32(9,5),
v16 decimal64(18,10),
v17 decimal128(38,18)
) ENGINE=OLAP
primary KEY(k0)
COMMENT "OLAP"
PARTITION BY RANGE (k0)
(
    PARTITION p1 VALUES LESS THAN ("1"),
    PARTITION p2 VALUES LESS THAN ("2"),
    PARTITION p3 VALUES LESS THAN ("4"),
    PARTITION p4 VALUES LESS THAN MAXVALUE
)
DISTRIBUTED BY HASH(k0) BUCKETS 3
PROPERTIES (
    "replication_num" = "3"
);
