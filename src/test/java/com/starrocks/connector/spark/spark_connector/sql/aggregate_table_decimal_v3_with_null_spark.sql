CREATE TABLE `aggregate_table_decimal_v3_with_null_spark` (
k0 int,
v1 date,
v2 datetime,
v3 char(20),
v4 varchar(20),
v5 string,
v6 boolean,
v7 tinyint max,
v8 smallint max,
v9 int min,
v10 bigint min,
v11 largeint min,
v12 float max,
v13 double max,
v14 decimal(27,9) max,
v15 decimal32(9,5) max,
v16 decimal64(18,10) max,
v17 decimal128(38,18) max
) ENGINE=OLAP
aggregate KEY(k0, v1, v2, v3, v4, v5, v6)
COMMENT "OLAP"
DISTRIBUTED BY HASH(k0) BUCKETS 3
PROPERTIES (
    "replication_num" = "3"
);
