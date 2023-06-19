CREATE TABLE `trips_mergetree`
(
    `pickup_datetime` DateTime ,
    `trip_id` INT ,
    `vendor_id` String ,
    `pickup_date` Date ,
    `dropoff_date` Date ,
    `dropoff_datetime` DateTime ,
    `store_and_fwd_flag` TINYINT ,
    `rate_code_id` TINYINT ,
    `pickup_longitude` DOUBLE ,
    `pickup_latitude` DOUBLE ,
    `dropoff_longitude` DOUBLE ,
    `dropoff_latitude` DOUBLE ,
    `passenger_count` TINYINT ,
    `trip_distance` DOUBLE ,
    `fare_amount` FLOAT ,
    `extra` FLOAT ,
    `mta_tax` FLOAT ,
    `tip_amount` FLOAT ,
    `tolls_amount` FLOAT ,
    `ehail_fee` FLOAT ,
    `improvement_surcharge` FLOAT ,
    `total_amount` FLOAT ,
    `payment_type_` String ,
    `trip_type` TINYINT ,
    `pickup` String ,
    `dropoff` String ,
    `cab_type` String ,
    `pickup_nyct2010_gid` TINYINT ,
    `pickup_ctlabel` FLOAT ,
    `pickup_borocode` TINYINT ,
    `pickup_boroname` String ,
    `pickup_ct2010` String ,
    `pickup_boroct2010` String ,
    `pickup_cdeligibil` String ,
    `pickup_ntacode` String ,
    `pickup_ntaname` String ,
    `pickup_puma` SMALLINT ,
    `dropoff_nyct2010_gid` TINYINT ,
    `dropoff_ctlabel` FLOAT ,
    `dropoff_borocode` TINYINT ,
    `dropoff_boroname` String ,
    `dropoff_ct2010` String ,
    `dropoff_boroct2010` String ,
    `dropoff_cdeligibil` String ,
    `dropoff_ntacode` String ,
    `dropoff_ntaname` String ,
    `dropoff_puma` SMALLINT
)
DUPLICATE KEY (pickup_datetime, trip_id, vendor_id)
PARTITION BY RANGE (pickup_date) (
   START ("2009-01-01") END ("2017-01-01") EVERY (INTERVAL 1 YEAR)
)
DISTRIBUTED BY HASH(trip_id) BUCKETS 10;