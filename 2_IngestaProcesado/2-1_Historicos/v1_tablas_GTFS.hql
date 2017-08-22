USE emt_smartbus;

------------------------------------------------------
--- INGESTA DE LOS DATOS GTFS
------------------------------------------------------

!sh echo "7.1. Ingesta calendar";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_calendar_antes 
(service_id STRING,
monday INT,
tuesday INT,
wednesday INT,
thursday INT,
friday INT,
saturday INT,
sunday INT,
start_date INT,
end_date INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_calendar
(service_id STRING,
monday INT,
tuesday INT,
wednesday INT,
thursday INT,
friday INT,
saturday INT,
sunday INT,
start_date INT,
end_date INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/calendar.txt' OVERWRITE INTO TABLE gtfs_calendar_antes;

INSERT INTO gtfs_calendar SELECT * FROM gtfs_calendar_antes where monday is not null;


!sh echo "7.2. Ingesta calendar_dates";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_calendar_dates_antes 
(service_id STRING,
date INT,
exception_type INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_calendar_dates
(service_id STRING,
date INT,
exception_type INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/calendar_dates.txt' OVERWRITE INTO TABLE gtfs_calendar_dates_antes;

INSERT INTO gtfs_calendar_dates SELECT * FROM gtfs_calendar_dates_antes where service_id<>'service_id';


!sh echo "7.3. Ingesta Frequencies";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_frequencies_antes 
(trip_id STRING,
start_time STRING,
end_time STRING,
headway_secs INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_frequencies
(trip_id STRING,
start_time STRING,
end_time STRING,
headway_secs INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/frequencies.txt' OVERWRITE INTO TABLE gtfs_frequencies_antes;

INSERT INTO gtfs_frequencies SELECT * FROM gtfs_frequencies_antes where trip_id<>'trip_id';



!sh echo "7.4. Ingesta Routes";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_routes_antes 
(route_id INT,
agency_id STRING,
route_short_name STRING,
route_long_name STRING,
route_desc STRING,
route_type INT,
route_url STRING,
route_color STRING,
route_text_color STRING) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_routes
(route_id INT,
agency_id STRING,
route_short_name STRING,
route_long_name STRING,
route_desc STRING,
route_type INT,
route_url STRING,
route_color STRING,
route_text_color STRING) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/routes.txt' OVERWRITE INTO TABLE gtfs_routes_antes;

INSERT INTO gtfs_routes SELECT * FROM gtfs_routes_antes where route_id is not null;


!sh echo "7.5. Ingesta Shapes";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_shapes_antes 
(shape_id STRING,
shape_pt_lat DOUBLE,
shape_pt_lon DOUBLE,
shape_pt_sequence INT,
shape_dist_traveled INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_shapes
(shape_id STRING,
shape_pt_lat DOUBLE,
shape_pt_lon DOUBLE,
shape_pt_sequence INT,
shape_dist_traveled INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/shapes.txt' OVERWRITE INTO TABLE gtfs_shapes_antes;

INSERT INTO gtfs_shapes SELECT * FROM gtfs_shapes_antes where shape_id<>'shape_id';


!sh echo "7.6. Ingesta STOP Times";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_stopTimes_antes 
(trip_id STRING,
arrival_time STRING,
departure_time STRING,
stop_id INT,
stop_sequence INT,
stop_headsign STRING,
pickup_type STRING,
drop_off_type STRING,
shape_dist_traveled INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_stopTimes
(trip_id STRING,
arrival_time STRING,
departure_time STRING,
stop_id INT,
stop_sequence INT,
stop_headsign STRING,
pickup_type STRING,
drop_off_type STRING,
shape_dist_traveled INT) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/stop_times.txt' OVERWRITE INTO TABLE gtfs_stopTimes_antes;

INSERT INTO gtfs_stopTimes SELECT * FROM gtfs_stopTimes_antes where trip_id<>'trip_id';



!sh echo "7.7. Ingesta STOPS ";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_stops_antes 
(stop_id INT,
stop_code INT,
stop_name STRING,
stop_desc STRING,
stop_lat DOUBLE,
stop_lon DOUBLE,
zone_id STRING,
stop_url STRING,
location_type STRING,
parent_station STRING) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_stops
(stop_id INT,
stop_code INT,
stop_name STRING,
stop_desc STRING,
stop_lat DOUBLE,
stop_lon DOUBLE,
zone_id STRING,
stop_url STRING,
location_type STRING,
parent_station STRING) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/stops.txt' OVERWRITE INTO TABLE gtfs_stops_antes;

INSERT INTO gtfs_stops SELECT * FROM gtfs_stops_antes where stop_id is not null;



!sh echo "7.8. Ingesta TRIPS ";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_trips_antes 
(route_id INT,
service_id STRING,
trip_id STRING,
trip_headsign STRING,
direction_id INT,
block_id STRING,
shape_id STRING) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_trips
(route_id INT,
service_id STRING,
trip_id STRING,
trip_headsign STRING,
direction_id INT,
block_id STRING,
shape_id STRING) 
row format delimited fields terminated BY ',' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/trips.txt' OVERWRITE INTO TABLE gtfs_trips_antes;

INSERT INTO gtfs_trips SELECT * FROM gtfs_trips_antes where route_id is not null;


!sh echo "7.9. Ingesta GTFS_Lineas ";

CREATE TEMPORARY TABLE IF NOT EXISTS gtfs_lineas_antes 
(shape_id STRING,
linea INT,
sentido INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TABLE IF NOT EXISTS gtfs_lineas
(shape_id STRING,
linea INT,
sentido INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/gtfs_lineas.csv' OVERWRITE INTO TABLE gtfs_lineas_antes;

INSERT INTO gtfs_lineas SELECT * FROM gtfs_lineas_antes where linea is not null;


!sh echo "7.10. JOIN para GTFS ";

CREATE TEMPORARY TABLE tmp_gtfs1 as
select a.*,b.agency_id,b.route_short_name,b.route_long_name,b.route_desc,b.route_type,b.route_color,b.route_text_color
from gtfs_trips a
INNER JOIN gtfs_routes b
ON (a.route_id=b.route_id);

CREATE TEMPORARY TABLE tmp_gtfs2 as
select a.*,b.arrival_time,b.departure_time,b.stop_id,b.stop_sequence,b.shape_dist_traveled
from tmp_gtfs1 a
INNER JOIN gtfs_stoptimes b
ON (a.trip_id=b.trip_id);

CREATE TEMPORARY TABLE tmp_gtfs3 as
select a.*,b.stop_code,b.stop_name,b.stop_desc,b.stop_lat,b.stop_lon,b.zone_id
from tmp_gtfs2 a
INNER JOIN gtfs_stops b
ON (a.stop_id=b.stop_id);


CREATE TABLE gtfs stored as orc as
select a.*,b.monday,b.tuesday,b.wednesday,b.thursday,b.friday,b.saturday,b.sunday,b.start_date,b.end_date
from tmp_gtfs3 a
INNER JOIN gtfs_calendar b
ON (a.service_id=b.service_id);




