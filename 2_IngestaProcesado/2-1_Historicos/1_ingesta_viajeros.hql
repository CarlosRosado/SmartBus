CREATE DATABASE IF NOT EXISTS emt_smartbus;

USE emt_smartbus;

------------------------------------------------------
--- INGESTA DE DATOS PARA VIAJEROS
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year_month}_antes
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year_month}
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/${hivevar:year_month}.csv' OVERWRITE INTO TABLE viajeros_${hivevar:year_month}_antes;

INSERT INTO viajeros_${hivevar:year_month} SELECT * FROM viajeros_${hivevar:year_month}_antes where fecha is not null;

--- INSERTAR TODOS LOS DATOS EN UNA TABLA

CREATE TABLE IF NOT EXISTS viajeros_${hivevar:year}
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

INSERT INTO TABLE viajeros_${hivevar:year} select * from viajeros_${hivevar:year_month};


