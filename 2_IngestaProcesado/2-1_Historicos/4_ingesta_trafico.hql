USE emt_smartbus;

------------------------------------------------------
--- INGESTA DE DATOS PARA EL TRAFICO (2015/16/17)
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS trafico_${hivevar:file_1516}_antes
(idelem INT,
fecha STRING,
identif STRING,
tipo_elem STRING,
Intensidad INT,
Ocupacion INT,
Carga INT,
vmed INT,
error STRING,
periodo_integracion INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");

CREATE TEMPORARY TABLE IF NOT EXISTS trafico_${hivevar:file_1516}
(idelem INT,
fecha STRING,
identif STRING,
tipo_elem STRING,
Intensidad INT,
Ocupacion INT,
Carga INT,
vmed INT,
error STRING,
periodo_integracion INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/${hivevar:file_1516}.csv' OVERWRITE INTO TABLE trafico_${hivevar:file_1516}_antes;

INSERT INTO trafico_${hivevar:file_1516} SELECT * FROM trafico_${hivevar:file_1516}_antes where idelem is not null;

--- Insertamos todo en una misma tabla

CREATE TEMPORARY TABLE IF NOT EXISTS trafico_${hivevar:year}
(idelem INT,
fecha STRING,
identif STRING,
tipo_elem STRING,
Intensidad INT,
Ocupacion INT,
Carga INT,
vmed INT,
error STRING,
periodo_integracion INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO trafico_${hivevar:year} SELECT * FROM trafico_${hivevar:file_1516};
