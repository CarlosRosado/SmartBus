USE proyectoemt_smartbus;

------------------------------------------------------
--- INGESTA DE DATOS PARA EL TRAFICO
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS trafico_${hivevar:year_month}_antes
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

CREATE TEMPORARY TABLE IF NOT EXISTS trafico_${hivevar:year_month}
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

LOAD DATA INPATH '/user/crosado/Proyecto_EMT/transport/${hivevar:trafic_file}.csv' OVERWRITE INTO TABLE trafico_${hivevar:year_month}_antes;

INSERT INTO trafico_${hivevar:year_month} SELECT * FROM trafico_${hivevar:year_month}_antes where idelem is not null;


----Insertamos en el año completo

INSERT INTO trafico_${hivevar:year} SELECT * FROM trafico_${hivevar:year_month};


CREATE TEMPORARY TABLE IF NOT EXISTS trafico_madrid_${hivevar:year_month}_temp AS
SELECT idelem,fecha,identif,tipo_elem,Intensidad,Ocupacion,Carga,vmed,error,periodo_integracion 
FROM trafico_${hivevar:year_month};


CREATE TEMPORARY TABLE IF NOT EXISTS trafico_madrid_${hivevar:year_month}_aux AS
SELECT idelem,fecha,SUBSTR(fecha,0,4),SUBSTR(fecha,6,2),SUBSTR(fecha,9,2),SUBSTR(fecha,12,5),identif,tipo_elem,Intensidad,Ocupacion,Carga,vmed,error,periodo_integracion 
FROM trafico_madrid_${hivevar:year_month}_temp;


CREATE TEMPORARY TABLE IF NOT EXISTS trafico_madrid_${hivevar:year_month} AS
SELECT idelem,anho,mes,dia,instante,SUBSTR(instante,0,2),identif,Intensidad,Ocupacion,Carga,vmed,error,periodo_integracion 
FROM trafico_madrid_${hivevar:year_month}_aux;


CREATE TEMPORARY TABLE IF NOT EXISTS trafico_madrid_tramos_${hivevar:year_month} AS
SELECT idelem,anho,mes,dia,tramo,AVG(intensidad),SUM(Ocupacion),AVG(Carga),AVG(vmed) 
FROM trafico_madrid_${hivevar:year_month} 
WHERE error='N'
GROUP BY idelem,anho,mes,dia,tramo;


-- Insertamos en las tablas generales de trafico

INSERT INTO trafico_madrid_tramos
SELECT * from trafico_madrid_tramos_${hivevar:year_month};

INSERT INTO trafico_madrid
SELECT * from trafico_madrid_${hivevar:year_month};



------------------------------------------------------
--- INGESTA DE DATOS PARA LA AEMET
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS aemet_${hivevar:year_month}_antes 
(dia INT,
inten_lluvia DOUBLE,
inten_lluvia_00_06 DOUBLE,
inten_lluvia_06_12 DOUBLE,
inten_lluvia_12_18 DOUBLE,
inten_lluvia_18_24 DOUBLE,
lluvia INT,
lluvia_00_06 INT,
lluvia_06_12 INT,
lluvia_12_18 INT,
lluvia_18_24 INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TEMPORARY TABLE IF NOT EXISTS aemet_${hivevar:year_month}
(dia INT,
inten_lluvia DOUBLE,
inten_lluvia_00_06 DOUBLE,
inten_lluvia_06_12 DOUBLE,
inten_lluvia_12_18 DOUBLE,
inten_lluvia_18_24 DOUBLE,
lluvia INT,
lluvia_00_06 INT,
lluvia_06_12 INT,
lluvia_12_18 INT,
lluvia_18_24 INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT/transport/aemet_new_procesado.csv' OVERWRITE INTO TABLE aemet_${hivevar:year_month}_antes;

INSERT INTO aemet_${hivevar:year_month} SELECT * FROM aemet_${hivevar:year_month}_antes where dia is not null;


- Insertamos en la tabla general de la AEMET

INSERT INTO aemet
SELECT * from aemet_${hivevar:year_month};


------------------------------------------------------
--- INGESTA DE DATOS PARA LOS EVENTOS DEPORTIVOS
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS eventos_deportivos_${hivevar:year_month}_antes
(fecha INT,
estadioid INT,
equipo_local STRING,
equipo_visitante STRING,
hora STRING,
intensidad INT,
tramo_comienzo INT,
tramo_hora INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");

CREATE TEMPORARY TABLE IF NOT EXISTS eventos_deportivos_${hivevar:year_month}
(fecha INT,
estadioid INT,
equipo_local STRING,
equipo_visitante STRING,
hora STRING,
intensidad INT,
tramo_comienzo INT,
tramo_hora INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/calendario_futbol_procesado.csv' OVERWRITE INTO TABLE eventos_deportivos_${hivevar:year_month}_antes;

INSERT INTO eventos_deportivos_${hivevar:year_month} SELECT * FROM eventos_deportivos_${hivevar:year_month}_antes where fecha is not null;


CREATE TEMPORARY TABLE IF NOT EXISTS paradas_eventos_${hivevar:year_month}_antes
(parada INT,
estadioid INT,
equipo STRING) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");

CREATE TEMPORARY TABLE IF NOT EXISTS paradas_eventos_${hivevar:year_month}
(parada INT,
estadioid INT,
equipo STRING)  
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/paradas_equipos.csv' OVERWRITE INTO TABLE paradas_eventos_${hivevar:year_month}_antes;

INSERT INTO paradas_eventos_${hivevar:year_month} SELECT * FROM paradas_eventos_${hivevar:year_month}_antes where parada is not null;


CREATE TEMPORARY TABLE IF NOT EXISTS eventos_deportivos_paradas_${hivevar:year_month} AS
SELECT A.*,B.parada
FROM eventos_deportivos_${hivevar:year_month} A
LEFT JOIN paradas_eventos_${hivevar:year_month} B
ON (A.estadioid=B.estadioid);


--- Insertamos en la tabla general general

INSERT INTO eventos_deportivos_paradas SELECT * FROM eventos_deportivos_paradas_${hivevar:year_month};
