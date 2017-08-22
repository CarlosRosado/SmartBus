USE emt_smartbus;

------------------------------------------------------
--- INGESTA DE DATOS PARA LAS PLAZAS
------------------------------------------------------

!sh echo "2.1 Ingesta de la tabla para las plazas";

CREATE TEMPORARY TABLE IF NOT EXISTS plazas_antes 
(vehiculo INT,
marca STRING,
modelo STRING,
version INT,
combustible STRING,
plazas INT,
plazas_calidad INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TABLE IF NOT EXISTS plazas
(vehiculo INT,
marca STRING,
modelo STRING,
version INT,
combustible STRING,
plazas INT,
plazas_calidad INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/plazas.csv' OVERWRITE INTO TABLE plazas_antes;

INSERT INTO plazas SELECT * FROM plazas_antes where vehiculo is not null;

------------------------------------------------------
--- INGESTA DE DATOS PARA LOS TIPOS DE BILLETE
------------------------------------------------------

!sh echo "2.2 Ingesta de la tabla para los tipos";

CREATE TEMPORARY TABLE IF NOT EXISTS tipos_antes 
(titulo STRING,
descripcion STRING) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TABLE IF NOT EXISTS tipos
(titulo STRING,
descripcion STRING) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/tipos.csv' OVERWRITE INTO TABLE tipos_antes;

INSERT INTO tipos SELECT * FROM tipos_antes where titulo is not null;

------------------------------------------------------
--- INGESTA DE DATOS PARA EL CALENDARIO DE MADRID
------------------------------------------------------

!sh echo "2.3 Ingesta de la tabla el calendario de Madrid";

CREATE TEMPORARY TABLE IF NOT EXISTS calendario_antes 
(fecha INT,
mes INT,
dia INT,
anho INT,
dia_semana STRING,
festivo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TABLE IF NOT EXISTS calendario_madrid
(fecha INT,
mes INT,
dia INT,
anho INT,
dia_semana STRING,
festivo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/calendario_madrid.csv' OVERWRITE INTO TABLE calendario_antes;

INSERT INTO calendario_madrid SELECT * FROM calendario_antes where fecha is not null;


------------------------------------------------------
--- INGESTA DE DATOS PARA LA AEMET
------------------------------------------------------

!sh echo "2.4 Ingesta de la tabla de la AEMET";

CREATE TEMPORARY TABLE IF NOT EXISTS aemet_antes 
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


CREATE TABLE IF NOT EXISTS aemet
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

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/aemet_15_16_17_procesado.csv' OVERWRITE INTO TABLE aemet_antes;

INSERT INTO aemet SELECT * FROM aemet_antes where dia is not null;

------------------------------------------------------
--- INGESTA DE DATOS PARA LAS COORDENADAS DE LAS PARADAS
------------------------------------------------------

!sh echo "2.5 Ingesta de la tabla de las coordenadas de las paradas";

CREATE TEMPORARY TABLE IF NOT EXISTS geo_trafico_paradas_antes
(LINEA INT,
ETIQUETA STRING,
SENTIDO  INT,
PARADA INT,
POSX STRING,
POSY STRING,
GID INT,
TIPO_ELEM STRING,
IDLEM INT,
COD_CENT INT,
NOMBRE STRING) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TABLE IF NOT EXISTS geo_trafico_paradas
(LINEA INT,
ETIQUETA STRING,
SENTIDO  INT,
PARADA INT,
POSX STRING,
POSY STRING,
TIPO_ELEM STRING,
IDLEM INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/trafico_paradas.csv' OVERWRITE INTO TABLE geo_trafico_paradas_antes;

INSERT INTO geo_trafico_paradas SELECT LINEA,ETIQUETA,SENTIDO,PARADA,POSX,POSY,TIPO_ELEM,IDLEM FROM geo_trafico_paradas_antes where LINEA is not null;

------------------------------------------------------
--- INGESTA DE DATOS PARA LOS EVENTOS DEPORTIVOS
------------------------------------------------------

!sh echo "2.5 Ingesta de la tabla de los eventos deportivos;


CREATE TEMPORARY TABLE IF NOT EXISTS eventos_deportivos_antes
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

CREATE TEMPORARY TABLE IF NOT EXISTS eventos_deportivos
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


LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/calendario_futbol_procesado.csv' OVERWRITE INTO TABLE eventos_deportivos_antes;

INSERT INTO eventos_deportivos SELECT * FROM eventos_deportivos_antes where fecha is not null;


CREATE TEMPORARY TABLE IF NOT EXISTS paradas_eventos_antes
(parada INT,
estadioid INT,
equipo STRING) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");

CREATE TEMPORARY TABLE IF NOT EXISTS paradas_eventos
(parada INT,
estadioid INT,
equipo STRING)  
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/paradas_equipos.csv' OVERWRITE INTO TABLE paradas_eventos_antes;

INSERT INTO paradas_eventos SELECT * FROM paradas_eventos_antes where parada is not null;


CREATE TABLE IF NOT EXISTS eventos_deportivos_paradas
(fecha INT,
estadioid INT,
equipo_local STRING,
equipo_visitante STRING,
hora STRING,
intensidad INT,
tramo_comienzo INT,
tramo_hora INT,
parada INT)  
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

INSERT INTO eventos_deportivos_paradas 
SELECT A.*,B.parada
FROM eventos_deportivos A
LEFT JOIN paradas_eventos B
ON (A.estadioid=B.estadioid);


------------------------------------------------------
--- INGESTA DE DATOS PARA LOS TIPOS DE LINEA
------------------------------------------------------

!sh echo "2.6 Ingesta de la tabla para los tipos de lineas";

CREATE TEMPORARY TABLE IF NOT EXISTS tipos_lineas_antes 
(linea INT,
etiqueta STRING,
tipo_linea STRING) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");


CREATE TABLE IF NOT EXISTS tipos_lineas
(linea INT,
etiqueta STRING,
tipo_linea STRING)
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/tipos_lineas.csv' OVERWRITE INTO TABLE tipos_lineas_antes;

INSERT INTO tipos_lineas SELECT * FROM tipos_lineas_antes where linea is not null;

