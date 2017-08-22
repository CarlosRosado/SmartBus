USE emt_smartbus;

------------------------------------------------------
--- INGESTA DE DATOS PARA EL SENTIDO Y TRAMOS
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_new1
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

INSERT INTO viajeros_${hivevar:year}_new1 SELECT *,cast(SUBSTR(instante,0,2) as int) as tramo from viajeros_${hivevar:year} WHERE fecha is not null;


--- Extrapolamos el Sentido 0s por 1s o por 2s: (group by y luego el max de sentido)


CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_join
(Fecha INT,
Linea INT,
Bus INT,
nviaje INT,
sentido INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO TABLE viajeros_${hivevar:year}_join
SELECT fecha,linea,bus,nviaje,MAX(sentido) FROM viajeros_${hivevar:year}_new1 GROUP BY fecha,linea,bus,nviaje;


------------------------
----------------- CREACION DE LAS TABLAS TEMPORALES PREVIAS
-----------------------

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_aux
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO TABLE viajeros_${hivevar:year}_aux
SELECT u.fecha,u.instante,u.linea,u.parada,u.titulo,u.bus,u.nviaje,j.sentido,u.viajeros,u.tramo FROM viajeros_${hivevar:year}_new1 u
LEFT JOIN viajeros_${hivevar:year}_join j
ON (u.fecha = j.fecha AND u.linea = j.linea AND u.bus=j.bus AND u.nviaje=j.nviaje);


CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_aux_tmp2
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT,
Sentido_recal INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO TABLE viajeros_${hivevar:year}_aux_tmp2 
SELECT u.*,j.sentido FROM viajeros_${hivevar:year}_aux u
LEFT JOIN paradas_sentido j
ON (u.linea = j.linea AND u.parada = j.parada);


CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_aux_tmp3
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT,
Sentido_recal INT,
Sentido_recalculado INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO viajeros_${hivevar:year}_aux_tmp3
SELECT u.*,
CASE
   WHEN u.sentido_recal is null THEN u.sentido
   ELSE u.sentido_recal
END
FROM viajeros_${hivevar:year}_aux_tmp2 u;


--- Creamos la tabla final para el sentido

CREATE TABLE IF NOT EXISTS viajeros_${hivevar:year}_sentido
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO viajeros_${hivevar:year}_sentido 
SELECT fecha,instante,linea,parada,titulo,bus,nviaje,Sentido_recalculado,viajeros,tramo 
FROM viajeros_${hivevar:year}_aux_tmp3 
WHERE Sentido_recalculado!=0;


CREATE TABLE IF NOT EXISTS viajeros_${hivevar:year}_tramo
(Fecha INT,
Linea INT,
sentido INT,
viajeros INT,
tramo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO viajeros_${hivevar:year}_tramo
SELECT fecha,linea,sentido,sum(viajeros),tramo 
FROM viajeros_${hivevar:year}_sentido 
GROUP BY fecha,linea,sentido,tramo;


---Generacion de la tabla de sentido

CREATE TABLE IF NOT EXISTS viajeros_sentido
(Fecha INT,
Instante STRING,
Linea INT,
Parada INT,
Titulo INT,
Bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO viajeros_sentido SELECT * FROM viajeros_${hivevar:year}_sentido;

---Generamos las tablas de tramos

CREATE TABLE IF NOT EXISTS viajeros_tramo
(Fecha INT,
Linea INT,
sentido INT,
viajeros INT,
tramo INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO viajeros_tramo SELECT * FROM viajeros_${hivevar:year}_tramo;
