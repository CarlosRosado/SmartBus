USE emt_smartbus;

------------------------------------------------------
--- TABLAS MASTER SIN OUTLIERS
------------------------------------------------------


--- MASTER SENTIDOS SIN OUTLIERS

!sh echo "Generacion tabla master sentidos sin outliers.";


CREATE TEMPORARY TABLE tmp_nooutliers1 as 
SELECT *
FROM viajeros_sentido_master
WHERE tipo_linea='Nocturno' and (tramo>=23 or tramo<=7);

CREATE TEMPORARY TABLE tmp_nooutliers2 as 
SELECT *
FROM viajeros_sentido_master
WHERE tipo_linea<>'Nocturno' and tipo_linea<>'Trabajo' and tipo_linea<>'Otros' and (tramo<2 or tramo>=6);

CREATE TEMPORARY TABLE tmp_nooutliers3 as 
SELECT *
FROM viajeros_sentido_master
WHERE tipo_linea='Trabajo' or tipo_linea='Otros';


CREATE TABLE IF NOT EXISTS viajeros_sentido_master_nooutliers
(Fecha INT,
instante STRING,
parada INT,
titulo INT,
bus INT,
nviaje INT,
sentido INT,
viajeros INT,
tramo INT,
tipo_linea STRING,
mes INT,
dia INT,
anho INT,
dia_semana STRING,
festivo INT,
intensidad_evento INT,
id_medidor INT,
inten_lluvia DOUBLE,
lluvia INT,
inten_lluvia_tramo DOUBLE,
lluvia_tramo INT,
intensidad_trafico DOUBLE,
ocupacion_trafico INT,
carga_trafico INT,
vmed_trafico DOUBLE) partitioned by (Linea INT)
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapreduce.reduce.memory.mb=6120;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT INTO TABLE viajeros_sentido_master_nooutliers PARTITION (Linea) 
SELECT Fecha,instante,parada ,titulo, bus,nviaje,sentido,viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,id_medidor,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,Linea
FROM tmp_nooutliers1
DISTRIBUTE BY linea;

INSERT INTO TABLE viajeros_sentido_master_nooutliers PARTITION (Linea) 
SELECT Fecha,instante,parada ,titulo, bus,nviaje,sentido,viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,id_medidor,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,Linea
FROM tmp_nooutliers2
DISTRIBUTE BY linea;

INSERT INTO TABLE viajeros_sentido_master_nooutliers PARTITION (Linea) 
SELECT Fecha,instante,parada ,titulo, bus,nviaje,sentido,viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,id_medidor,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,Linea
FROM tmp_nooutliers3
DISTRIBUTE BY linea;

--- MASTER TRAMOS SIN OUTLIERS

!sh echo "Generacion tabla master tramos sin outliers.";


CREATE TEMPORARY TABLE tmp_tramos_nooutliers1 as 
SELECT *
FROM viajeros_tramos_master
WHERE tipo_linea='Nocturno' and (tramo>=23 or tramo<=7);

CREATE TEMPORARY TABLE tmp_tramos_nooutliers2 as 
SELECT *
FROM viajeros_tramos_master
WHERE tipo_linea<>'Nocturno' and tipo_linea<>'Trabajo' and tipo_linea<>'Otros' and (tramo<2 or tramo>=6);

CREATE TEMPORARY TABLE tmp_tramos_nooutliers3 as 
SELECT *
FROM viajeros_tramos_master
WHERE tipo_linea='Trabajo' or tipo_linea='Otros';


CREATE TABLE IF NOT EXISTS viajeros_tramos_master_nooutliers
(fecha INT,
sentido INT,
n_titulos INT,
n_bus INT,
n_viajeros INT,
tramo INT,
tipo_linea STRING,
mes INT,
dia INT,
anho INT,
dia_semana STRING,
festivo INT,
intensidad_evento INT,
inten_lluvia DOUBLE,
lluvia INT,
inten_lluvia_tramo DOUBLE,
lluvia_tramo INT,
intensidad_trafico DOUBLE,
ocupacion_trafico DOUBLE,
carga_trafico DOUBLE,
vmed_trafico DOUBLE) partitioned by (linea INT)
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET mapreduce.reduce.memory.mb=6120;
set hive.exec.max.dynamic.partitions.pernode=1000;


INSERT INTO TABLE viajeros_tramos_master_nooutliers PARTITION (Linea) 
SELECT fecha,sentido,n_titulos,n_bus,n_viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,linea
FROM tmp_tramos_nooutliers1
DISTRIBUTE BY linea;

INSERT INTO TABLE viajeros_tramos_master_nooutliers PARTITION (Linea) 
SELECT fecha,sentido,n_titulos,n_bus,n_viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,linea
FROM tmp_tramos_nooutliers2
DISTRIBUTE BY linea;

INSERT INTO TABLE viajeros_tramos_master_nooutliers PARTITION (Linea) 
SELECT fecha,sentido,n_titulos,n_bus,n_viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,linea
FROM tmp_tramos_nooutliers3
DISTRIBUTE BY linea;


