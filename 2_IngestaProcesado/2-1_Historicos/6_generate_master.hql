USE emt_smartbus;

------------------------------------------------------
--- GENRERACION DE LAS TABLAS MASTER (SENTIDO Y TRAMOS)
------------------------------------------------------


--- JOIN con los tipos de linea

!sh echo "Join Sentido con los tipos de linea.";

CREATE TEMPORARY TABLE viajeros_sentido_tipos_lineas STORED AS ORC as
SELECT A.*,
(CASE
       WHEN B.tipo_linea is null THEN 'Otros'
       ELSE B.tipo_linea
END) as tipo_linea
from viajeros_sentido A
LEFT JOIN tipos_lineas B
ON (A.linea=B.linea);


--- JOIN con el calendario

!sh echo "Join Sentido con el calendario.";

CREATE TEMPORARY TABLE viajeros_sentido_festivos STORED AS ORC as 
SELECT A.*,b.mes,B. dia,B.anho,B.dia_semana,B.festivo
FROM viajeros_sentido_tipos_lineas A
LEFT JOIN calendario_madrid B
ON (A.fecha=B.fecha);

--- JOIN con los eventos deportivos

!sh echo "Join Sentido con los eventos deportivos.";

CREATE TEMPORARY TABLE viajeros_sentido_festivos_eventos STORED AS ORC as
SELECT A.*,B.intensidad as intensidad_evento
FROM viajeros_sentido_festivos A
LEFT JOIN eventos_deportivos_paradas B
ON (A.fecha=B.fecha AND A.parada=B.parada AND A.tramo=B.tramo_hora);


--- JOIN con los medidores de las paradas

!sh echo "Join Sentido con los medidores de las paradas.";

CREATE TABLE geo_trafico_paradas_nolineas STORED AS ORC as
SELECT parada,idlem
FROM geo_trafico_paradas 
GROUP BY parada,idlem;


CREATE TEMPORARY TABLE viajeros_sent_fest_event_geotrafic STORED AS ORC as
SELECT A.*,B.idlem as id_medidor
FROM viajeros_sentido_festivos_eventos A
LEFT JOIN geo_trafico_paradas_nolineas B
ON (A.parada=B.parada);


--- JOIN con la AEMET

!sh echo "Join Sentido con el tiempo de la AEMET.";

CREATE TEMPORARY TABLE viajeros_sent_fest_event_geotrafic_aemet STORED AS ORC as
SELECT A.*,B.inten_lluvia,B.lluvia, 
(CASE
       WHEN A.tramo>=0 and A.tramo<6 THEN B.inten_lluvia_00_06
       WHEN A.tramo>=6 and A.tramo<12 THEN B.inten_lluvia_06_12
       WHEN A.tramo>=12 and A.tramo<18 THEN B.inten_lluvia_12_18
       ELSE B.inten_lluvia_18_24

END) as inten_lluvia_tramo,
(CASE
       WHEN A.tramo>=0 and A.tramo<6 THEN B.lluvia_00_06
       WHEN A.tramo>=6 and A.tramo<12 THEN B.lluvia_06_12
       WHEN A.tramo>=12 and A.tramo<18 THEN B.lluvia_12_18
       ELSE B.lluvia_18_24

END) as lluvia_tramo
from viajeros_sent_fest_event_geotrafic A
LEFT JOIN aemet B
ON (A.fecha = B.dia);

--- JOIN con el trafico, y generacion de la tabla sentido master

!sh echo "Generacion tabla sentido master.";

CREATE TEMPORARY TABLE viajeros_sentido_master_noorc STORED AS ORC as
SELECT A.*, B.intensidad as intensidad_trafico,B.ocupacion as ocupacion_trafico,B.carga as carga_trafico,B.vmed as vmed_trafico
FROM viajeros_sent_fest_event_geotrafic_aemet A
LEFT JOIN trafico_madrid_tramos B
ON (A.tramo=B.tramo AND A.id_medidor=B.idelem AND A.anho=B.anho AND A.dia=B.dia AND A.mes=B.mes);


CREATE TABLE IF NOT EXISTS viajeros_sentido_master
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


INSERT INTO TABLE viajeros_sentido_master PARTITION (Linea) select Fecha,instante,parada ,titulo, bus,nviaje,sentido,viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,id_medidor,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,Linea
FROM viajeros_sentido_master_noorc DISTRIBUTE BY linea;


--- Generacion de la tabla tramos master

!sh echo "Generacion tabla tramos master.";

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_tramo_master_noorc STORED AS ORC as
SELECT fecha,linea,sentido,count(distinct titulo) as n_titulos,count(distinct bus) as n_bus,sum(viajeros) as n_viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,max(intensidad_evento) as intensidad_evento,avg(inten_lluvia) as inten_lluvia,max(lluvia) as lluvia,avg(inten_lluvia_tramo) as inten_lluvia_tramo,max(lluvia_tramo) as lluvia_tramo,avg(intensidad_trafico) as intensidad_trafico,avg(ocupacion_trafico) as ocupacion_trafico,avg(carga_trafico) as carga_trafico,avg(vmed_trafico) as vmed_trafico
FROM viajeros_sentido_master
GROUP BY fecha,linea,sentido,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo;


CREATE TABLE IF NOT EXISTS viajeros_tramos_master
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

INSERT INTO TABLE viajeros_tramos_master PARTITION (Linea) select fecha,sentido,n_titulos,n_bus,n_viajeros,tramo,tipo_linea,mes,dia,anho,dia_semana,festivo,intensidad_evento,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,linea
FROM viajeros_tramo_master_noorc DISTRIBUTE BY linea;

--- BORRADO DE TABLAS AUXILIARES

drop table viajeros_2015;
drop table viajeros_2016;
drop table viajeros_2017;
