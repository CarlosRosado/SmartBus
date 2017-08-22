USE proyectoemt_smartbus;

------------------------------------------------------
--- INGESTA PARA LAS TABLAS MASTER
------------------------------------------------------

-1. JOIN CON EL CALENDARIO

CREATE TEMPORARY TABLE viajeros_sentido_festivos_${hivevar:year_month} as 
SELECT A.*,b.mes,B. dia,B.anho,B.dia_semana,B.festivo
FROM viajeros_${hivevar:year}_sentidotemp A
LEFT JOIN calendario_madrid B
ON (A.fecha=B.fecha);


-2. JOIN CON LOS EVENTOS DEPORTIVOS

CREATE TEMPORARY TABLE viajeros_sentido_festivos_eventos_${hivevar:year_month}  as
SELECT A.*,B.intensidad as intensidad_evento
FROM viajeros_sentido_festivos_${hivevar:year_month} A
LEFT JOIN eventos_deportivos_paradas_${hivevar:year_month} B
ON (A.fecha=B.fecha AND A.parada=B.parada AND A.tramo=B.tramo_hora);


-3. JOIN PARA ASIGNAR LOS MEDIDORES A LAS PARADAS   (TODO: REGENERAR DESDE AQUI)


CREATE TEMPORARY TABLE viajeros_sent_fest_event_geotrafic_${hivevar:year_month} as
SELECT A.*,B.idlem as id_medidor
FROM viajeros_sentido_festivos_eventos_${hivevar:year_month} A
LEFT JOIN geo_trafico_paradas_nolineas B
ON (A.parada=B.parada);

-4. JOIN CON LA AEMET

CREATE TEMPORARY TABLE viajeros_sent_fest_event_geotrafic_aemet_${hivevar:year_month} as
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
from viajeros_sent_fest_event_geotrafic_${hivevar:year_month} A
LEFT JOIN aemet_${hivevar:year_month} B
ON (A.fecha = B.dia);


-5. JOIN CON EL TRAFICO DE MADRID

CREATE TEMPORARY TABLE viajeros_sentido_master_noorc_${hivevar:year_month} as
SELECT A.*, B.intensidad as intensidad_trafico,B.ocupacion as ocupacion_trafico,B.carga as carga_trafico,B.vmed as vmed_trafico
FROM viajeros_sent_fest_event_geotrafic_aemet_${hivevar:year_month} A
LEFT JOIN trafico_madrid_tramos_${hivevar:year_month} B
ON (A.tramo=B.tramo AND A.id_medidor=B.idelem AND A.anho=B.anho AND A.dia=B.dia AND A.mes=B.mes);


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE viajeros_sentido_master PARTITION (Linea) select Fecha,instante,parada ,titulo, bus,nviaje,sentido,viajeros,tramo,mes,dia,anho,dia_semana,festivo,intensidad_evento,id_medidor,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,Linea
FROM viajeros_sentido_master_noorc_${hivevar:year_month} DISTRIBUTE BY linea;


-- Ingesta para la master de tramos

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_tramo_master_prev_${hivevar:year_month} as
SELECT fecha,linea,sentido,count(distinct titulo) as n_titulos,count(distinct bus) as n_bus,sum(viajeros) as n_viajeros,tramo,mes,dia,anho,dia_semana,festivo,max(intensidad_evento) as intensidad_evento,avg(inten_lluvia) as inten_lluvia,max(lluvia) as lluvia,avg(inten_lluvia_tramo) as inten_lluvia_tramo,max(lluvia_tramo) as lluvia_tramo,avg(intensidad_trafico) as intensidad_trafico,avg(ocupacion_trafico) as ocupacion_trafico,avg(carga_trafico) as carga_trafico,avg(vmed_trafico) as vmed_trafico
FROM viajeros_sentido_master_noorc_${hivevar:year_month}
GROUP BY fecha,linea,sentido,tramo,mes,dia,anho,dia_semana,festivo;


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


INSERT INTO TABLE viajeros_tramos_master PARTITION (Linea) select fecha,sentido,n_titulos,n_bus,n_viajeros,tramo,mes,dia,anho,dia_semana,festivo,intensidad_evento,inten_lluvia,lluvia,inten_lluvia_tramo,lluvia_tramo,intensidad_trafico,ocupacion_trafico,carga_trafico,vmed_trafico,linea
FROM viajeros_tramo_master_prev_${hivevar:year_month} DISTRIBUTE BY linea;

