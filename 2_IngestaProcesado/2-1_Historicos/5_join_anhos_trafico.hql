USE emt_smartbus;

------------------------------------------------------
--- JOIN PARA LOS DATOS DEL TRAFICO
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS trafico_madrid_temp 
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


INSERT INTO trafico_madrid_temp SELECT * FROM trafico_2015;

INSERT INTO trafico_madrid_temp SELECT * FROM trafico_2016;

INSERT INTO trafico_madrid_temp SELECT * FROM trafico_2017;


CREATE TEMPORARY TABLE IF NOT EXISTS trafico_madrid_aux
(idelem INT,
fecha STRING,
anho INT,
mes INT,
dia INT,
instante STRING,
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

INSERT INTO trafico_madrid_aux SELECT idelem,fecha,SUBSTR(fecha,0,4),SUBSTR(fecha,6,2),SUBSTR(fecha,9,2),SUBSTR(fecha,12,5),identif,tipo_elem,Intensidad,Ocupacion,Carga,vmed,error,periodo_integracion 
FROM trafico_madrid_temp;



CREATE TABLE IF NOT EXISTS trafico_madrid
(idelem INT,
anho INT,
mes INT,
dia INT,
instante STRING,
tramo INT,
identif STRING,
Intensidad INT,
Ocupacion INT,
Carga INT,
vmed INT,
error STRING,
periodo_integracion INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

INSERT INTO trafico_madrid
SELECT idelem,anho,mes,dia,instante,SUBSTR(instante,0,2),identif,Intensidad,Ocupacion,Carga,vmed,error,periodo_integracion 
FROM trafico_madrid_aux;


CREATE TABLE IF NOT EXISTS trafico_madrid_tramos
(idelem INT,
anho INT,
mes INT,
dia INT,
tramo INT,
Intensidad DOUBLE,
Ocupacion INT,
Carga INT,
vmed DOUBLE) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");


INSERT INTO trafico_madrid_tramos
SELECT idelem,anho,mes,dia,tramo,AVG(intensidad),SUM(Ocupacion),AVG(Carga),AVG(vmed) 
FROM trafico_madrid 
WHERE error='N'
GROUP BY idelem,anho,mes,dia,tramo;
