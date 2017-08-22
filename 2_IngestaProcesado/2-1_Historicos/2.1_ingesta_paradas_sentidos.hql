USE emt_smartbus;

---Nuevo dataset de paradas vs sentidos


CREATE TEMPORARY TABLE IF NOT EXISTS paradas_sentido_antes
(linea INT,
parada INT,
Sentido INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as textfile
tblproperties("skip.header.line.count"="0");

CREATE TABLE IF NOT EXISTS paradas_sentido
(linea INT,
parada INT,
Sentido INT) 
row format delimited fields terminated BY '\073' lines terminated BY '\n' stored as orc
tblproperties("skip.header.line.count"="0");

LOAD DATA INPATH '/user/crosado/Proyecto_EMT_smartbus/transport/paradas_sentido_new.csv' OVERWRITE INTO TABLE paradas_sentido_antes;

INSERT INTO paradas_sentido SELECT * FROM paradas_sentido_antes where linea is not null;
