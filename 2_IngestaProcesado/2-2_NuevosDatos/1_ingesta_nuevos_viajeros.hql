USE proyectoemt_smartbus;

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


--- INSERTAMOS EN EL AÑO CORRESPONDIENTE

INSERT INTO viajeros_${hivevar:year} SELECT * from viajeros_${hivevar:year_month};


CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_new AS
SELECT *,cast(SUBSTR(instante,0,2) as int) as tramo 
FROM viajeros_${hivevar:year_month} 
WHERE fecha is not null;

------------------------------------------------------
--- EXTRAPOLAMOS PARA EL SENTIDO
------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_join AS
SELECT fecha,linea,bus,nviaje,MAX(sentido) 
FROM viajeros_${hivevar:year}_new
GROUP BY fecha,linea,bus,nviaje;

-1. INSERTAMOS LOS NUEVOS SENTIDOS

CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_aux AS
SELECT u.fecha,u.instante,u.linea,u.parada,u.titulo,u.bus,u.nviaje,j.sentido,u.viajeros,u.tramo 
FROM viajeros_${hivevar:year}_new u
LEFT JOIN viajeros_${hivevar:year}_join j
ON (u.fecha = j.fecha AND u.linea = j.linea AND u.bus=j.bus AND u.nviaje=j.nviaje);


-2. Sentido recalculado


CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_aux2 AS
SELECT u.*,j.sentido 
FROM viajeros_${hivevar:year}_aux u
LEFT JOIN paradas_sentido j
ON (u.linea = j.linea AND u.parada = j.parada);


CREATE TEMPORARY TABLE IF NOT EXISTS viajeros_${hivevar:year}_aux3 AS
SELECT u.*,
CASE
       WHEN u.sentido_recal is null THEN u.sentido
       ELSE u.sentido_recal

END
from viajeros_${hivevar:year}_aux2 u;


------------------------------------------------------
--- INGESTA DE DATOS PARA EL SENTIDO Y TRAMOS
------------------------------------------------------

INSERT INTO viajeros_${hivevar:year}_sentido 
SELECT fecha,instante,linea,parada,titulo,bus,nviaje,Sentido_recalculado,viajeros,tramo 
FROM viajeros_${hivevar:year}_aux3 
WHERE Sentido_recalculado!=0;

CREATE TEMPORARY IF NOT EXISTS viajeros_${hivevar:year}_sentidotemp AS 
SELECT fecha,instante,linea,parada,titulo,bus,nviaje,Sentido_recalculado,viajeros,tramo 
FROM viajeros_${hivevar:year}_aux3 
WHERE Sentido_recalculado!=0;

CREATE TEMPORARY IF NOT EXISTS viajeros_${hivevar:year}_tramostemp AS 
SELECT fecha,linea,sentido,sum(viajeros),tramo 
FROM viajeros_${hivevar:year}_sentidotemp GROUP BY fecha,linea,sentido,tramo;


INSERT INTO viajeros_${hivevar:year}_tramos
SELECT * FROM viajeros_${hivevar:year}_tramostemp;


-- Insertamos en las tablas de sentido y de tramos generales

INSERT INTO viajeros_sentido 
SELECT * FROM viajeros_${hivevar:year}_sentidotemp;

INSERT INTO viajeros_tramo 
SELECT * FROM viajeros_${hivevar:year}_tramostemp;



