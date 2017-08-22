USE emt_smartbus;

------------------------------------------------------
--- GENRERACION DE LAS TABLAS PARA EL PINTADO DE BILLETES
------------------------------------------------------

!sh echo "Generamos el numero de billetes global.";

CREATE TABLE num_billetes stored as orc as
SELECT fecha,titulo,linea,sentido,anho,mes,dia_semana,sum(viajeros) as viajeros,tipo_linea 
FROM viajeros_sentido_master_nooutliers 
group by titulo,fecha,linea,anho,mes,dia_semana,tipo_linea,sentido;  