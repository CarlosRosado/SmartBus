#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"
echo " LOAD NEW DATA IN HDFS"
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"

#Le pasamos el nombre del fichero como arg 1 y el año como arg 2 y el arg 3 para el fichero del trafico
echo "1. Carga datos de viajeros"

unzip  /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/${1}.zip 
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/${1}/${year_month}.csv /user/crosado/Proyecto_EMT_smartbus/transport/
rm -r /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/${1} 
rm /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/${1}.zip

echo "2. Carga datos de trafico"

unzip  /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/trafico/${3}.zip 
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/trafico/${3}.csv /user/crosado/Proyecto_EMT_smartbus/transport/

rm -r /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/${3} 
rm /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/${3}.zip


echo "3. Carga de otros datos"

unzip  /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/otros/otros.zip 

hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/datasets_nuevos/otros/aemet_new_procesado.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/datasets_nuevos/otros/calendario_futbol_procesado.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/datasets_nuevos/otros/paradas_equipos.csv /user/crosado/Proyecto_EMT_smartbus/transport/

rm -r /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/otros 
rm -r /home/crosado/Proyecto_EMT_smartbus/datasets_nuevos/viajeros/otros.zip

#El calendario en principio no nos afecta para su cambio

