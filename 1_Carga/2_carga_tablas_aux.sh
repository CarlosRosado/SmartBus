#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"
echo "Load temporary tables in hdfs"
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"

# Insertamos los csv en el HDFS

unzip  /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux.zip -d /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/

hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/tipos_lineas.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/plazas.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/tipos.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/calendario_madrid.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/aemet_15_16_17_procesado.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/trafico_paradas.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/calendario_futbol_procesado.csv /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux/paradas_equipos.csv /user/crosado/Proyecto_EMT_smartbus/transport/

rm -r /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux
rm  /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/tablas_aux.zip

