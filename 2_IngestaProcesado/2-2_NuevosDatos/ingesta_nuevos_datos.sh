#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"
echo "Starting table generator process new data"
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"

#Le pasamos el nombre del fichero como arg 1 y el año como arg 2 y el arg 3 para el fichero del trafico

echo "1. Ingesta de viajeros (nuevos datos)"

beeline_shell --hivevar year=${2} --hivevar year_month=${1} -f 1_ingesta_nuevos_viajeros.hql

echo "2. Ingesta de tablas auxiliares (trafico,aemet,eventos_deportivos,etc)"

beeline_shell --hivevar year=${2} --hivevar year_month=${1} --hivevar trafic_file=${3} -f 2_ingesta_nuevos_auxiliares.hql


echo "3. Joins para las tablas master (datos nuevos)"

beeline_shell --hivevar year=${2} --hivevar year_month=${1} -f 3_ingesta_nuevos_master.hql

#El calendario en principio no nos afecta para su cambio