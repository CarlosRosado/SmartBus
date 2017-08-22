#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|##|#|#"
echo "     Starting aux tables generator process  "
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|##|#|#"

echo "2.1 Ingesta de tablas (plazas,tipos,calendario,etc)"


# Realizamos la ingesta de los datos
beeline_shell -f 3_ingesta_tablas_aux.hql

echo "2.2.1. Ingesta del trafico del 2015"


year_2015=2015

for file_2015 in 201501 201502 201503 201504 201505 201506 201507 201508 201509 201510 201511 201512;
do
  beeline_shell --hivevar year=$year_2015 --hivevar file_1516=$file_2015 -f 4_ingesta_trafico.hql
done


echo "2.2.2. Ingesta del trafico del 2016"

year_2016=2016

for file_2016 in 201601 201602 201603 201604 201605 201606 201607 201608 201609 201610 201611 201612;
do
  beeline_shell --hivevar year=$year_2016 --hivevar file_1516=$file_2016 -f 4_ingesta_trafico.hql
done

echo "2.2.3. Ingesta del trafico del 2017"

year_2017=2017

for file_2017 in 201701 201702;
do
  beeline_shell --hivevar year=$year_2017 --hivevar file_1516=$file_2017 -f 4_ingesta_trafico.hql
done

echo "2.2.3. Ingesta del trafico para el 2015/2016/2017"

beeline_shell -f 5_join_anhos_trafico.hql

