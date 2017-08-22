#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|#"
echo "     Starting table generator process  "
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|#"

echo "1.1 Ingesta de tablas para el 2015"


year_2015=2015

for year_month_2015 in 201501 201502 201503 201504 201505 201506 201507 201508 201509 201510 201511 201512;
do
  beeline_shell --hivevar year=$year_2015 --hivevar year_month=$year_month_2015 -f 1_ingesta_viajeros.hql
  echo "Mes insertado..."
done

echo "1.2 Ingesta de tablas para el 2016"

year_2016=2016

for year_month_2016 in 201601 201602 201603 201604 201605 201606 201607 201608 201609 201610 201611 201612;
do
  beeline_shell --hivevar year=$year_2016 --hivevar year_month=$year_month_2016 -f 1_ingesta_viajeros.hql
  echo "Mes insertado..."
done

echo "1.3 Ingesta de tablas para el 2017"

year_2017=2017

for year_month_2017 in 201701 201702;
do
  beeline_shell --hivevar year=$year_2017 --hivevar year_month=$year_month_2017 -f 1_ingesta_viajeros.hql
  echo "Mes insertado..."
done

echo "1.3 Generacion de tabla sentido y tramos"

beeline_shell -f 2.1_ingesta_paradas_sentidos.hql
beeline_shell --hivevar year=2015 -f 2.2_join_groupby_sentidos.hql
echo "Sentidos 2015 insertados..."
beeline_shell --hivevar year=2016 -f 2.2_join_groupby_sentidos.hql
echo "Sentidos 2016 insertados..."
beeline_shell --hivevar year=2017 -f 2.2_join_groupby_sentidos.hql
echo "Sentidos 2017 insertados..."

