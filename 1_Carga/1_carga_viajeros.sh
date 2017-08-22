#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"
echo "Starting loading of transport data"
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"

hdfs dfs -mkdir -p /user/crosado/Proyecto_EMT_smartbus/transport/

unzip  /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}.zip -d /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/


if [ ${1} == 2015 ]
then
	year_month=("201501" "201502" "201503" "201504" "201505" "201506" "201507" "201508" "201509" "201510" "201511" "201512"); 
elif [ ${1} == 2016 ]
then
	year_month=("201601" "201602" "201603" "201604" "201605" "201606" "201607" "201608" "201609" "201610" "201611" "201612");
else
	year_month=("201701" "201702");
fi

for y_month in ${year_month[*]}
do
  unzip  /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}/$y_month.zip -d /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}/
  hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}/$y_month.csv /user/crosado/Proyecto_EMT_smartbus/transport/
  rm /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}/$y_month.csv
  rm /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}/$y_month.zip
done

rm -r /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}
rm /home/crosado/Proyecto_EMT_smartbus/dataset_viajeros/${1}.zip

hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_tablas_aux/paradas_sentido_new.csv /user/crosado/Proyecto_EMT_smartbus/transport/

