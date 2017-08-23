#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"
echo "Starting load traffic data"
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"

unzip  /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}.zip -d /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/

if [ ${1} == 2015 ]
then
	file_transport=("201501" "201502" "201503" "201504" "201505" "201506" "201507" "201508" "201509" "201510" "201511" "201512"); 
elif [ ${1} == 2016 ]
then
	file_transport=("201601" "201602" "201603" "201604" "201605" "201606" "201607" "201608" "201609" "201610" "201611" "201612");  
else
	file_transport=("201701" "201702"); 
fi

for f_transport in ${file_transport[*]}
do
  unzip  /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}/$f_transport.zip -d /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}
  hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}/$f_transport.csv /user/crosado/Proyecto_EMT_smartbus/transport/
  rm /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}/$f_transport.csv
  rm /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}/$f_transport.zip 
done

rm  -r /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1} 
rm  /home/crosado/Proyecto_EMT_smartbus/datasets_trafico/${1}.zip 


