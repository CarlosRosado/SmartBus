#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"
echo "		Load GTFS tables in HDFS 	"
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#"

# Insertamos los csv en el HDFS

unzip  /home/crosado/Proyecto_EMT_smartbus/transitEMT/transitEMT.zip -d /home/crosado/Proyecto_EMT_smartbus/transitEMT/

hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/calendar.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/calendar_dates.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/frequencies.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/routes.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/shapes.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/stop_times.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/stops.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/trips.txt /user/crosado/Proyecto_EMT_smartbus/transport/
hadoop fs -put /home/crosado/Proyecto_EMT_smartbus/transitEMT/gtfs_lineas.csv /user/crosado/Proyecto_EMT_smartbus/transport/

rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/agency.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/calendar.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/calendar_dates.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/frequencies.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/routes.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/shapes.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/stop_times.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/stops.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/trips.txt
rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/gtfs_lineas.csv

rm /home/crosado/Proyecto_EMT_smartbus/transitEMT/transitEMT.zip
