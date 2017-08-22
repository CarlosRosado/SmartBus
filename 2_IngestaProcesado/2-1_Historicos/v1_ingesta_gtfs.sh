#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|##|#|#"
echo "     Starting GTFS tables generator process  "
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|##|#|#"

echo "Generacion de las tablas GTFS"


# Realizamos la ingesta de los datos
beeline_shell -f v1_tablas_GTFS.hql


