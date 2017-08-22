#!/bin/bash

echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|#"
echo "    Starting master joins generator    "
echo "|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|#|##|#|#"

echo "3. Generacion de las tablas master"

beeline_shell -f 6_generate_master.hql

beeline_shell -f 7_poda_outliers.hql
