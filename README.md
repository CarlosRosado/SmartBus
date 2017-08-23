# SmartBus: Big Data & Data Science en Trasnporte Urbano

Trabajo  de  fin  de  Máster  (TFM),  trata  de  la  realización  de  un  estudio y análisis de ciertos tipos de datos procedentes de la red de autobuses de Madrid (EMT). El cual se basa en una plataforma Big Data mediante la cual se almacenarán grandes cantidades de datos procedentes de diversas fuentes, tanto internas a la EMT como externas, con el fin de aplicar técnicas de aprendizaje automático y realizar una predicción del número de viajeros que emplear ́an ciertos tipos de autobús.

Por otro lado, se realizará un análisis de los resultados obtenidos mediante el cual se pueda mejorar la eficiencia de la red de autobuses de la EMT, por medio de técnicas de regresión en grandes cantidades de datos.

Para finalizar, se implementará un DashBoard mediante el cual el usuario final pueda ver los resultados de nuestro estudio y pueda tomar decisiones en base a un criterio basado en grandes cantidades de datos.

## Estructura

* 1_Carga: En este apartado tenemos los scripts para realizar la carga de los datos desde el cluster a HDFS.
  * 1_carga_viajeros.sh
  * 2_carga_tablas_aux.sh
  * 3_carga_trafico.sh
  * 4_carga_gtfs.sh
  * 5_carga_nueva.sh
  * README.txt

* 2_IngestaProcesado
  * 2-1_Historicos: Scripts para la ingesta y procesado de los datos históricos de la EMT y de fuentes externas.
    * 1_ingesta_viajeros.hql
    * 1_ingesta_viajeros.sh
    * 2.1_ingesta_paradas_sentidos.hql
    * 2.2_join_groupby_sentidos.hql
    * 2_ingesta_tablas_aux.sh
    * 3_ingesta_tabla_master.sh
    * 3_ingesta_tablas_aux.hql
    * 4_ingesta_trafico.hql
    * 5_join_anhos_trafico.hql
    * 6_generate_master.hql
    * 7_poda_outliers.hql
    * v1_ingesta_gtfs.sh
    * v1_tablas_GTFS.hql
    * v2_ingesta_count_billetes.hql

  * 2-2_NuevosDatos: Scripts para la ingesta de nuevos datos de la EMT y de fuentes externas.
    * 1_ingesta_nuevos_viajeros.hql
    * 2_ingesta_nuevos_auxiliares.hql
    * 3_ingesta_nuevos_master.hql
    * ingesta_nuevos_datos.sh

* 3_Auditoria: Notebooks para la realización de una auditoría de los datos.
  * 1_Auditoria_EstudioLineas.ipynb
  * 2_Auditoria_EstudioTemporal.ipynb
  * 3_Auditoria_EstudioDemanda.ipynb
  * 4_Auditoria_EstudioVariables.ipynb

* 4_Analítica
  * 1_Clustering: Notebooks para la realización de la segmentación de las lineas de la EMT.
    * 1_ClusteringLineas_SAXKmeans.ipynb
    * 2_ClusteringLineas_KMeans_Jerarquico.ipynb
    * 3_ClusteringLineas_Conteo.ipynb
    
  * 2_PrediccionDemanda: Notebooks para la realización de la predicción de la demanda en las lineas de la EMT.
    * 1_PrediccionDemanda_ARIMA.ipynb
    * 2_PrediccionDemanda_Extract2keras.ipynb
    * 3_PrediccionDemanda_Keras.ipynb



## Uso y Requisitos

