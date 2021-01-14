# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_movies para hallar el numero total de peliculas
hechas por cada pais '''

## IMPORTS ##
# Python

import string
import sys

# Spark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc, desc

## INICILIAZACION DE SPARK ##
conf = SparkConf().setMaster('local[*]').setAppName('peliculas_por_pais')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

# Estas lineas para obtener el numero de ejecutores y cores no funcionan como deberian
#NUM_EXECUTORS = conf.get('spark.executor.instances')
#NUM_CORES = conf.get('spark.executor.cores')
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

## PROCESAMIENTO DE LOS DATOS ##
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "true").csv("../datasets/IMDb_movies.csv")

# Especificamos que columnas queremos usar para este caso...
colNames = DFVar.schema.names 
wantedCols = ["country"]

# ... y nos deshacemos del resto
droppedCols = set(colNames).symmetric_difference(set(wantedCols))
DFVar = DFVar.drop(*droppedCols)

# Nos quedamos solo con el pais principal de la pelicula
DFVar = DFVar.withColumn("mainCountry", split(col("country"), "\\,").getItem(0))
DFVar = DFVar.drop("country")

# Agrupamos los datos
DFVar = DFVar.groupBy("mainCountry").count()

DFVar = DFVar.withColumnRenamed("_1", "Country").withColumnRenamed("_2", "Count")
DFVar = DFVar.sort(desc("Count"))

DFVar.write.format("csv").save("../output/peliculas_por_pais")
