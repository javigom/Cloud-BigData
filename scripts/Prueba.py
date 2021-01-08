# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

'''Ejemplo de lectura de datos del archivo "IMDb_movies" '''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc

import string
import sys

# Inicializamos Spark
conf = SparkConf().setMaster('local').setAppName('MovieTrends')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "true").csv("./datasets/IMDb_movies.csv")

# Especificamos que columnas queremos usar para este caso...
colNames = DFVar.schema.names
wantedCols = ["imdb_title_id", "title", "year", "genre", "country", "director", "avg_vote"]

# ... y nos deshacemos del resto
droppedCols = set(colNames).symmetric_difference(set(wantedCols))
DFVar = DFVar.drop(*droppedCols)

# TODO: Quitar las entradas con datos incorrectos en la columna avg_vote

# Mostramos los datos
DFVar.show(1000)

# Los guardamos en disco
#DFVar.write.format("csv").save("./output/Prueba")

