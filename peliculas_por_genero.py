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

conf = SparkConf().setMaster('local[*]').setAppName('peliculas_por_pais')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

## PROCESAMIENTO DE LOS DATOS ##
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "true").csv("../datasets/IMDb_movies.csv")

# Selecciono la columna del genero
DFVar = DFVar.select(DFVar['genre'])

# Nos quedamos solo con el pais principal de la pelicula
DFVar = DFVar.withColumn("mainGenre", split(col("genre"), "\\,").getItem(0))
DFVar = DFVar.drop("genre")

# Agrupamos los datos
DFVar = DFVar.groupBy("mainGenre").count()
DFVar = DFVar.sort(desc("Count"))

DFVar.write.format("csv").save("../output/peliculas_por_genero")
