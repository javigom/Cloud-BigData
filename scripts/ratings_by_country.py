#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_movies para hallar la media aritmetica de las peliculas hechas por cada pais '''

## IMPORTS ##
# Python
import time
start_time = time.time()
import string
import sys
import re
reload(sys)
sys.setdefaultencoding('utf8')

# Spark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc, desc

## INICILIAZACION DE SPARK ##
''' BENCHMARK: para obtener tiempos optimos 
- Numero de tareas = coincide con el num. de particiones del dataframe.
Por defecto se crean 200, pero nosotros usamos una heuristica de [num. ejecutores * num.cores de cada ejecutor] 
- Numero de ejecutores = 1 si se lanza en local, tantos como nodos si se trata de un cluster 
- Numero de hilos/ejecutor = usaremos tantos cores como tenga el ejecutor (local[*])
'''
conf = SparkConf().setMaster('local[*]').setAppName('RatingsByCountry')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

# Num de tareas
DATAFRAME_PARTITIONS = 6
sqlContext.setConf("spark.sql.shuffle.partitions", DATAFRAME_PARTITIONS)

## PROCESAMIENTO DE LOS DATOS ##

# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "True").csv("../datasets/IMDb_movies.csv")

# Selecciono las columnas pais y media de votos
DFVar = DFVar.select(DFVar['country'], DFVar['avg_vote'])

#Función que devuelve True si la valoracion se puede transformar en float 
def esFloat(valoracion):
    try:
        float(valoracion)
        return True
    except ValueError:
        return False

# Lo transformo en un rdd y hago un filtro de errores
RDDVar = DFVar.rdd.map(lambda (x, y): (x, y)).filter(lambda(x, y): esFloat(y) and x != None) 

# Me quedo con el primer pais y transformo la nota en un float
RDDVar = RDDVar.map(lambda(x, y): (re.split(',', unicode(x))[0], float(y)))

# Suma de la nota de los paises por clave
suma = RDDVar.reduceByKey(lambda x, y: x+y)

# Contador del numero de veces que aparece un pais
counter = RDDVar.map(lambda (x, y): (x, 1)).reduceByKey(lambda x, y: x+y)

# Calculo de la media
res = suma.union(counter).reduceByKey(lambda x, y: round(x/y, 1))

# Transformacion en un DF ordenado alfabeticamente
DFRes = res.toDF(["Country", "Rating"]).sort(desc("Rating"))

# Lo guardo en un fichero de texto
DFRes.write.format("csv").save("../output/ratings_by_country")

# Debug del tiempo, para el benchmarking
print("--- %s seconds ---" % (time.time() - start_time))
