#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_movies para hallar la media aritmetica de las peliculas de cada genero '''

## IMPORTS ##
# Python
import string
import sys
import re
reload(sys)
sys.setdefaultencoding('utf8')

# Spark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc, desc

# Inicializamos Spark
conf = SparkConf().setMaster('local').setAppName('RatingPerGenre')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

## PROCESAMIENTO DE LOS DATOS ##
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "True").csv("../datasets/IMDb_movies.csv")

# Selecciono la columna del genero y media de votos
DFVar = DFVar.select(DFVar['genre'], DFVar['avg_vote'])

# Lo transformo en un rdd
RDDVar = DFVar.rdd.map(lambda (x, y): (unicode(x), y)) 

#Función que devuelve True si la valoracion se puede transformar en float 
def esFloat(valoracion):
    try:
        float(valoracion)
     	return True
    except ValueError:
        return False
        
# Hago un filtro para evitar errores
RDDVar = RDDVar.filter(lambda(x, y): esFloat(y) and x != None) 

# Me quedo con el primer genero y transformo la nota en un float
RDDVar = RDDVar.map(lambda(x, y): (re.split(',', x)[0], float(y)))

# Suma de la nota de los generos por clave
suma = RDDVar.reduceByKey(lambda x, y: x+y)

# Contador del numero de veces que aparece un genero
counter = RDDVar.map(lambda (x, y): (x, 1)).reduceByKey(lambda x, y: x+y)

# Calculo de la media
res = suma.union(counter).reduceByKey(lambda x, y: round(x/y, 1))

# Transformacion en un DF ordenado alfabeticamente
DFRes = res.toDF(["genre", "count"]).sort(desc("count"))

# Lo muestro por la consola
DFRes.show(1000)

# Lo guardo en un fichero
#DFRes.write.format("csv").save("../output/media_por_genero")
