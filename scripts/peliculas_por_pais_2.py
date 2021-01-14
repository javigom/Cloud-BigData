#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

'''Ejemplo de lectura de datos del archivo "IMDb_movies" '''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc, desc

import string
import sys
import re
reload(sys)
sys.setdefaultencoding('utf8')

# Inicializamos Spark
conf = SparkConf().setMaster('local').setAppName('RatingPerCountry')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "True").csv("../datasets/IMDb_movies.csv")

# Selecciono las columnas pais y media de votos
DFVar = DFVar.select(DFVar['country'], DFVar['avg_vote'])

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

# Me quedo con el primer pais y transformo la nota en un float
RDDVar = RDDVar.map(lambda(x, y): (re.split(',', x)[0], float(y)))

# Contador del numero de veces que aparece un pais
counter = RDDVar.map(lambda (x, y): (x, 1)).reduceByKey(lambda x, y: x+y)

# Transformacion en un DF ordenado alfabeticamente
DFRes = counter.toDF(["country", "count"]).sort(desc("count"))

# Lo muestro por la consola
DFRes.show(1000)

# Lo guardo en un fichero
#DFRes.write.format("csv").save("../output/pelis_por_pais")
