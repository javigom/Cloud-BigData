#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

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
DFRes = res.toDF(["country", "avg"]).sort(desc("avg"))

# Lo muestro por la consola
DFRes.show(1000)

# Lo guardo en un fichero de texto
#DFRes.write.format("csv").save("../output/nota_media_por_pais")
