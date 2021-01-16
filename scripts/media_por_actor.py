#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

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
conf = SparkConf().setMaster('local').setAppName('RatingPerActor')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

## PROCESAMIENTO DE LOS DATOS ##
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFMovies = spark.read.option("header", "true").csv("../datasets/IMDb_movies.csv")

# Selecciono las columnas del fichero movies que me interesan, los actores y la valoración media
DFMovies = DFMovies.select(DFMovies["actors"], DFMovies["avg_vote"])

# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFActors = spark.read.option("header", "true").csv("../datasets/IMDb_names.csv")

# Selecciono la columna del fichero name que me interesa, en este caso su nombre
DFActors = DFActors.select(DFActors["imdb_name_id"], DFActors["name"])

# Funcion que devuelve True si se trata de un identificador y False en caso contrario
def esID(x):
    try:
        x = str(x)
        if((x[0] == 'n') and (x[1] == 'm')):
            return True
        else:
            return False
    except ValueError:
        return False
   
# Debido al formato del fichero, es necesario filtrar filas no validas
RDDActors = DFActors.rdd.map(lambda (x, y): (x, y)).filter(lambda (x, y): esID(x))

# Guardo en una lista las claves, es decir, los nombres de los actores
actors_list = RDDActors.map(lambda (x, y): (unicode(y), x)).keys().collect()

#Función que devuelve True si la valoracion se puede transformar en float 
def esFloat(valoracion):
    try:
        float(valoracion)
     	return True
    except ValueError:
        return False

# Funcion que dado un nombre de un actor, me devuelve la media de sus peliculas
def devuelveMediaPelis(nombre):
    RDDAux = DFMovies.rdd.map(lambda (x,y): (x, y)).filter(lambda (x,y): nombre in unicode(x) and esFloat(y))
    counter = RDDAux.map(lambda (x, y): (nombre, 1)).reduceByKey(lambda x, y: x+y)
    suma = RDDAux.map(lambda (x, y): (nombre, float(y))).reduceByKey(lambda x, y: x+y)
    res = suma.union(counter).reduceByKey(lambda x, y: x/y)
    try:
        mediaPelis = res.collect()[0][1]
    except:
        mediaPelis = 0
    return round(mediaPelis, 1)

# PRUEBAS PARA COMPROBAR EL CORRECTO FUNCIONAMIENTO DE devuelveMediaPelis
#print(devuelveMediaPelis(unicode(u'Polly Bergen')))
#print(devuelveMediaPelis("Dean Cain"))
#print(devuelveMediaPelis(unicode(u'Kishô Taniyama')))

# Lista donde guardare un par con el nombre y la media de apariciones
def_list = []

# Relleno dicha lista (LIMITADO A 100 POR AHORA)
for i in range(100):
    
    # Calculo la media de las peliculas en las que aparecen
    def_list.append((actors_list[i], devuelveMediaPelis(actors_list[i])))

# Lo transformo a un dataframe
DFRes = sc.parallelize(def_list).filter(lambda (x, y): y > 0).toDF()

# Lo muestro por la consola
DFRes.show(100)

# Lo guardo en un fichero
#DFRes.write.format("csv").save("../output/media_por_actor")
