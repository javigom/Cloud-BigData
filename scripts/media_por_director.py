#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_movies, IMDb_names e IMDb_title_principals para hallar la media aritmetica de las peliculas en las que han participado cada director '''

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
conf = SparkConf().setMaster('local[*]').setAppName('RatingPerActor')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

# Num de tareas
DATAFRAME_PARTITIONS = 6
sqlContext.setConf("spark.sql.shuffle.partitions", DATAFRAME_PARTITIONS)

## PROCESAMIENTO DE LOS DATOS ##

## FICHERO TITLE PRINCIPALS ##

# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFtp = spark.read.option("header", "true").csv("../datasets/IMDb_title_principals.csv")

# Selecciono las columnas con el id de la pelicula, de la persona y su categoria
DFtp = DFtp.select(DFtp["imdb_title_id"], DFtp["imdb_name_id"], DFtp["category"])

# Elimino aquellos que no sean directores
DFtp = DFtp.filter(DFtp["category"] == "director")

# Elimino la columna de la gategoria
DFtp = DFtp.select(DFtp["imdb_title_id"], DFtp["imdb_name_id"])


## FICHERO MOVIES ##
  
#Función que devuelve True si la valoracion se puede transformar en float 
def esFloat(valoracion):
    try:
        float(valoracion)
     	return True
    except ValueError:
        return False
        
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera  
DFMovies = spark.read.option("header", "true").csv("../datasets/IMDb_movies.csv")

# Selecciono las columnas del identificador y de la nota media
DFMovies = DFMovies.select(DFMovies["imdb_title_id"], DFMovies["avg_vote"])

# Filtro las filas con valoraciones incorrectas para evitar errores
RDDMovies = DFMovies.rdd.map(lambda (x, y): (x, y)).filter(lambda (x, y): esFloat(y))

#Guardo el RDD como una lista
MoviesList = RDDMovies.collect()

#Lo convierto en un diccionario para usarlo mas adelante
MoviesDict = dict(MoviesList)


## FICHERO NAMES ##
  
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
        
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera     
DFNames = spark.read.option("header", "true").csv("../datasets/IMDb_names.csv")

# Selecciono las columnas del identificador y del nombre
DFNames = DFNames.select(DFNames["imdb_name_id"], DFNames["name"])

# Filtro las filas con identificadores incorrectos para evitar errores
RDDNames = DFNames.rdd.map(lambda (x, y): (x, y)).filter(lambda (x, y): esID(x))

#Guardo el RDD como una lista
NamesList = RDDNames.collect()

#Lo convierto en un diccionario para usarlo mas adelante
NamesDict = dict(NamesList)

## CONTINUACION ##

# Creo un RDD a partir del primer DF que contenía una relacion de identificadores entre peliculas y actores. A partir del mismo, creo otro sustituyendo el identificador de la pelicula por su valoracion e intercambiando el orden con el identificador. Como puede haber películas que debido a errores no tengan una valoracion, es necesario filtarlas para descartarlas.
RDDAux = DFtp.rdd.map(lambda (x, y): (y, MoviesDict.get(x))).filter(lambda (x, y): y != None)

# Calculo la suma de las valoraciones de las peliculas en las que participa un director
suma = RDDAux.map(lambda (x, y): (x, float(y))).reduceByKey(lambda x, y: x+y)

# Calculo el numero de peliculas por actor
counter = RDDAux.map(lambda (x, y): (x, 1)).reduceByKey(lambda x, y: x+y)

# Uno las suma con el contador para calcular la media
res = suma.union(counter).reduceByKey(lambda x, y: round(x/y, 1))

# Guardo el resultado como un DF, sustituyendo el identificador del director por su nombre y ordenando en orden decreciente por la valoracion
DFRes = res.map(lambda (x, y): (NamesDict.get(x), y)).toDF(["Name", "Rating"]).sort(desc("Rating"))

# Lo guardo en un fichero
DFRes.write.format("csv").save("../output/rating_director")

# Debug del tiempo, para el benchmarking
print("--- %s seconds ---" % (time.time() - start_time))
