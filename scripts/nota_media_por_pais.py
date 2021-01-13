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

# Nos quedamos solo con el pais principal de la pelicula
DFVar = DFVar.withColumn("mainCountry", split(col("country"), "\\,").getItem(0))
DFVar = DFVar.drop("country")

# Lo transformo en un rdd y hago un filtro de errores
RDDVar = DFVar.rdd.map(lambda (x, y): (y, x)).filter(lambda(x, y): esFloat(y) and x != None) 

def esFloat(valoracion):
    try:
        float(valoracion)
     	return True
    except ValueError:
        return False
        
RDDVar = RDDVar.map(lambda(x, y): (x, float(y)))

# Suma de la nota de los paises por clave
suma = RDDVar.reduceByKey(lambda x, y: x+y)

# Contador del numero de veces que aparece un pais
counter = RDDVar.map(lambda (x, y): (x, 1)).reduceByKey(lambda x, y: x+y)

# Calculo de la media
res = suma.union(counter).reduceByKey(lambda x, y: x/y)

# Lo guardo en un fichero de texto
#res.sortByKey().saveAsTextFile("media_paises")
res.sortByKey().toDF().write.format("csv").save("../output/nota_media_por_pais")
