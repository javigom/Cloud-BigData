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
DFVar = spark.read.option("header", "True").csv("IMDb_movies.csv")

# Selecciono las columnas pais y media de votos
DFVar2 = DFVar.select(DFVar['country'], DFVar['avg_vote'])

# Lo transformo en un rdd
RDDVar = DFVar2.rdd.map(lambda (x, y): (x, y))

# Hago un filtro para evitar errores
RDDVar2 = RDDVar.filter(lambda(x, y): esFloat(y) and esString(x)) 

def esString(line):
    try:
        str(line)
        return True
    except ValueError:
        return False
        
def esFloat(valoracion):
    try:
        float(valoracion)
     	return True
    except ValueError:
        return False

# Me quedo con el primer pais y transformo la nota en un float
RDDVar3 = RDDVar2.map(lambda(x, y): (re.split(',', str(x))[0], float(y)))

# Contador del numero de veces que aparece un pais
counter = RDDVar3.map(lambda (x, y): (x, 1)).reduceByKey(lambda x, y: x+y)

# Transformacion en un DF ordenado alfabeticamente
DFRes = counter.sortByKey().toDF(["country", "number of films"])

# Lo guardo en un fichero
DFRes.write.format("csv").save("./output/num_pelis_por_pais")
