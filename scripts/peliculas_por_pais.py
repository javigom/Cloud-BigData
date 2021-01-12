# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_movies para hallar el numero total de peliculas
hechas por cada pais '''

## IMPORTS ##
# Spark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc, desc

# Graficos
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Python
import string
import sys

## CONSTANTES ##
VISIBLE_COUNTRIES = 10 # Solo queremos mostrar 10 a la vez


## PROCESAMIENTO DEL DATASET ##
# Inicializamos Spark
conf = SparkConf().setMaster('local').setAppName('MovieTrends')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "true").csv("./datasets/IMDb_movies.csv")

# Especificamos que columnas queremos usar para este caso...
colNames = DFVar.schema.names 
wantedCols = ["country"]

# ... y nos deshacemos del resto
droppedCols = set(colNames).symmetric_difference(set(wantedCols))
DFVar = DFVar.drop(*droppedCols)

# Nos quedamos solo con el pais principal de la pelicula
DFVar = DFVar.withColumn("mainCountry", split(col("country"), "\\,").getItem(0))
DFVar = DFVar.drop("country")

# Agrupamos los datos y los ordenamos de mayor a menor
DFVar = DFVar.groupBy("mainCountry").count()
DFVar = DFVar.sort(desc("count"))

## GRAFICA ##
# Preprocesado
num_countries = DFVar.count()
DFVar = DFVar.limit(VISIBLE_COUNTRIES - 1)
DFVar.show()

# Metadatos
labels = DFVar.select("mainCountry").rdd.flatMap(lambda x: x).collect()
sizes = DFVar.select("count").rdd.flatMap(lambda x: x).collect()
explode = (0.1, 0, 0, 0, 0, 0, 0, 0, 0)

# Se dibuja la figura
fig1, ax1 = plt.subplots()
ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', 
shadow=True, startangle=90)
ax1.axis('equal') # circulo

plt.show()
plt.savefig('./results/peliculas_por_pais.png')
