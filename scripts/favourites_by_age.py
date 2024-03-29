# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_ratings para hallar la pelicula favorita de cada
rango de edad. Posteriormente usa el archivo IMDb_movies para leer el nombre de dichas peliculas '''

## IMPORTS ##
# Python
import time
start_time = time.time()
import string
import sys

# Spark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sum, mean, ceil, collect_list, asc, desc

# Graficos
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

## CONSTANTES
AGE_RANGES = ["0-18", "18-30", "30-45", "45-100"]
AGE_RANGES_NO = 4
MOVIES_PER_RANGE = 5

## INICILIAZACION DE SPARK ##
''' BENCHMARK: para obtener tiempos optimos 
- Numero de tareas = coincide con el num. de particiones del dataframe.
Por defecto se crean 200, pero nosotros usamos una heuristica de [num. ejecutores * num.cores de cada ejecutor] 
- Numero de ejecutores = 1 si se lanza en local, tantos como nodos si se trata de un cluster 
- Numero de hilos/ejecutor = usaremos tantos cores como tenga el ejecutor (local[*])
'''
conf = SparkConf().setMaster('local[*]').setAppName('favouritesByAge')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

# Num de tareas
DATAFRAME_PARTITIONS = 4
sqlContext.setConf("spark.sql.shuffle.partitions", DATAFRAME_PARTITIONS)

## PROCESAMIENTO DE LOS DATOS ##
## IMDb_ratings##
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "true").csv("../datasets/IMDb_ratings.csv")

# Especificamos que columnas queremos usar para este caso...
colNames = DFVar.schema.names 
wantedCols = ["imdb_title_id","total_votes", "allgenders_0age_avg_vote", "allgenders_18age_avg_vote",
"allgenders_30age_avg_vote", "allgenders_45age_avg_vote"]

# ... y nos deshacemos del resto
droppedCols = set(colNames).symmetric_difference(set(wantedCols))
DFVar = DFVar.drop(*droppedCols)
colNames = DFVar.schema.names

# Obtenemos los identifacdores de las peliculas preferidas por cada rango de edad
movieIds = []
for i in range(AGE_RANGES_NO):
	# Quitamos filas con valores nulos
	AuxVar = DFVar.where(DFVar[colNames[i + 2]].isNotNull())
	# Ordenamos por nota y guardamos el id de las X mejores
	AuxVar = AuxVar.sort(desc(colNames[i + 2]))
	id = AuxVar.limit(MOVIES_PER_RANGE).select("imdb_title_id").rdd.flatMap(lambda x: x).collect()
	movieIds.append([])
	movieIds[i].append(id)

## IMDb_movies ##
## Vamos al archivo de las peliculas y hallamos los nombres de estas
DFVar = spark.read.option("header", "true").csv("../datasets/IMDb_movies.csv")
colNames = DFVar.schema.names
wantedCols = ["imdb_title_id","title", "year", "genre", "duration", "avg_vote"]

droppedCols = set(colNames).symmetric_difference(set(wantedCols))
DFVar = DFVar.drop(*droppedCols)
colNames = DFVar.schema.names

# Nos quedamos con las filas que nos interesan
for i in range (AGE_RANGES_NO):
	print("Favoutite movies of ages " + AGE_RANGES[i] + ":") 
	AuxDF = DFVar.where(DFVar["imdb_title_id"].isin(*movieIds[i]))
	AuxDF = AuxDF.drop("imdb_title_id")
	AuxDF.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("../results/" + AGE_RANGES[i] + "_favourites.csv")

# Debug del tiempo, para el benchmarking
print("--- %s seconds ---" % (time.time() - start_time))
