# Cloud & Big Data, UCM, 2021

# Ramon Arjona Quiniones
# Javier Gomez Moraleda
# Michael Steven Paredes Sanchez

''' Procesa el archivo IMDb_movies para hallar el numero total de peliculas
hechas por cada pais '''

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


## CONSTANTES ##
VISIBLE_COUNTRIES = 10 # Solo queremos mostrar 10 a la vez


## INICILIAZACION DE SPARK ##
''' BENCHMARK: para obtener tiempos optimos 
- Numero de tareas = coincide con el num. de particiones del dataframe.
Por defecto se crean 200, pero nosotros usamos una heuristica de [num. ejecutores * num.cores de cada ejecutor] 
- Numero de ejecutores = 1 si se lanza en local, tantos como nodos si se trata de un cluster 
- Numero de hilos/ejecutor = usaremos tantos cores como tenga el ejecutor (local[*])
'''
conf = SparkConf().setMaster('local[*]').setAppName('moviesByCountry')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

# Particiones de los datos: tantas como cores tengamos
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

## PROCESAMIENTO DE LOS DATOS ##
# Lectura del archivo csv: con la opcion "header" hacemos que la primera fila haga de cabecera
DFVar = spark.read.option("header", "true").csv("../datasets/IMDb_movies.csv")

# Especificamos que columnas queremos usar para este caso...
colNames = DFVar.schema.names 
wantedCols = ["country"]

# ... y nos deshacemos del resto
droppedCols = set(colNames).symmetric_difference(set(wantedCols))
DFVar = DFVar.drop(*droppedCols)

# Nos quedamos solo con el pais principal de la pelicula
num_movies = DFVar.count()
DFVar = DFVar.withColumn("mainCountry", split(col("country"), "\\,").getItem(0))
DFVar = DFVar.drop("country")

# Agrupamos los datos y los ordenamos de mayor a menor
DFVar = DFVar.groupBy("mainCountry").count()
DFVar = DFVar.sort(desc("count"))

# Lo guardo en un fichero
DFVar.write.format("csv").save("../output/movies_by_country")

## GRAFICA ##
# Preprocesado
num_countries = DFVar.count()
DFVar = DFVar.limit(VISIBLE_COUNTRIES - 1)

# Le metemos una fila al DF
maincountries_movies = DFVar.agg(sum("count")).select("sum(count)").rdd.flatMap(lambda x: x).collect()[0]
otros_lista = [["Others", num_movies - maincountries_movies]]
otros_DF = spark.createDataFrame(otros_lista)
DFVar = DFVar.union(otros_DF)
DFVar.show()

# Metadatos
labels = DFVar.select("mainCountry").rdd.flatMap(lambda x: x).collect()
sizes = DFVar.select("count").rdd.flatMap(lambda x: x).collect()
explode = [0] * VISIBLE_COUNTRIES
explode[0] = 0.1

# Se dibuja el grafico de tarta
fig1, ax1 = plt.subplots()
ax1.pie(sizes, explode=explode, autopct='%1.1f%%', 
pctdistance=1.1, shadow=True, startangle=90)
ax1.axis('equal') # para que sea un circulo
plt.legend(labels, loc = "upper left")

# La guardamos en el sistema de ficheros
plt.savefig('../results/movies_by_country.png')

# Debug del tiempo, para el benchmarking
print("--- %s seconds ---" % (time.time() - start_time))
