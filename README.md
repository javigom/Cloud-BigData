# Movie Trends

## Índice
1. **Introducción**
   - Necesidad de Big Data
   - Solución
2. **Modelo de datos**
3. **Descripción técnica**
   - Entorno de trabajo
   - Software
   - Reproducir nuestro estudio
4. **Resultados**
   - Ficheros de salida
   - Rendimiento
5. **Conclusiones**

## 1. Introducción
 
 Este proyecto nace de una idea muy simple, somos un alto cargo de una empresa de distribución de películas como Netflix y queremos sacar el máximo beneficio de las películas que compramos y producimos, para alcanzar y mantener el mayor número de suscripciones a nuestra plataforma. ¿Cual fue el género más visto en años anteriores? ¿Que películas fueron las mejor valoradas? ¿Que actores/actrices han sido los mejor puntuados? La respuesta a estas preguntas nos ayuda a tomar decisiones para sacar una mayor rentabilidad a nuestra empresa.
 
### - Necesidad del Big Data -
 El Big Data es necesario por la gran cantidad de datos que manejamos, actualmente +85k películas, con posible expansión de datos a partir de otras plataformas. 
Al estar estructurados la búsqueda y análisis de datos se procesarán a mayor velocidad.
 Haciendo uso de estos datos y de métodos estadísticos se pueden hacer predicciones, a mayor cantidad de datos mayor fiabilidad del resultado. Estas predicciones hablan de los gustos y necesidades de los espectadores y de cómo van evolucionando. La evolución tiene que ver con los avances tecnológicos y culturales, lo que vemos reflejado en los datos.
 
### - Nuestra solución -
Nosotros hemos planteado como la solución a esos problemas, manejar datasets sumamente amplios, con información variada de las películas, que usamos para conseguir, por ejemplo, las mejores películas de un país, los países con más películas, el género más visto de un país como España... Con el objetivo de demostrar mediante datos, que películas merecería la pena adquirir para lo servicios de streaming de peliculas, ya que son muy populares y esta siendo todo un éxito en la audiencia, o una prediccion de que género esta siendo muy aceptado en la audiencia y se debería tener en cuenta para aumentar la probabilidad de éxito.


## 2. Modelo de datos
Nuestros datos han sido obtenidos de cuatro datasets sobre IMDb, que tiene información relevante de de las películas como su año de estreno, valoraciones, duración, donde fue hecha, su género, al igual que la información de los actores que intervienen en ella.
Con todos estos datos, lo que hemos hecho es ir organizando y filtrando distintas condiciones para poder hacer nuestras predicciones y gráficos.


## 3. Descripción técnica
 
### - Entorno de trabajo -

Para nuestro proyecto, las herramientas que vamos a utilizar son las siguientes:

- Para gestionar todos los ficheros que forman nuestro proyecto, hemos utilizado **GitHub**.
- Como lenguaje de programación para todos nuestros scripts, hemos utilizado **Python**.
- Para procesar dichos scripts hemos utilizado **Apache Spark**, ya que nos permite hacer uso de una programación funcional paralela.
- Para llevar a cabo las pruebas de nuestros scripts, hemos utilizado **Amazon Web Services** como plataforma para la ejecución de dichos scripts. 

### - Software -

Como hemos visto anteriormente, nuestro dataset se compone de 4 ficheros complementarios en formato CSV: 
 - **IMDb_movies.csv**: información sobre las películas
 - **IMDb_names.csv**: información sobre las personas 
 - **IMDb_ratings.csv**: información sobre las valoraciones
 - **IMDb_title_principals.csv**: información sobre la relacion de una película y las personas que participan en ella
 
Las versiones subidas en la GitHub, son una versión reducida debido a la limitación de espacio de la propia plataforma. Para obtener el dataset completo, visita el siguiente enlace (https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset?select=IMDb+title_principals.csv).

El software, lo hemos desarrollado en Python utilizando el dataset mencionado anteriormente. Se componen de varios scripts con distintos propósitos. A continuación mostramos una breve descripción de cada script, el código se encuentra subido en nuestro repositorio de GitHub.

 - **movies_by_country.py**: Genera un CSV con el número de películas que ha realizado cada país en nuestro dataset. Como puede haber varios paises participando en una misma película, sólo nos quedamos con el primero que aparece.
 - **ratings_by_country.py**: Genera un CSV con la media de cada país en función de las películas que ha realizado. Al igual que el anterior, sólo tenemos en cuenta el primer país. Debido a que hay columnas desplazadas, es necesario filtrar aquellas valoraciones que no sean de tipo *float*.
  - **movies_by_genre.py**: Al igual que el de los paises, genera un CSV con el número de películas de cada género teniendo en cuenta el primero que aparece.
  - **ratings_by_genre.py**: Al igual que el de las valoraciones por país, genera un CSV con la valoración media de cada género, filtrando aquellas que sean erroneas.
  - **favourites_by_year.py**: Genera un CSV en el que para cada año registrado en el dataset, muestra cual es la película que tiene una mejor valoración.
  - **favourites_by_age.py**: Este script devuelve varios CSV con las 3 películas favoritas de cada rango de edad. Para ello utiliza el fichero *IMDb_ratings.csv*, donde obtiene los identificadores y lo combina con *IMDb_movies.csv* para obtener más detalles de las películas en cuestión.
  - **favourites_by_sex.py**: Tiene un funcionamiento parecido al anterior, sólo que en este caso devuelve dos ficheros CSV con las 11 películas mejor valoradas por hombres y mujeres.
  - **ratings_by_actor.py**: Devuelve un CSV con las valoraciones media de cada actor o actriz ordenadas en orden decreciente por su valoración. Para ello, obtengo del fichero *IMDb_title_principals.csv* la relación entre el identificador de una película y el identificador de una persona para filtrar por actores/actrices. Después proceso el fichero *IMDb_movies.csv*, donde me quedo con las columna con el identificador y su valoración media para más tarde transformarlo en un diccionario. También proceso el fichero *IMDb_names.csv* del cual sme quedo con las columnas con el identificador y su nombre para construir un segundo diccionario. Al final, sólo me queda sustituir el identificador de la película con su nota, y reducir en función del identificador de la persona para obtener su media. Para terminar, sustituyo el identificador por su nombre real para saber de quien se trata.
  - **ratings_by_director.py**: Devuelve un CSV con las valoraciones medias de cada director ordenadas en orden decreciente por su valoración. Tiene un funcionamiento similar al anterior pero en este caso, filtro los directores.
  
 
### - Reproducir nuestro estudio -

Para reproducir nuestro proyecto, podremos ejecutarlo en una instancia de AWS, o bien en nuestro propio computador en modo local. En cualquiera de los dos casos, vamos a suponer que no disponemos de ninguna instalación anterior. Los siguientes pasos sirven como referencia de instalaciones previas en una instancia **m4.xlarge** con **Ubuntu (16.04)**. (Los 5 primeros pasos están sacados del PDF proporcionado por el profesor para la realización del *Hands-on Lab 4 - Install Spark in Local Mode*).

**1. Instalación de Java:**
```markdown
$ sudo apt-add-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt install openjdk-8-jdk
```

**2. Instalación de Scala:**
```markdown
$ sudo apt-get install scala
```

**3. Instalación de Python:**
```markdown
$ sudo apt-get install python
```

**4. Instalación de Spark y configuración del entorno:**

Descargamos Spark de la página, lo descomprimimos y copiamos todos sus archivos a la carpeta 'local'
```markdown
$ sudo curl -O http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
$ sudo tar xvf ./spark-2.2.0-bin-hadoop2.7.tgz
$ sudo mkdir /usr/local/spark
$ sudo cp -r spark-2.2.0-bin-hadoop2.7/* /usr/local/spark
```

**5. Configurar el entorno:**

Debemos añadir spark (*/usr/local/spark/bin*) al *PATH* en el fichero *~/.profile*. Para ello debemos editar dicho fichero y añadirle la siguiente linea:
```markdown
$ export PATH="$PATH:/usr/local/spark/bin"

Después debemos ejecutar el siguiente comando para actualizar el PATH en la sesión actual.
```markdown
*source ~/.profile*
```
En el caso en el que estemos utilizando una VM en AWS, debemos incluir el nombre del host y la ip a la ruta */etc/hosts*. Por ejemplo:
```markdown
$ cat /etc/hosts
127.0.0.1 localhost
172.30.4.210 ip-172-30-4-210
```

**6. Instación de la librería gráfica Matplotlib:**
```markdown
$ sudo apt-get install python-matplotlib
```

**7. Descarga los archivos:**

Se deben crear 3 carpetas; 'scripts/', 'datasets/' y 'results/' de forma similar a la que se ve en el repositorio.
Se copian por tanto los 4 datasets en su carpeta, y los scripts que queramos testar en la correspondiente.


**8. Ejecución de los scripts:**

Antes de lanzar los scripts, y esto debe hacerse **cada vez que arranquemos el computador que estemos usando**, debemos especificar qué tipo de codificación queremos que use Python con:
```markdown
export PYTHONIOENCODING=utf-8
```
Una vez hecho esto, nos situamos dentro de la carpeta 'scripts/' y encargamos a Spark que ejecute el programa. Por ejemplo:
```markdown
spark-submit movies_by_country.py
```

Los resultados se guardarán automáticamente dentro de la carpeta 'results'. Si el script genera un '.csv', se creará una carpeta con un único archivo dentro. El nombre de este archivo es del tipo 'partxxxx-xxxxx', pero se encuentra ya unido con las demás particiones creadas por Spark.
Si por el contrario genera un png, se guardará directamente en la carpeta 'results'.

### - Aspectos avanzados -
- Se ha usado la librería 'matplotlib' para ilustrar algunos resultados que no tenían sentido en caso de ser guardados en 'csv', como el número de pelícuals que realiza cada país.
- Se ha cambiado el número de particiones de los datos para mejorar el rendimiento. Esto se explica en la sección siguiente.
- Se ha explorado la API de los DataFrames de Spark para realizar operaciones como unión de Dataframes

## 5. Resultados

### - Rendimiento -
Cuando hablamos de rendimiento usamos como única métrica el tiempo de ejecución de los scrips. Esto es, el tiempo desde que se le encarga a Spark ejecutarlo (con "spark-submit") hasta que se genera el fichero de salida correspondiente. Sobre un mismo código, conseguimos optimizar este tiempo gracias a las herramientas de paralelización de Spark, que son:

### Número de ejecutores
Si ejecutamos Spark en modo local, este número será siempre 1. Sin embargo, si estamos lanzándolo en un cluster, podremos tener tantos ejecutores como nodos vayamos añadiendo. Eso sí, en este caso será necesario quitar el siguiente fragmento de código en la inicializaciónd de Spark:
```markdown
setMaster('local[*]')
```

### Número de hilos/ejecutor
Una vez más distinguimos entre modo local y cluster. En modo local, especificamos a Spark que queremos que use cree tantos hilos como cores haya disponibles. De ahí la línea:


En modo cluster, le especificamos que use los cores directamente al lanzar el script
```markdown
spark-submit --num-executors <x> --executor-cores <y> script
```

### Número de tareas
Podemos especificar cuántas tareas distintas creará Spark por debajo para hacer el shuffle. Estas tareas coinciden, en nuestro caso, con el número de particiones que se harán del dataframe en cuestión. Usando una heurística bastante fiable, establecemos el nº de tareas (o particiones) al **producto de los ejecutores * hilos que tiene cada uno**. De esta forma conseguimos tiempos óptimos, y significa que cada core disponible estaría encargándose de una partición del dataframe


## 5. Conclusiones

- Hemos realizado un pequeño estudio sobre las películas mejor valoradas para distintos propósitos.

- Mejorar las valoraciones medias utilizando el número de votos y de películas para una valoración más precisa.

- Hemos aprendido a manejar grandes cantidades de datos, a desenvolvernos con el uso de spark y también a utilizar los servicios básicos de AWS.

## 6. Citas

- Repositorio: https://github.com/ramonarj/Cloud-BigData

- Página web: https://ramonarj.github.io/Cloud-BigData/pagina%20web%202.0/index.html

- Dataset de Kaggle: https://www.kaggle.com/stefanoleone992/imdb-extensive-dataset?select=IMDb+title_principals.csv

