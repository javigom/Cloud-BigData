# MOVIE TRENDS

## 1. INTRODUCCIÓN - MICHAEL
 
 Este proyecto nace de una idea muy simple, somos un alto cargo de una empresa de distribución de películas como Netflix y queremos sacar el máximo beneficio de las películas que compramos y producimos, para alcanzar y mantener el mayor número de suscripciones a nuestra plataforma. ¿Cual fue el género más visto en años anteriores? ¿Que películas fueron las mejor valoradas? ¿Que actores/actrices han sido los mejor puntuados? La respuesta a estas preguntas nos ayuda a tomar decisiones para sacar una mayor rentabilidad a nuestra empresa.
 
### Necesidad del Big Data
 El Big Data es necesario por la gran cantidad de datos que manejamos, actualmente +85k películas, con posible expansión de datos a partir de otras plataformas. 
Al estar estructurados la búsqueda y análisis de datos se procesarán a mayor velocidad.
 Haciendo uso de estos datos y de métodos estadísticos se pueden hacer predicciones, a mayor cantidad de datos mayor fiabilidad del resultado. Estas predicciones hablan de los gustos y necesidades de los espectadores y de cómo van evolucionando. La evolución tiene que ver con los avances tecnológicos y culturales, lo que vemos reflejado en los datos.
 

## 2. MODELO DE DATOS - MICHAEL


## 3. DESCRIPCIÓN TÉCNICA - JAVI
 
### ENTORNO DE TRABAJO

Para nuestro proyecto, las herramientas que vamos a utilizar son las siguientes:

- Como lenguaje de programación para todos nuestros scripts, utilizaremos **Python**.
- Para procesar dichos scripts utilizaremos **Apache Spark**, lo que nos permite hacer uso de una programación funcional paralela.
- Para llevar a cabo las pruebas de nuestros scripts, hemos utilizado **Amazon Web Services** como plataforma para la ejecución de dichos scripts. 

### REPRODUCIR NUESTRO ESTUDIO

Para reproducir nuestro proyecto, podremos ejecutarlo en una instancia de AWS, o bien en nuestro propio computador en modo local. En cualquiera de los dos casos, vamos a suponer que no disponemos de ninguna instalación anterior. Los siguientes pasos sirven como referencia de instalaciones previas en una instancia **m4.xlarge** con **Ubuntu (16.04)**. (Los 5 primeros pasos están sacados del PDF proporcionado por el profesor para la realización del *Hands-on Lab 4 - Install Spark in Local Mode*).

**1. Instalación de Java:**
```markdown
$ sudo apt-add-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt install openjdk-8-jdk
```
Comprobación de que todo ha funcionado correctamente:
```markdown
$ java -version
openjdk version "1.8.0_242"
OpenJDK Runtime Environment (build 1.8.0_242-8u242-b08-0ubuntu3~16.04-b08)
OpenJDK 64-Bit Server VM (build 25.242-b08, mixed mode)
```
**2. Instalación de Scala:**
```markdown
$ sudo apt-get install scala
```
Comprobación de que todo ha funcionado correctamente:
```markdown
$ scala -version
Scala code runner version 2.11.6 -- Copyright 2002-2013, LAMP/EPFL
```

**3. Instalación de Python:**
```markdown
$ sudo apt-get install python
```
Comprobación de que todo ha funcionado correctamente:
```markdown
$ python -h
```

**4. Instalación de Spark:**
```markdown
$ sudo curl -O http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
$ sudo tar xvf ./spark-2.2.0-bin-hadoop2.7.tgz
$ sudo mkdir /usr/local/spark
$ sudo cp -r spark-2.2.0-bin-hadoop2.7/* /usr/local/spark
```

**5. Configurar el entorno:**

Debemos añadir */usr/local/spark/bin* al *PATH* en el fichero *.profile*. Para ello debemos editar dicho fichero y añadirle la siguiente linea:
```markdown
export PATH="$PATH:/usr/local/spark/bin"
```
Después debemos ejecutar *source ~/.profile* para actualizar el PATH en la sesión actual. En el caso en el que estemos utilizando una VM en AWS, debemos incluir el nombre del host y la ip a la ruta */etc/hosts*. Por ejemplo:
```markdown
$ cat /etc/hosts
127.0.0.1 localhost
172.30.4.210 ip-172-30-4-210
```

**6. Instalar PIP:**
```markdown
sudo apt-get install python3-pip
```

**7. Instalar Matplotlib:**
```markdown
python -m pip install -U pip
python -m pip install -U matplotlib
```
**8. Subir los scripts y el dataset (sólo si estamos en AWS):**

Mediante el comando **SCP**, necesitaremos subir todos los ficheros necesarios (scripts y dataset) para el correcto desarrollo del estudio.


### NUESTRO SOFTWARE

Como hemos visto anteriormente, nuestro dataset se compone de 4 ficheros complementarios en formato CSV: 
 - **IMDb_movies.csv**: información sobre las películas
 - **IMDb_names.csv**: información sobre las personas 
 - **IMDb_ratings.csv**: información sobre las valoraciones
 - **IMDb_title_principals.csv**: información sobre la relacion de una película y las personas que participan en ella
 
Las versiones subidas en la GitHub, son una versión reducida debido a la limitación de espacio de la propia plataforma.

El software, lo hemos desarrollado en Python utilizando el dataset mencionado anteriormente. Se componen de varios scripts con distintos propósitos.
 - **movies_by_country.py**: Genera un CSV con el número de películas que ha realizado cada país en nuestro dataset. Como puede haber varios paises participando en una misma película, sólo nos quedamos con el primero que aparece.
 - **ratings_by_country.py**: Genera un CSV con la media de cada país en función de las películas que ha realizado. Al igual que el anterior, sólo tenemos en cuenta el primer país. Debido a que hay columnas desplazadas, es necesario filtrar aquellas valoraciones que no sean de tipo *float*.
  - **movies_by_genre.py**: Al igual que el de los paises, genera un CSV con el número de películas de cada género teniendo en cuenta el primero que aparece.
  - **ratings_by_genre.py**: Al igual que el de las valoraciones por país, genera un CSV con la valoración media de cada género, filtrando aquellas que sean erroneas.
  - **favourites_by_age.py**: Este script devuelve varios CSV con las 3 películas favoritas de cada rango de edad. Para ello utiliza el fichero *IMDb_ratings.csv*, donde obtiene los identificadores y lo combina con *IMDb_movies.csv* para obtener más detalles de las películas en cuestión.
  - **favourites_by_sex.py**: Tiene un funcionamiento parecido al anterior, sólo que en este caso devuelve dos ficheros CSV con las 11 películas mejor valoradas por hombres y mujeres.
  - **ratings_by_actor.py**: Devuelve un CSV con las valoraciones media de cada actor o actriz ordenadas en orden decreciente por su valoración. Para ello, obtengo del fichero *IMDb_title_principals.csv* la relación entre el identificador de una película y el identificador de una persona para filtrar por actores/actrices. Después proceso el fichero *IMDb_movies.csv*, donde me quedo con las columna con el identificador y su valoración media para más tarde transformarlo en un diccionario. También proceso el fichero *IMDb_names.csv* del cual sme quedo con las columnas con el identificador y su nombre para construir un segundo diccionario. Al final, sólo me queda sustituir el identificador de la película con su nota, y reducir en función del identificador de la persona para obtener su media. Para terminar, sustituyo el identificador por su nombre real para saber de quien se trata.
  - **ratings_by_director.py**: Devuelve un CSV con las valoraciones medias de cada director ordenadas en orden decreciente por su valoración. Tiene un funcionamiento similar al anterior pero en este caso, filtro los directores.
  

### ASPECTOS AVANZADOS



## 4. RENDIMIENTO - RAMÓN




## 5. CONCLUSIONES









You can use the [editor on GitHub](https://github.com/ramonarj/Cloud-BigData/edit/main/README.md) to maintain and preview the content for your website in Markdown files.

Whenever you commit to this repository, GitHub Pages will run [Jekyll](https://jekyllrb.com/) to rebuild the pages in your site, from the content in your Markdown files.

### Markdown

Markdown is a lightweight and easy-to-use syntax for styling your writing. It includes conventions for

```markdown
Syntax highlighted code block

# Header 1
## Header 2
### Header 3

- Bulleted
- List

1. Numbered
2. List

**Bold** and _Italic_ and `Code` text

[Link](url) and ![Image](src)
```

For more details see [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown/).

### Jekyll Themes

Your Pages site will use the layout and styles from the Jekyll theme you have selected in your [repository settings](https://github.com/ramonarj/Cloud-BigData/settings). The name of this theme is saved in the Jekyll `_config.yml` configuration file.

### Support or Contact

Having trouble with Pages? Check out our [documentation](https://docs.github.com/categories/github-pages-basics/) or [contact support](https://github.com/contact) and we’ll help you sort it out.
