#import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Criando uma sessão do spark 
# Utilizando das bases de dados do imdb, temos varias bsese onde podemos coorelacionar
# Para esta ativdade, utilizei uma base onde contem titulos dos filmes generos, etc.
# A outra base é a votação de cada filme.
# utilizei o spark para reorganizar as bases de dados 
spark = (
        SparkSession.builder
        .master('local')
        .appName('Analise STL')
        .getOrCreate()
)
#criando dataframe
df_title = spark.read.option("header", "true").option("sep", "\t").option("multiLine","true").option("quote","\"").option("scape","\"").option("ignoreTrailingWhiteSpace", "true").csv("title.basic.tsv")
df_title.show()

df_movies = df_title.select('primaryTitle', 'originalTitle', 'startYear', 'isAdult', 'runtimeMinutes', 'genres')
df_movies.show()

#dataFrame titles
df_rating = spark.read.option("header", "true").option("sep", "\t").option("multiLine","true").option("quote","\"").option("scape","\"").option("ignoreTrailingWhiteSpace", "true").csv("title.ratings.tsv")
df_rating.show()

df_title.join(df_rating, df_title.tconst == df_rating.tconst, 'inner')\
        .select(df_title.primaryTitle, df_title.originalTitle, df_title.startYear, df_title.genres, df_rating.averageRating)\
        .show(truncate=False)
