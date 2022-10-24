from time import sleep
from pyspark.sql.session import SparkSession
import hashlib
import requests
import json
import pyspark.sql.functions as F
from schemas import *
import mysql.connector

PRIVATE_KEY = ''
PUBLIC_KEY = ''
TIMESTAMP = '10'
COMBINED_KEYS = TIMESTAMP+ PRIVATE_KEY + PUBLIC_KEY
MD5HASH = hashlib.md5(COMBINED_KEYS.encode()).hexdigest()


CONECTOR_TYPE = "com.mysql.cj.jdbc.Driver"
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "root"
MYSQL_DBNAME = "db"
MYSQL_SERVERNAME = "db_MYSQL"
MYSQL_PORT = "3306"
url_mysql = f'jdbc:mysql://{MYSQL_SERVERNAME}:{MYSQL_PORT}/{MYSQL_DBNAME}'

def get_api_marvel(session, endpoint, offset):

    sleep(2)
    url = f'https://gateway.marvel.com:443/v1/public/{endpoint}?ts={TIMESTAMP}&apikey={PUBLIC_KEY}&hash={MD5HASH}&offset={offset}&limit=100'
    try:
        r = session.get(url)
        return r.json()
    except ConnectionError as e:
        print("CONNECTION ERROR: ")
        print(e)
        error = True
        return error

def write_json(endpoint, get_endpoint):
    with open(f'/usr/app/src/files/{endpoint}_data.json', 'w') as outfile:
        json.dump(get_endpoint['data']['results'], outfile)

def read_json_with_spark(endpoint, schema):
    result = spark.read.schema(schema).json(f'/usr/app/src/files/{endpoint}_data.json', multiLine = "true")
    return result

def write_mysql(endpoint, rawDF, mode):
    rawDF.write \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("driver", CONECTOR_TYPE) \
        .option("dbtable", endpoint) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .mode(mode) \
        .save()

def read_with_spark(endpoint):
    result = spark.read \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("driver", CONECTOR_TYPE) \
        .option("dbtable", endpoint) \
        .option("user", MYSQL_USERNAME) \
        .option("password", MYSQL_PASSWORD) \
        .load()
    return result

def extraction(endpoint, schema):

    sql_config = f"SELECT * FROM config WHERE name = '{endpoint}'"
    cursor.execute(sql_config)
    offset = cursor.fetchone()
    offset = int(offset[1])

    s = requests.Session()
    get_endpoint = get_api_marvel(s, endpoint, offset)
    while get_endpoint is True:
         get_endpoint = get_api_marvel(s, endpoint, offset)

    total = get_endpoint['data']['total']

    while offset < total:

        write_json(endpoint, get_endpoint)
        rawDF = read_json_with_spark(endpoint, schema)

        if endpoint == 'comics':

            rawDF = rawDF.withColumn("thumbnail", F.col("thumbnail.path"))\
                .withColumn("prices", F.explode_outer("prices"))\
                .withColumn("prices", F.col("prices.price"))\
                .withColumnRenamed("prices", "price")\
                .withColumn("modified", F.to_date("modified"))\
                .filter(rawDF.title.isNotNull())

        elif endpoint == 'characters':

            rawDF = rawDF.withColumn("comics", F.arrays_zip("comics.items"))\
                .withColumn("comics", F.explode_outer("comics.items"))\
                .withColumn("comics", F.col("comics.resourceURI"))\
                .withColumn("comics", F.expr("substring(comics, 44, length(comics)-43)"))\
                .withColumnRenamed("comics", "comicId")\
                .withColumn("thumbnail", F.col("thumbnail.path"))\
                .withColumn("modified", F.to_date("modified"))
    
        write_mysql(endpoint, rawDF, "append")

        offset += 100
        update_offset = f"UPDATE config SET offset = {offset} WHERE name = '{endpoint}'"
        cursor.execute(update_offset)
        cnx.commit()

        get_endpoint = get_api_marvel(s, endpoint, offset)

def prefix_columns(endpoint):
    for column in endpoint.columns:
        endpoint = endpoint.withColumnRenamed(column, endpoint + "_" + column)
    return endpoint

def main():

    extraction('comics', comicsSchema)
    extraction('characters', charactersSchema)

    comics = read_with_spark('comics')
    for column in comics.columns:
        comics = comics.withColumnRenamed(column, "comics" + "_" + column)

    characters = read_with_spark('characters')
    for column in characters.columns:
        characters = characters.withColumnRenamed(column, "characters" + "_" + column)

    obtDF = characters.join(comics, characters.characters_comicId == comics.comics_id, how = 'inner') \
        .drop("characters_comicId") \
        .dropDuplicates()

    write_mysql('all_comics_and_characters', obtDF, "overwrite")

if __name__ == '__main__':

    spark = SparkSession.builder \
            .appName("PySpark") \
            .master("local[*]") \
            .config("spark.driver.extraClassPath", "/usr/app/src/java/mysql-connector-java-8.0.22.jar") \
            .getOrCreate()

    cnx = mysql.connector.connect(
    host=MYSQL_SERVERNAME, user=MYSQL_USERNAME, password=MYSQL_PASSWORD, database=MYSQL_DBNAME, port=MYSQL_PORT)
    cursor = cnx.cursor()

    sql = "CREATE TABLE IF NOT EXISTS config \
        SELECT name, offset FROM (VALUES ROW('comics', 0), \
        ROW('characters', 0) ) As config (name, offset)"
    
    cursor.execute(sql)

    main()