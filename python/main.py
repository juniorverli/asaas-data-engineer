import requests
import json
import mysql.connector
import hashlib

#Keys necessárias para a API e Banco de Dados
TIMESTAMP = '1'
PRIVATE_KEY = ''
PUBLIC_KEY = ''
COMBINED_KEY = TIMESTAMP+PRIVATE_KEY+PUBLIC_KEY
MD5HASH = hashlib.md5(COMBINED_KEY.encode()).hexdigest()
MYSQL_USER = ''
MYSQL_PASSWORD_ROOT = ''


#Conectando ao banco de dados
cnx = mysql.connector.connect(
    host="localhost", user=MYSQL_USER, password=MYSQL_PASSWORD_ROOT, database="db", port="3306")
cursor = cnx.cursor()

#Função de conexão aos endpoints para retornar o json
def get_api_marvel(session, endpoint, offset):
    url = f'https://gateway.marvel.com:443/v1/public/{endpoint}?ts={TIMESTAMP}&apikey={PUBLIC_KEY}&hash={MD5HASH}&offset={offset}&limit=100'
    r = session.get(url)
    return r.json()

#Função específica para tratar a data de modificação, pois em alguns casos algumas datas estavam vindo fora do padrão
def get_datemodified(i):
    if len(i['modified'][:-5]) != 19:
        datemodified = None
    else:
        datemodified = i['modified'][:-5].replace("T", " ")
    return datemodified

#Função criada para a extração dos dados
def extraction(endpoint):

    #Para que não perca nenhum dado foi criado a tabela config no banco de dados onde é retornado o valor do offset que será utilizado na quantidade de retornos dentro da API
    sql = f"SELECT * FROM config WHERE name = '{endpoint}'"
    cursor.execute(sql)
    offset = cursor.fetchone()
    #Retorna o valor de offset
    offset = int(offset[1])
    s = requests.Session()
    #Retorna o valor de quantos characters e comics tem dentro da base da API
    get_endpoint = get_api_marvel(s, endpoint, offset)
    total = get_endpoint['data']['total']

    while offset < total:
        #Aqui executa linha por linha a inserção dos dados de cada tabela
        if endpoint == 'characters':
            for i in get_endpoint['data']['results']:

                for x in i['comics']['items']:
                    sql = "INSERT INTO characters (id, name, description, modified, resourceURI, thumbnail, comicId) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    datemodified = get_datemodified(i)
                    val = (i['id'], i['name'], i['description'], datemodified, i['resourceURI'], i['thumbnail']['path'], x['resourceURI'][43:])

                    cursor.execute(sql, val)

                offset = offset+1
                update_offset = f"UPDATE config SET offset = {offset} WHERE name = 'characters'"
                cursor.execute(update_offset)
                #O commit só é realizado no final da query pois é ele que irá confirmar que não perdemos nenhum dado
                cnx.commit()
        elif endpoint == 'comics':
            for i in get_endpoint['data']['results']:
                sql = "INSERT INTO comics (id, digitalid, title, issueNumber, variantDescription, description, modified, isbn, upc, \
                    diamondCode, ean, issn, format, pageCount, resourceURI, thumbnail) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                datemodified = get_datemodified(i)
                val = (i['id'], i['digitalId'], i['title'], i['issueNumber'], i['variantDescription'], i['description'], datemodified, \
                    i['isbn'], i['upc'], i['diamondCode'], i['ean'], i['issn'], i['format'], i['pageCount'], i['resourceURI'], i['thumbnail']['path']) 

                cursor.execute(sql, val)
                offset = offset+1
                update_offset = f"UPDATE config SET offset = {offset} WHERE name = 'comics'"
                cursor.execute(update_offset)
                #O commit só é realizado no final da query pois é ele que irá confirmar que não perdemos nenhum dado
                cnx.commit()
        s = requests.Session()
        get_endpoint = get_api_marvel(s, endpoint, offset)


def main():

    extraction('characters')
    extraction('comics')

    #Após executado a extração é criado um script que confirma se existe a OBT e caso exista é realizado o DROP da tabela para criação da mesma
    sql = "DROP TABLE IF EXISTS db.all_characters_and_comics; \
        CREATE TABLE db.all_characters_and_comics \
        SELECT cha.id as characterId, cha.name as character_name, \
        cha.description as character_description, cha.modified as character_modified, \
        cha.resourceURI as character_resourceURI, cha.thumbnail as character_thumbnail, \
        co.id as comicId, co.digitalId as comic_digitalId, co.title as comic_title, co.issueNumber as comic_issueNumber, \
        co.variantDescription as comic_variantDescription, co.description as comic_description, co.modified as comic_modified, \
        co.isbn as comic_isbn, co.upc as comic_upc, co.diamondCode as comic_diamondCode, co.ean as comic_ean, co.issn as comic_issn, \
        co.format as comic_format, co.pageCount as comic_pageCount, co.resourceURI as comic_resourceURI, co.thumbnail as comic_thumbnail FROM db.characters as cha \
        INNER JOIN db.comics as co on cha.comicId = co.id;"
    cursor.execute(sql)

main()