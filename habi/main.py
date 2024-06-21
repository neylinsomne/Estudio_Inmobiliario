from pymongo import MongoClient
from propiedades import scrape_info_for_pages
from links import dicc_links
from datetime import datetime
import json
import os
from dotenv import load_dotenv,find_dotenv

dotenv_path=find_dotenv()
load_dotenv(dotenv_path)



# Función para conectar a MongoD
def connect_to_mongodb():
    username = os.getenv('MONGO_DB_USERNAME')
    password = os.getenv('MONGO_DB_PSW')
    client = MongoClient(f'mongodb+srv://{username}:{password}@realstatecolombia.hhtn5cb.mongodb.net')
    db = client['Finca_inmuebles']
    return db

# Función para agregar la fecha a cada documento
def agregar_fecha_a_documentos(data):
    for documento in data:
        documento['fecha'] = datetime.now()
    return data

# Función para guardar o actualizar en MongoDB
def guardar_o_actualizar_en_mongodb(data, db, collection_name):
    collection = db[collection_name]
    # Actualizar los documentos existentes si hay coincidencias, de lo contrario, insertar nuevos documentos
    for documento in data:
        filtro = {'codigo_habi': documento['codigo_habi'], "fecha":documento["fecha"]}  # Suponiendo que 'codigo_habi' es un identificador único
        collection.update_one(filtro, {'$set': documento}, upsert=True)

# Conectar a MongoDB
          
db = connect_to_mongodb()

    # Leer el archivo JSON

    #ruta_archivo_json = "C:/Users/neylp/OneDrive/Escritorio/Scrapeo/Selenium/habi/info/apto_bogota.json"
    #with open(ruta_archivo_json, "r") as archivo:
        #all_info = json.load(archivo)
key=dicc_links("https://habi.co/venta-apartamentos/bogota?page=1")
all_info = scrape_info_for_pages(key)
    # Agregar la fecha a cada documento
all_info_con_fecha = agregar_fecha_a_documentos(all_info)
print(all_info)
    # Nombre de la colección existente
collection_name = "habi_newera"

    # Guardar o actualizar la lista de diccionarios en MongoDB
guardar_o_actualizar_en_mongodb(all_info_con_fecha, db, collection_name)
