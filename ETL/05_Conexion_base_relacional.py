import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import pymongo
import numpy as np
import logging
import os
import json

from dotenv import load_dotenv

if os.getcwd().split('/')[-1] == 'ETL':
    logging.info('Cambiando directorio de trabajo')
    os.chdir('..')

load_dotenv()

filename = f'logs/data_processing/03_data_enrichment.log'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)

# Connect to MongoDB
logging.info('Connecting to MongoDB')
try:
    client = pymongo.MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB_PRO')]
    collection = db[os.getenv('MONGO_COLE_analized_01')]
    logging.info('Connected to MongoDB')


except pymongo.errors.ConnectionFailure as error:
    logging.error(error)
    exit(1)


df = pd.DataFrame(list(collection.find()))

# Define tu conexión a PostgreSQL
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
ENDPOINT = 'localhost'  # dirección del host
USER = 'postgres'       # usuario de la base de datos
PASSWORD = 'xd'         # contraseña del usuario
PORT = 5434             # puerto mapeado de PostgreSQL en Docker
DATABASE = 'postgres'   # nombre de la base de datos

# Crear la URL de conexión
DATABASE_URL = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

# Crear la conexión
engine = create_engine(DATABASE_URL)


gdf = gpd.GeoDataFrame(df, geometry='geometry')
gdf['geometry'] = gdf['geometry'].apply(lambda x: WKTElement(x, srid=4326))

# Especifica el nombre de la tabla en la base de datos
table_name = 'mi_tabla_geoespacial'

# Envía el GeoDataFrame a PostgreSQL
gdf.to_sql(table_name, engine, if_exists='replace', index=False, dtype={'geometry': Geometry('POINT', srid=4326)})

print("Datos geoespaciales insertados en la base de datos exitosamente")
