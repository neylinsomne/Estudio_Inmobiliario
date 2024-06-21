import geopandas as gpd
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import pymongo
import logging
import os
from dotenv import load_dotenv

# Configurar el registro de logs
filename = 'logs/rela/03_data_enrichment.log'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)

# Cargar variables de entorno
load_dotenv()

# Cambiar directorio de trabajo si es necesario
if os.getcwd().split('/')[-1] == 'ETL':
    logging.info('Cambiando directorio de trabajo')
    os.chdir('..')

# Conectar a MongoDB
logging.info('Connecting to MongoDB')
try:
    client = pymongo.MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB_PRO')]
    collection = db[os.getenv('MONGO_COLE_PRO')]
    logging.info('Connected to MongoDB')
except pymongo.errors.ConnectionFailure as error:
    logging.error(error)
    exit(1)

# Convertir la colección de MongoDB a DataFrame de pandas
df = pd.DataFrame(list(collection.find()))

# Verificar si la columna 'geometry' existe y contiene datos geométricos válidos
if 'geometry' not in df.columns:
    logging.error("La columna 'geometry' no existe en el DataFrame")
    exit(1)

# Definir la conexión a PostgreSQL
DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
ENDPOINT = 'localhost'  # dirección del host
USER = 'postgres'       # usuario de la base de datos
PASSWORD = 'xd'         # contraseña del usuario
PORT = 5480           # puerto mapeado de PostgreSQL en Docker
DATABASE = 'postgres'   # nombre de la base de datos

# Crear la URL de conexión
DATABASE_URL = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

conn=psycopg2.connect(host=ENDPOINT,dbname=DATABASE, user=USER,password=PASSWORD,port=PORT)
cur= conn.cursor()

cur.execute("""CREATE TABLE IF NOT_EXISTS person(
            id INR PRIMARY KEY
            name VARCHAR(255),
            age INT)""")
conn.commit()
cur.close()
conn.close()



# Crear la conexión
engine = create_engine(DATABASE_URL)

# Convertir el DataFrame de pandas a un GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry='geometry')

# Convertir la geometría a WKTElement
gdf['geometry'] = gdf['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))

# Especificar el nombre de la tabla en la base de datos
table_name = 'tabla_base'

# Enviar el GeoDataFrame a PostgreSQL
gdf.to_sql(table_name, engine, if_exists='replace', index=False, dtype={'geometry': Geometry('POINT', srid=4326)})

logging.info("Datos geoespaciales insertados en la base de datos exitosamente")
