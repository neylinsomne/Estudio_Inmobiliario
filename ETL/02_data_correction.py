"""
This script imports data from CSV files, performs data correction and enrichment, and exports the cleaned data to a new CSV file.
The script first imports apartment data from a CSV file and shapefiles containing information about Bogota's localities and neighborhoods.
It then performs data correction and enrichment, including adding missing locality and neighborhood information to apartments, removing apartments with invalid locality or neighborhood information, and dropping duplicates.
Finally, the cleaned data is exported to a new CSV file.
"""

from src import data_enrichment, data_correction
from unidecode import unidecode
from dotenv import load_dotenv
from datetime import datetime
import logging
import pandas as pd
import geopandas as gpd
import warnings
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
import os
import pymongo
import json

warnings.filterwarnings('ignore')

filename = f'logs/data_processing/02_data_correction.log'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)

# verificar si etoy dentro de la carpeta notebooks o no
if os.getcwd().split('/')[-1] == 'ETL':
    logging.info('Cambiando directorio de trabajo')
    os.chdir('..')

load_dotenv()

logging.info('Connecting to MongoDB')

try:
    client = pymongo.MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB_PRO')]
    collection= db[os.getenv('MONGO_COLE_PREPRO')]
    collection_final = db[os.getenv('MONGO_COLE_localidad')]
    logging.info('Connected to MongoDB')



except pymongo.errors.ConnectionFailure as error:
    logging.error(error)
    exit(1)


def normalize_text(text):
    try:
        return unidecode(text).lower()
    except:
        return text

# Importar Datos
logging.info('Importando datos...')
inmuebles = pd.DataFrame(list(collection.find()))
inmuebles = inmuebles.drop(columns=['_id'], axis=1) 
# df_existente = pd.DataFrame(list(collection_final.find()))
# df_existente = df_existente.drop(columns=['_id'], axis=1)

#inmuebles = pd.read_csv('data/inmuebles.csv')
inmuebles['coords_modified'] = False # Para saber si se modificó la coordenada original

logging.info('Ahora datos externos...')

shp_path = 'data/adicionales/sector.shp.03.24'
gdf_barrios = gpd.read_file(shp_path)
gdf_localidades=gpd.read_file("data/adicionales/poligonos-localidades.geojson")
gdf_barrios.rename(columns={"SCANOMBRE":'nombre'}, inplace=True)
gdf_localidades.rename(columns={'Nombre de la localidad':'nombre'}, inplace=True)

gdf_barrios = data_correction.normalizar_nombre_columna(gdf_barrios)
gdf_localidades = data_correction.normalizar_nombre_columna(gdf_localidades)

gdf_barrios['nombre'] = gdf_barrios['nombre'].apply(data_correction.normalize)
gdf_localidades['nombre'] = gdf_localidades['nombre'].apply(data_correction.normalize)


# Data Corection
logging.info('Corrigiendo datos... (esto puede tardar un rato)')
geometry_points = [Point(xy) for xy in zip(inmuebles['longitud'],inmuebles['latitud'])]

# Convertir a GeoDataFrame
gdf_inmueble = gpd.GeoDataFrame(inmuebles, geometry=geometry_points)
gdf_inmueble['barrio'] = gdf_inmueble['geometry'].apply(lambda x: data_correction.find_polygon_name(x, gdf_barrios))
gdf_inmueble['localidad'] = gdf_inmueble['geometry'].apply(lambda x: data_correction.find_polygon_name(x, gdf_localidades))



# Agregando barrios, segun las coordenadas de los apartamentos

#barrios.localidad.unique()

#barrios.loc[barrios['localidad'] == 'RAFAEL URIBE', 'localidad'] = 'RAFAEL URIBE URIBE'
#barrios.loc[barrios['localidad'].isna(), 'localidad'] = 'SUBA'

#apartments['barrio'] = apartments.apply(data_enrichment.get_barrio, axis=1, barrios=barrios)
#apartments = apartments.apply(data_correction.correction_ubication, axis=1, barrios=barrios, localidades=localidades)

#conditions = {'KENNEDY': [6, 5],    'RAFAEL URIBE URIBE': [6, 5],    'LA PAZ CENTRAL': [6], 'BOSA': [6, 5, 4],    'USME': [6, 5, 4, 3],   'SAN CRISTOBAL': [6, 5, 4], 'CIUDAD BOLIVAR': [6, 5, 4],   'FONTIBON': [6], 'LOS MARTIRES': [6, 5, 1],'SANTA FE': [6, 5],'TUNJUELITO': [6, 5, 4],'BARRIOS UNIDOS': [1, 2, 6],'TEUSAQUILLO': [1, 2, 6],'ANTONIO NARIÑO': [1, 5, 6],'CANDELARIA': [6, 5, 4],
#}
#for loc, estratos in conditions.items():
#    for estrato in estratos:
#        out = apartments.loc[(apartments['localidad'] == loc) & (apartments['estrato'] == estrato)]
#        apartments = apartments.drop(out.index)
#apartments.dropna(subset=['localidad', 'barrio'], how='all', inplace=True)

# del apartments['direccion']
#apartments = apartments.drop_duplicates(subset=['codigo'], keep='first')


# Convertir geometrías a GeoJSON
#gdf_inmueble['geometry'] = gdf_inmueble['geometry'].apply(lambda geom: json.loads(geom.to_geojson()))
gdf_inmueble['geometry'] = gdf_inmueble['geometry'].apply(lambda geom: mapping(geom))

# Convertir geometrías a WKT (Well-Known Text)
#gdf_inmueble['geometry'] = gdf_inmueble['geometry'].apply(lambda geom: geom.wkt)
gdf_inmueble.to_csv('data/subprocesado/.csv', index=False)

logging.info('Saving the processed data to MongoDB')
# leer, buscar si existe, sie existe mirar si es igual, si es igual no hacer nada, si es diferente actualizar, si no existe insertar

# set_nuevos = set(gdf_inmueble["codigo_busqueda"])
# set_existentes = set(df_existente["codigo_busqueda"])

# # Filtrar los códigos que no están en la colección final
# codigos_nuevos = set_nuevos - set_existentes

# nuevos_documentos_df = gdf_inmueble[gdf_inmueble["codigo_busqueda"].isin(codigos_nuevos)]

# # Convertir el DataFrame de nuevos documentos a una lista de diccionarios
# nuevos_documentos = nuevos_documentos_df.to_dict('records')

# # Insertar los nuevos documentos en la colección final
# if nuevos_documentos:
#     collection_final.insert_many(nuevos_documentos)
#     print(f'{len(nuevos_documentos)} documentos nuevos insertados en la colección final.')
# else:
#     print('No hay documentos nuevos para insertar.')

# Actualizar los documentos existentes
try:
    for index, row in gdf_inmueble.iterrows():
        logging.debug(f"Procesando fila {index}: {row['codigo_busqueda']}")
        apartment = collection_final.find_one({'codigo_busqueda': row['codigo_busqueda']})
        
        if apartment is None:
            logging.debug(f"No se encontró el documento con código {row['codigo_busqueda']}, insertando nuevo documento.")
            collection_final.insert_one(row.to_dict())
        else:
            # Eliminar la clave '_id' para comparación
            apartment_id = apartment.pop('_id', None)
            row_dict = row.to_dict()

            # Comparar los documentos
            if apartment != row_dict:
                # Imprimir diferencias para depuración
                logging.debug(f"Diferencias detectadas para el código {row['codigo_busqueda']}:")
                for key in row_dict.keys():
                    if key not in apartment or apartment[key] != row_dict[key]:
                        logging.debug(f"  Campo '{key}': BD='{apartment.get(key)}' -> DF='{row_dict[key]}'")

                # Actualizar documento en MongoDB
                logging.debug(f"Actualizando documento con código {row['codigo_busqueda']}.")
                collection_final.update_one({'codigo_busqueda': row['codigo_busqueda']}, {'$set': row_dict})
            else:
                logging.debug(f"El documento con código {row['codigo_busqueda']} ya está actualizado.")
except Exception as e:
    logging.error(f"Se produjo un error: {e}")
    exit(1)


# Close the connection to MongoDB
logging.info('Closing the connection to MongoDB')
client.close()