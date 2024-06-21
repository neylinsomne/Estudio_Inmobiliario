from dotenv import load_dotenv
from unidecode import unidecode
from src import data_enrichment
import math
import pymongo
import logging
#import requests
import numpy as np
import pandas as pd
import geopandas as gpd
import os

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
    collection = db[os.getenv('MONGO_COLE_localidad')]
    collection_final = db[os.getenv('MONGO_COLE_enriched')]
    logging.info('Connected to MongoDB')


except pymongo.errors.ConnectionFailure as error:
    logging.error(error)
    exit(1)

def normalize(text):
    try:
        return unidecode(text).upper()
    except:
        return text



# Read apartments data
logging.info('Reading apartments data...')
inmuebles = pd.DataFrame(list(collection.find()))
inmuebles = inmuebles.drop(columns=['_id'], axis=1) 
# df_existente = pd.DataFrame(list(collection_final.find()))
# df_existente = df_existente.drop(columns=['_id'], axis=1)
#inmuebles = pd.read_csv('data/subprocesado/inmuebles.csv', low_memory=False)

# Get TransMilenio stations data
logging.info('Getting TransMilenio stations data...')
gdf_sitp=gpd.read_file("data/adicionales/psitp")
gdf_trasmi=gpd.read_file("data/adicionales/Estaciones_Troncales_de_TRANSMILENIO.geojson")
gdf_trasmi.rename(columns={"nombre_estacion":'nombre'}, inplace=True)
gdf_sitp.rename(columns={'consola_pa':'nombre'}, inplace=True)
gdf_trasmi.rename(columns={"troncal_estacion":'troncal'}, inplace=True)
gdf_sitp.rename(columns={'via_par':'troncal'}, inplace=True)

#response = requests.get('https://gis.transmilenio.gov.co/arcgis/rest/services/Troncal/consulta_estaciones_troncales/FeatureServer/1/query?where=1%3D1&outFields=*&f=json').json()
#troncal_transmilenio = pd.DataFrame(response['features'])
#troncal_transmilenio = pd.json_normalize(troncal_transmilenio['attributes'])


logging.info('Adding Public Transport data, it will take a while...')






# Llamar a la función para calcular todas las distancias
distances_df_trasmi = data_enrichment.calculate_distances(inmuebles, gdf_trasmi)
distances_df_sitp = data_enrichment.calculate_distances(inmuebles, gdf_sitp)

# crear columnas listas sobre las distancias menores a 500m
result_sitp=distances_df_sitp.apply(data_enrichment.tomar_menores, axis=1)
result_trasmi=distances_df_trasmi.apply(data_enrichment.tomar_menores, axis=1)

inmuebles=pd.concat([inmuebles, result_sitp], axis=1)
inmuebles.rename(columns={"estaciones_distancias_menores":"sitp_distancias_menores"}, inplace=True )
inmuebles.rename(columns={"estaciones_nombres_menores":"sitp_nombres_menores"}, inplace=True )

inmuebles=pd.concat([inmuebles, result_trasmi], axis=1)
inmuebles.rename(columns={"estaciones_distancias_menores":"trasmi_distancias_menores"}, inplace=True )
inmuebles.rename(columns={"estaciones_nombres_menores":"trasmi_nombres_menores"}, inplace=True )

logging.info('Adding troncales info of sitp and trasmilenio...')
inmuebles['troncales_trasmi_menores'] = inmuebles['trasmi_nombres_menores'].apply(lambda x: data_enrichment.obtener_troncales_lista(x, gdf_trasmi))
inmuebles['troncales_sitp_menores'] = inmuebles['sitp_nombres_menores'].apply(lambda x: data_enrichment.obtener_troncales_lista(x, gdf_sitp))




#PARQUES:

logging.info('Adding parks data...')
# Get parks data
parques = pd.read_csv('data/adicionales/espacios_para_deporte_bogota/directorio-parques-y-escenarios-2023-datos-abiertos-v1.0.csv')



logging.info('Adding parque_cercano and distancia_al_parque columns...')
inmuebles[['parque_cercano', 'distancia_parque_m']] = inmuebles.apply(lambda x: data_enrichment.get_distance_to_park(x['latitud'], x['longitud'], x['localidad'],parques), axis=1, result_type='expand')
inmuebles['is_cerca_parque'] = inmuebles['distancia_parque_m'].apply(data_enrichment.is_near_park)

# Save processed data
logging.info('Saving processed data...')
inmuebles.to_csv('data/procesado/enriched.csv', index=False)


# set_nuevos = set(inmuebles["codigo_busqueda"])
# set_existentes = set(df_existente["codigo_busqueda"])

# # Filtrar los códigos que no están en la colección final
# codigos_nuevos = set_nuevos - set_existentes

# nuevos_documentos_df = inmuebles[inmuebles["codigo_busqueda"].isin(codigos_nuevos)]

# # Convertir el DataFrame de nuevos documentos a una lista de diccionarios
# nuevos_documentos = nuevos_documentos_df.to_dict('records')

# # Insertar los nuevos documentos en la colección final
# if nuevos_documentos:
#     collection_final.insert_many(nuevos_documentos)
#     print(f'{len(nuevos_documentos)} documentos nuevos insertados en la colección final.')
# else:
#     print('No hay documentos nuevos para insertar.')

# # Actualizar los documentos existentes
try:
    for index, row in inmuebles.iterrows():
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
    
logging.info('Thats it :)') 