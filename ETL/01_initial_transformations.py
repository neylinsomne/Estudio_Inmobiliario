"""
This script connects to a MongoDB database, retrieves data, performs some transformations on it, and saves the transformed data to a CSV file. 

The script first connects to a MongoDB database using the MONGO_URI and MONGO_DATABASE environment variables. It then retrieves data from the 'scrapy_bogota_apartments' collection and stores it in a pandas DataFrame. 

The script then performs two transformations on the data. First, it explodes the 'imagenes' column and saves the resulting DataFrame to a CSV file. Second, it extracts several features from the 'featured_interior', 'featured_zona_comun', 'featured_exterior', and 'featured_sector' columns and saves the resulting DataFrame to a CSV file.

The resulting CSV files are saved in the 'data/processed' and 'data/interim' directories, respectively.

The script requires the following packages to be installed: src, dotenv, logging, pandas, pymongo, os.
"""
from src import extract_features
from dotenv import load_dotenv
import logging
import pandas as pd
import numpy as np
import pymongo
import os
import re

load_dotenv()

filename = f'logs/data_processing/01_initial_transformations.log'


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)

# verificar si etoy dentro de la carpeta notebooks o no
if os.getcwd().split('/')[-1] == 'ETL':
    os.chdir('..')

# Connect to MongoDB
logging.info('Connecting to MongoDB')

try:
    client = pymongo.MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('MONGO_DB_RAW')]
    db_final=client[os.getenv('MONGO_DB_PRO')]
    collection_finca = db[os.getenv('MONGO_COLE_RAW_FINCA')]
    collection_habi = db[os.getenv('MONGO_COLE_RAW_HABI')]
    collection_final = db_final[os.getenv('MONGO_COLE_PREPRO')]
    logging.info('Connected to MongoDB')



except pymongo.errors.ConnectionFailure as error:
    logging.error(error)
    exit(1)

# Data_ from mongo but with out hte id that puts mongo.
df_finca = pd.DataFrame(list(collection_finca.find()))
df_finca = df_finca.drop(columns=['_id'], axis=1) 
df_habi = pd.DataFrame(list(collection_habi.find()))
df_habi = df_habi.drop(columns=['_id'], axis=1)

# df_existente = pd.DataFrame(list(collection_final.find()))
# df_existente = collection_final.drop(columns=['_id'], axis=1)

# Transformations
logging.info('Filtering to get the last scraped data...)')
ultima_fecha_finca = df_finca['fecha'].dt.date.max()
ultima_fecha_habi = df_habi['fecha'].dt.date.max()

# Filtrar el DataFrame para obtener solo las filas con la última fecha
df_finca = df_finca[df_finca['fecha'].dt.date == ultima_fecha_finca]
df_habi = df_habi[df_habi['fecha'].dt.date == ultima_fecha_habi]

#images_explode_F = df_finca.explode('images')
#images_explode_F = images_explode_F.dropna(subset=['images'])
#images_explode_h = df_habi.explode('imagen')
#images_explode_h = images_explode_h.dropna(subset=['imagen'])

#images_df = images_explode_F[['codigo_fr', 'images']].rename(columns={'imagenes': 'url_imagen'})
#images_df.to_csv('data/processed/images_finca.csv', index=False)
#images_df = images_explode_h[['codigo_habi', 'imagen']].rename(columns={'imagenes': 'url_imagen'})
#images_df.to_csv('data/processed/images_habi.csv', index=False)
#logging.info(f'Images saved, shape: {images_df.shape}')

#df_finca = df_finca.drop(columns=['images'], axis=1)
#df_habi = df_habi.drop(columns=['imagen'], axis=1)
descrip_dicc = df_finca['descripciones'].to_dict()
carac_dicc = df_finca['caracteristicas'].to_dict()

df_finca.drop('descripciones', axis=1, inplace=False)
df_finca.drop('caracteristicas', axis=1, inplace=False)
# Crear DataFrames a partir de los diccionarios
df_descrip = pd.DataFrame(descrip_dicc)
df_carac = pd.DataFrame(carac_dicc)
df_descrip = df_descrip.T
df_carac = df_carac.T

#'seguridad': ["Vigilancia", "Vigilancia 24x7", "Vigilancia privada 24*7", "Porter\u00eda / Vigilancia", "Porter\u00eda / Recepci\u00f3n", "Control de acceso digital", "Con cerca el\u00e9ctrica", "Reja de Seguridad", "Seguridad","En conjunto cerrado","Cit\u00f3fono"]
#'zona_verde': ["Jardines Exteriores", "Zonas Verdes", "\u00c1rboles frutales", "B\u00f3sque nativo", "Senderos ecol\u00f3gicos", "Zona Campestre"],
#'gimnasio': ["Gimnasio"],
#'piscina': ["Piscina"]
#'ascensor': ["Ascensor", "Ascensor(es) inteligente(s)", "Ascensor Privado", "Ascensores Comunales"],




logging.info('Transforming data (extract features)')
extract_features.df_manage_nulls(df_finca)
extract_features.df_manage_nulls(df_habi)
extract_features.df_manage_nulls(df_carac)
extract_features.df_manage_nulls(df_descrip)


df_finca['porteria'] = df_carac.apply(lambda x: extract_features.extract_conjunto(x), axis=1)
df_finca['ascensor'] = df_carac.apply(lambda x: extract_features.extract_ascensor(x), axis=1)



df_finca = extract_features.normalizar_nombre_columna(df_finca)
df_carac = extract_features.normalizar_nombre_columna(df_carac)
df_descrip = extract_features.normalizar_nombre_columna(df_descrip)
df_habi = extract_features.normalizar_nombre_columna(df_habi)
df_finca=extract_features.igualar_nombres(df_finca)
df_carac=extract_features.igualar_nombres(df_carac)
df_descrip=extract_features.igualar_nombres(df_descrip)
df_habi=extract_features.igualar_nombres(df_habi)



logging.info(f'Iniciando manejo datos Finca_raiz')
#finca

df_finca= extract_features.extract_precio(df_finca,"precio")
df_finca['precio'] = df_finca['precio'].str.replace(r'[^\d]', '', regex=True)
df_finca["banos"]=df_descrip["banos"]
df_finca["estado"]=df_descrip["estado"]
df_finca["antiguedad"]=df_descrip["antiguedad"]
df_finca["parqueaderos"]=df_descrip["parqueaderos"]
df_finca["piso"]=df_descrip["piso"]
df_finca["habitaciones"]=df_descrip["habitaciones"]
df_finca["estrato"]=df_descrip["estrato"]
df_descrip = extract_features.extract_precio(df_descrip,"administracion")
df_finca['administracion']=df_descrip['administracion']


#extract_medidas

patron_numeros = re.compile(r'(\d+\.?\d*)')
df_finca['antiguedad'] = df_finca['antiguedad'].apply(lambda x: extract_features.dividir_por_dos(x, patron_numeros))
#logging.info(f"estos son :  {df_finca['antiguedad'].unique()}")
#df_finca['antiguedad'] = df_finca['antiguedad'].apply(lambda x: extract_features.extract_medidas)

df_finca['antiguedad'] = df_finca['antiguedad'].fillna(df_finca["antiguedad"].mean())
#logging.info(f"estos son :  {df_finca['antiguedad'].unique()}")

df_finca['administracion'] = df_finca['administracion'].str.replace(r'[^\d]', '', regex=True)
df_finca['area'] = df_descrip['area'].apply(extract_features.extract_medidas)
df_finca.rename(columns={'codigo_fr': 'codigo'}, inplace=True)
df_finca['pagina'] = 'finca_raiz'
#logging.info(f"estos son parqueaderos antes de :  {df_finca['parqueaderos'].unique()}")
df_finca['parqueaderos'] = df_descrip['parqueaderos'].apply(extract_features.extract_medidas)
df_finca['parqueaderos'] = df_finca['parqueaderos'].fillna(0)
df_finca['parqueaderos'] = df_finca['parqueaderos'].apply(extract_features.corregir_parqueadero)
#logging.info(f"estos son parqueaderos despues de :  {df_finca['parqueaderos'].unique()}")
df_finca['piso'] = df_finca['piso'].fillna(0)
df_finca['administracion'] = df_finca['administracion'].fillna(0)
#logging.info(f"estos son estrato antes de :  {df_finca['estrato'].unique()}")
df_finca["estrato"]=df_finca["estrato"].replace('antiguedad', 3).replace("nan",3)
df_finca['estrato'] = df_finca['estrato'].apply(extract_features.corregir_estrato)
df_finca['estrato'] = df_finca['estrato'].fillna(3)
#logging.info(f"estos son estrato despues de :  {df_finca['estrato'].unique()}")

df_finca=df_finca.dropna(subset=['habitaciones', 'banos'])



logging.info(f'Iniciando manejo datos habi')
#habi
df_habi= extract_features.extract_precio(df_habi,"administracion")
logging.info(f"estos son administracion:  {df_habi['administracion'].unique()}")
df_habi["administracion"]= df_habi["administracion"].fillna(0)
#logging.info(f"estos son :  {df_habi['antiguedad'].unique()}")
df_habi['antiguedad'] = df_habi['antiguedad'].apply(extract_features.extract_medidas)
#logging.info(f"estos son :  {df_habi['antiguedad'].unique()}")
df_habi['area'] =  df_habi['area'].apply(extract_features.extract_medidas) #"Casa en Venta" = Conjunto
df_habi = extract_features.extract_precio(df_habi,"precio")
logging.info(f"estos son precio:  {df_habi['precio'].unique()}")
df_habi['porteria'] = df_habi['porteria'].apply(extract_features.check_conjunto)
df_habi['parqueaderos'] = df_habi['parqueaderos'].apply(extract_features.check_garajes)
df_habi.rename(columns={'codigo_habi': 'codigo'}, inplace=True)
df_habi.rename(columns={'remodelado': 'estado'}, inplace=True)
df_habi['estado'] = df_habi['estado'].apply(extract_features.transform_estado)
df_habi['pagina'] = 'habi'
#logging.info(f"estos son :  {df_habi['estrato'].unique()}")
df_habi['parqueaderos'] = df_habi['parqueaderos'].fillna(0)
logging.info(f"estos son parqueaderos:  {df_habi['parqueaderos'].unique()}")
df_habi["estrato"]=df_habi["estrato"].replace('antiguedad', 3).replace("nan",3)
logging.info(f"estos son estrato:  {df_habi['estrato'].unique()}")
logging.info(f'Iniciando boca botella: solo usar columnas necesarias :)')

columnas_interes=["codigo", "precio","administracion","latitud","longitud","area","banos","habitaciones","porteria","antiguedad","estado","piso","parqueaderos","estrato","descripcion", "pagina","fecha"]

df_finca = df_finca[columnas_interes]
df_habi = df_habi[columnas_interes]

df_concatenado = pd.concat([df_finca, df_habi], ignore_index=True)
df_concatenado['latitud'] = df_concatenado['latitud'].replace({None: np.nan})
df_concatenado['longitud'] = df_concatenado['longitud'].replace({None: np.nan})

logging.info(f'cambiando el tipo de los valores del df concatenado :)')

# Convertir las columnas a numéricas, manejando errores y convirtiendo valores no numéricos a NaN
# df_concatenado['precio'] = pd.to_numeric(df_concatenado['precio'], errors='coerce')
# df_concatenado['estrato'] = pd.to_numeric(df_concatenado['estrato'], errors='coerce')
# df_concatenado['administracion'] = pd.to_numeric(df_concatenado['administracion'], errors='coerce')
# df_concatenado['parqueaderos'] = pd.to_numeric(df_concatenado['parqueaderos'], errors='coerce')

# df_concatenado["precio"] = df_finca["precio"].astype('int64')
# df_concatenado["estrato"] = df_finca["estrato"].astype('int64')
# df_concatenado["administracion"] = df_finca["administracion"].astype('int64')
# df_concatenado["parqueaderos"] = df_finca["parqueaderos"].astype('float64')
df_concatenado.to_csv('data/inmuebles.csv', index=False)
logging.info(f'Data saved, shape: {df_finca.shape}')

logging.info('Saving the processed data to MongoDB')
# leer, buscar si existe, sie existe mirar si es igual, si es igual no hacer nada, si es diferente actualizar, si no existe insertar
df_concatenado["codigo_busqueda"]=df_concatenado["codigo"]+"_"+df_concatenado["pagina"]

for col in df_concatenado.columns:
     logging.info(f"columna :{col} las unicas de :  {df_concatenado[col].unique()}")
# set_nuevos = set(df_concatenado["codigo_busqueda"])
# set_existentes = set(df_existente["codigo_busqueda"])

# # Filtrar los códigos que no están en la colección final
# codigos_nuevos = set_nuevos - set_existentes

# nuevos_documentos_df = df_concatenado[df_concatenado["codigo_busqueda"].isin(codigos_nuevos)]

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
    for index, row in df_concatenado.iterrows():
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