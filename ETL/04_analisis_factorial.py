
from sklearn.decomposition import PCA
import pandas as pd
from pandas import DataFrame
import geopandas  as gpd
from shapely.geometry import shape, mapping
from sklearn.preprocessing import StandardScaler
import pymongo
import numpy as np
import logging
import os
import json

from dotenv import load_dotenv


def run_data_analisis():
    if os.getcwd().split('/')[-1] == 'ETL':
        logging.info('Cambiando directorio de trabajo')
        os.chdir('..')

    load_dotenv()

    log_dir = 'logs/data_processing'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    filename = os.path.join(log_dir, '04_data_analisis.log')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)
    logging.info('Inicio de análisis de datos')


    # Connect to MongoDB
    logging.info('Connecting to MongoDB')
    try:
        client = pymongo.MongoClient(os.getenv('MONGO_URI'))
        db = client[os.getenv('MONGO_DB_PRO')]
        collection = db[os.getenv('MONGO_COLE_enriched')]
        collection_final = db[os.getenv('MONGO_COLE_analized_01')]
        logging.info('Connected to MongoDB')


    except pymongo.errors.ConnectionFailure as error:
        logging.error(error)
        exit(1)


    df = pd.DataFrame(list(collection.find()))
    df = df.drop(columns=['_id'], axis=1) 

    logging.info(f'Datos cargados desde MongoDB: {df.head()}')

    # Verificar que las columnas de interés existen en el DataFrame
    columnas_numer = ['area', 'banos', 'habitaciones', 'antiguedad', 'parqueaderos', 'estrato', 'porteria']

    # Verificar el contenido de las columnas antes de aplicar dropna
    for col in columnas_numer:
        if col in df.columns:
            logging.info(f'Valores únicos en la columna {col} antes de cambiar su astype: {df[col].unique()}')

    # Convertir la columna 'geometry' a objetos geométricos de Shapely
    #df['geometry'] = df['geometry'].apply(lambda x: shape(x))

    # Convertir DataFrame a GeoDataFrame
    #gdf = gpd.GeoDataFrame(df, geometry='geometry')
    for col in columnas_numer:
        if col in df.columns:
            try:
                df[col] = df[col].astype(float)
                logging.info(f'La columna {col} ha sido convertida a float.')
            except ValueError as e:
                logging.error(f'Error al convertir la columna {col} a float: {e}')
        
    df["precio"] = df["precio"].astype(float)

    for col in columnas_numer:
            if col in df.columns:
                logging.info(f'Valores únicos en la columna {col} despues de cambiar su astype: {df[col].unique()}')
    
    df_copy=df.copy()
    df_copy = df_copy.dropna(how='any').reset_index(drop=True)
    #df_copy = df_copy.dropna(how='any').reset_index(drop=True)
    datos_seleccionados = df_copy[columnas_numer]

    # Estandarizar los datos
    scaler = StandardScaler()
    datos_escalados = scaler.fit_transform(datos_seleccionados)

    # Aplicar PCA
    pca = PCA(n_components=1)  # Especifica el número de componentes principales que deseas obtener
    componentes_principales = pca.fit_transform(datos_escalados)

    # Crear un nuevo DataFrame con los componentes principales
    df_componentes = pd.DataFrame(data=componentes_principales, columns=['Componente1'])

    # Agregar los componentes principales al DataFrame original
    df = pd.concat([df, df_componentes], axis=1)


    # Obtener los pesos de las columnas en cada componente principal
    componentes = pca.components_

    # Crear un DataFrame para visualizar los pesos de las columnas en cada componente
    df_pesos = pd.DataFrame(data=componentes.T, columns=['Componente1'], index=datos_seleccionados.columns)

    # Ver los pesos de las columnas en cada componente principal
    logging.info(f'Los pesos fueron explotados exitosamente')

    # Varianza explicada por cada componente
    varianza_explicada = pca.explained_variance_ratio_
    logging.info(f'La varianza explicada por comp es: {varianza_explicada}')



    # Varianza explicada acumulada
    varianza_explicada_acumulada = np.cumsum(varianza_explicada)
    logging.info(f'La varianza explicada acumulada es: {varianza_explicada_acumulada}')

    component_dict = df_pesos['Componente1'].to_dict()
    #component_dict


    logging.info(f'INICIO DE ADAPTACION PESOS A VALORES')

    for col in df_copy[columnas_numer].columns:
        if col in component_dict:
            print(col)
            df[col] *= component_dict[col]

    df_copy["score_base"]=df_copy[columnas_numer].abs().sum(axis=1)

    df_copy['price_score_base_ratio'] = df_copy['precio'] / df_copy['score_base']

    df[["price_score_base_ratio", "score_base"]] = df_copy[["price_score_base_ratio", "score_base"]]
    
    
    
    logging.info(f'Los inmuebles fueron completamente pesados :)')
    logging.info(f'Ahora convirtuiendo a GDF:')
    #df['geometry'] = df['geometry'].apply(lambda x: shape(json.loads(x)))
    #df['geometry'] = df['geometry'].apply(lambda x: mapping(x))
    df['geometry'] = df['geometry'].apply(lambda x: shape(x))
    logging.info(f'Ahora convirtuiendo a GDF: {df["geometry"].head()}')
# Convertir el DataFrame a un GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: mapping(geom))
    
    component_dict['varianza_explicada'] = varianza_explicada
    component_dict['varianza_explicada_acumulada'] = varianza_explicada_acumulada

    for key, value in component_dict.items():
        if isinstance(value, np.ndarray):
            component_dict[key] = value.tolist()

    # Ahora puedes serializar component_dict a JSON
    with open('componentes.json', 'w') as json_file:
        json.dump(component_dict, json_file)

    #conectar a bases relacionales


    # Descripción estadística de la nueva columna
    #print(df['price_score_ratio'].describe())

    # Identificar outliers
    #outliers = df[df['price_score_ratio'] > df['price_score_ratio'].quantile(0.95)]
    #print(outliers)

    logging.info('Saving processed data...')
    df.to_csv('data/procesado/inmuebles.csv', index=False)

    
    try:
        for index, row in gdf.iterrows():
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
    
    # Visualizar la relación
    import matplotlib.pyplot as plt
    logging.info('Creando imagenes de relacion')
    try:
        plt.scatter(df['score_base'], df['precio'])
        plt.xlabel('Score')
        plt.ylabel('Price')
        plt.title('Relationship between Score and Price')
        plt.savefig('data/score_price_relationship.png')  # Guardar la imagen
        plt.close()  # Cerrar la figura para liberar memoria
    except Exception as error:
        logging.error(error)
        exit(1)


    try:
        # Visualizar la distribución del ratio y guardar la gráfica
        plt.hist(df['price_score_base_ratio'], bins=50)
        plt.xlabel('Price/Score Ratio')
        plt.ylabel('Frequency')
        plt.title('Distribution of Price/Score Ratio')
        plt.savefig('data/price_score_ratio_distribution.png')  # Guardar la imagen
        plt.close()  # Cerrar la figura para liberar memoria
    except Exception as error:
        logging.error(error)
        exit(1)


#run_data_analisis()

if __name__=='__main__':
    run_data_analisis()
