from dotenv import load_dotenv
import os
import pymongo
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon

# Cargar las variables de entorno
load_dotenv()

# Verificar que las variables de entorno se cargaron correctamente
mongo_uri = os.getenv('MONGO_URI')
mongo_db_pro = os.getenv('MONGO_DB_PRO')
mongo_cole_enriched = os.getenv('MONGO_COLE_enriched')
mongo_cole_prepro = os.getenv('MONGO_COLE_PREPRO')

print(f'MONGO_URI: {mongo_uri}')
print(f'MONGO_DB_PRO: {mongo_db_pro}')
print(f'MONGO_COLE_enriched: {mongo_cole_enriched}')
print(f'MONGO_COLE_PREPRO: {mongo_cole_prepro}')

# Verificar que las variables no sean None
if not all([mongo_uri, mongo_db_pro, mongo_cole_enriched, mongo_cole_prepro]):
    raise ValueError("Una o más variables de entorno no están definidas.")

# Conectar a MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db_pro]
collection = db[mongo_cole_enriched]
collection_final = db[mongo_cole_prepro]

# Cargar los datos de la colección en un DataFrame
inmuebles = pd.DataFrame(list(collection.find()))


# Iterar sobre las filas del GeoDataFrame y actualizar/incluir documentos en la colección final
for index, row in inmuebles.iterrows():
    apartment = collection_final.find_one({'codigo': row['codigo']})
    if apartment is None:
        collection_final.insert_one(row.to_dict())
    else:
        if apartment != row.to_dict():
            collection_final.update_one({'codigo': row['codigo']}, {'$set': row.to_dict()})


# bulk_operations = []

# for index, row in gdf_inmueble.iterrows():
#     query = {'codigo': row['codigo']}
#     update = {'$set': row.to_dict()}
#     bulk_operations.append(pymongo.UpdateOne(query, update, upsert=True))

# if bulk_operations:
#     result = collection_final.bulk_write(bulk_operations)
#     print(f'Bulk write result: {result.bulk_api_result}')

# print("Proceso completado.")