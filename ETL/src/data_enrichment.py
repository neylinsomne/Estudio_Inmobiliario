from shapely.geometry import Point
from unidecode import unidecode
import geopandas as gpd
import numpy as np
import pandas as pd
from math import radians, sin, cos, sqrt, atan2


def get_localidad(row, localidades: gpd.GeoDataFrame) -> str:
    try:
        point = Point(row['longitud'], row['latitud'])
        for i, localidad in localidades.iterrows():
            if point.within(localidad['geometry']):
                return unidecode(localidad['LocNombre']).upper()
        return np.nan
    except:
        return np.nan
    
def get_barrio(row, barrios: gpd.GeoDataFrame) -> str:

    try:
        point = Point(row['longitud'], row['latitud'])
        loca = row['localidad']
        barrios_localidad = barrios.loc[barrios['localidad'] == loca]
        for i, barrio in barrios_localidad.iterrows():
            if point.within(barrio['geometry']):
                return barrio['barriocomu']
            
        return np.nan
    except:
        return np.nan
    

def haversine(lat1, lon1, lat2, lon2):
    # convertimos las coordenadas de grados a radianes
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # formula de Haversine
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    r = 6371  # radio de la Tierra en kilómetros
    return c * r

def calculate_distances(inmuebles, transporte):
    # Crear un DataFrame vacío para almacenar las distancias
    distances_df = pd.DataFrame(index=inmuebles.index)

    for index, trasmi in transporte.iterrows():
        cord_x_tras = trasmi["geometry"].x
        cord_y_tras = trasmi["geometry"].y
        
        # Calcular las distancias usando una función vectorizada
        distances_df[trasmi['nombre']] = inmuebles.apply(lambda row: haversine(row["latitud"], row["longitud"],cord_y_tras, cord_x_tras), axis=1)
    return distances_df



def tomar_menores(row):
    # Crear una lista de pares (nombre, distancia) para cada columna y su valor en la fila
    estaciones_distancias = [(col, row[col]) for col in row.index]
    
    # Ordenar la lista por distancia (segundo elemento del par)
    estaciones_distancias.sort(key=lambda x: x[1])
    
    # Tomar las primeras 5 estaciones más cercanas
    estaciones_distancias = estaciones_distancias[:5]
    
    # Separar los nombres de las estaciones y las distancias en dos listas
    nombres_estaciones = [estacion for estacion, distancia in estaciones_distancias]
    distancias = [distancia for estacion, distancia in estaciones_distancias]
    
    return pd.Series([distancias, nombres_estaciones], index=['estaciones_distancias_menores', 'estaciones_nombres_menores'])

def obtener_troncales_lista(estaciones, df_estaciones):
    # Crear un diccionario de mapeo de nombres de estaciones a troncales
    nombre_to_troncal = pd.Series(df_estaciones["troncal"].values, index=df_estaciones["nombre"]).to_dict()
    # Obtener las troncales correspondientes a las estaciones en la lista
    troncales = [nombre_to_troncal[estacion] for estacion in estaciones if estacion in nombre_to_troncal.keys()]
    return troncales



#TRATAMIENTO DE PARQUES
def get_distance_to_park(lat, lon, localidad, df) -> (str, float):
    """
    Calculates the distance between a given location and the nearest park.

    Parameters:
    - lat (float): Latitude of the location.
    - lon (float): Longitude of the location.
    - localidad (str, optional): Name of the locality. If provided, only parks within the specified locality will be considered.

    Returns:
    - tuple: A tuple containing the name of the nearest park and the distance to it.
    """
    try:
        if pd.notna(localidad):
            parques_localidad = df.loc[df['LOCALIDAD'] == localidad]
        else:
            parques_localidad = df.copy()

        parques_localidad['distancia'] = parques_localidad.apply(lambda x: haversine(lat, lon, x['LATITUD'], x['LONGITUD']), axis=1)
        parque_cercano = parques_localidad.loc[parques_localidad['distancia'].idxmin()]
        nombre = 'PARQUE ' + parque_cercano['TIPO DE PARQUE'] + ' ' + parque_cercano['NOMBRE DEL PARQUE O ESCENARIO']
        return nombre, round(parque_cercano['distancia'], 2)
    except Exception as e:
        print(lat, lon, localidad)
        return None, None

    

def is_near_park(distancia):
    """
    Determines if a location is near a park based on the given distance.

    Parameters:
    distancia (float): The distance to the nearest park in meters.

    Returns:
    int: 1 if the location is near a park, 0 otherwise.
    """
    if distancia <= 500:
        return 1
    else:
        return 0
    



    