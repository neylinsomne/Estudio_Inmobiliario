import numpy as np
from shapely.geometry import Point, Polygon, MultiPolygon
import geopandas as gpd
import pandas as pd
from unidecode import unidecode
from shapely.geometry import Polygon, MultiPolygon, Point, mapping
from math import radians, sin, cos, sqrt, atan2
from sklearn.preprocessing import StandardScaler

def random_coords_in_polygon(polygon): #Va a recibir una latitud y longitud NULL, y lo va a cambiar por el valor del CENTROIDE DEL BARRIO donde se encuentra el inmueble
    while True:
        x = 4
        y = np.random.uniform(polygon.bounds[1], polygon.bounds[3])
        point = Point(x, y)
        if polygon.contains(point):
            return x, y
        
def getcentroids():
    shp_path = 'transporte//sector.shp.03.24'
    gdf_barrios = gpd.read_file(shp_path)
    gdf_barrios['SCANOMBRE'] = gdf_barrios['SCANOMBRE'].apply(lambda x: x.lower())
    poligonos = gdf_barrios["geometry"]
    gdf_barrios["centroid"] = poligonos.apply(lambda x: x.centroid)
    gdf_barrios["centroid_x"] = gdf_barrios["centroid"].apply(lambda p: p.x)
    gdf_barrios["centroid_y"] = gdf_barrios["centroid"].apply(lambda p: p.y)
    
    

def correction_ubication(row, barrios, localidades):
    getcentroids()
    #gdf_barrios["SCANOMBRE"]
    sector_dict = {
        'CHICO': {
            'polygon': gdf_barrios["SCANOMBRE"].loc[gdf_barrios["SCANOMBRE"]['barriocomu'] == 'S.C. CHICO NORTE', 'geometry'],
            'localidad': 'CHAPINERO',
            'barrio': 'S.C. CHICO NORTE'
        },
        'CEDRITOS': {
            'polygon': gdf_barrios["SCANOMBRE"].loc[barrios['barriocomu'] == 'CEDRITOS', 'geometry'],
            'localidad': 'USAQUEN',
            'barrio': 'CEDRITOS'
        },
        'CHAPINERO ALTO': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'S.C. CHAPINERO NORTE', 'geometry'],
            'localidad': 'CHAPINERO',
            'barrio': 'S.C. CHAPINERO NORTE'
        },
        'LOS ROSALES': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'LOS ROSALES', 'geometry'],
            'localidad': 'CHAPINERO',
            'barrio': 'LOS ROSALES'
        },
        'PUENTE ARANDA': {
            'polygon': localidades.loc[localidades['LocNombre'] == 'PUENTE ARANDA', 'geometry'],
            'localidad': 'PUENTE ARANDA',
            'barrio': 'PUENTE ARANDA'
        },
        'SANTA BARBARA': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'SANTA BARBARA OCCIDENTAL', 'geometry'],
            'localidad': 'USAQUEN',
            'barrio': 'SANTA BARBARA OCCIDENTAL'
        },
        'COUNTRY': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'NUEVO COUNTRY', 'geometry'],
            'localidad': 'USAQUEN',
            'barrio': 'NUEVO COUNTRY'
        },
        'CENTRO INTERNACIONAL': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'SAMPER', 'geometry'],
            'localidad': 'SANTA FE',
            'barrio': 'SAMPER'
        },
        'CERROS DE SUBA': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'S.C. NIZA SUBA', 'geometry'],
            'localidad': 'SUBA',
            'barrio': 'S.C. NIZA SUBA'
        },
        'NIZA ALHAMBRA': {
            'polygon': barrios.loc[barrios['barriocomu'] == 'NIZA SUR', 'geometry'],
            'localidad': 'SUBA',
            'barrio': 'NIZA SUR'
        }
    }

    sector = row['sector']
    if sector in sector_dict:
        if row['localidad'] != sector_dict[sector]['localidad']:
            polygon = sector_dict[sector]['polygon'].iloc[0] # extract the first element of the Series object
            x, y = random_coords_in_polygon(polygon)
            row['latitud'] = float(y)
            row['longitud'] = float(x)
            row['coords_modified'] = True
            row['localidad'] = sector_dict[sector]['localidad']
            row['barrio'] = sector_dict[sector]['barrio']

    return row

def normalizar_nombre_columna(df):
    for column in df.columns:
        columna = unidecode(column).lower()
        df.rename(columns={column: columna}, inplace=True)
    return df

def normalize(row):
    try:
        return unidecode(row).upper()
    except:
        return row
    

def find_polygon_name(point, gdf_polygons):
    for idx, row in gdf_polygons.iterrows():
        if row['geometry'].contains(point):
            return row['nombre']
    return np.nan