import numpy as np
import pandas as pd
import re
from unidecode import unidecode


def normalizar_nombre_columna(df):
    for column in df.columns:
        columna = unidecode(column).lower()
        df.rename(columns={column: columna}, inplace=True)
    return df


def df_manage_nulls(df):
    df.replace(["Preguntar", "NaN","nan","Otro"], np.nan, inplace=True)


def extract_conjunto(row):
    security_related = ["Vigilancia", "Vigilancia 24x7", "Vigilancia privada 24*7", 
                    "Portería / Vigilancia", "Portería / Recepción", "Control de acceso digital", 
                    "Seguridad", "En conjunto cerrado", "Citófono"]
    for feature in security_related:
        if feature in row.index and row[feature] == 1:
            return 1
    return 0

def extract_precio(df, col):
    # Llenar los valores nulos con una cadena vacía temporalmente
    df[col] = df[col].fillna('')

    # Usar expresiones regulares para eliminar los caracteres no deseados
    df[col] = df[col].str.replace('[\?\$]', '', regex=True).str.replace('.', '')

    # Convertir las cadenas vacías de nuevo a np.nan
    df[col] = df[col].replace('', np.nan)

    # Asegurarse de que la columna sigue siendo del tipo de datos correcto
    df[col] = df[col].astype('object')
    return df

def igualar_nombres(df):
    columnas_a_normalizar = ["piso ndeg","Piso N°", "precio(cop)","precio(COP)", "garajes", "floorlevel", "elevadores","area construida","administracion*"]
    columnas_cambiadas = ["piso","piso", "precio","precio", "parqueaderos", "piso", "ascensor","area","administracion"]
    mapeo_columnas = {columna_original: columnas_cambiadas[idx] 
                      for idx, columna_original in enumerate(columnas_a_normalizar) 
                      if columna_original in df.columns}
    df.rename(columns=mapeo_columnas, inplace=True)
    return df


def extract_medidas(col):
    match = re.match(r"(\d+)\s?[a-zA-Z²]*", str(col).strip())
    if match:
        return int(match.group(1))
    else:
        return np.nan
   

def corregir_estrato(row):
    try:
        estrato = float(row)  # Convertir a float para comparación
        if estrato < 2:
            return np.nan
        else:
            return estrato
    except ValueError:
        # Si no se puede convertir a número, devolver NaN
        return np.nan

def extract_ascensor(row):
    ascensor= ["Ascensor", "Ascensor(es) inteligente(s)", "Ascensor Privado", "Ascensores Comunales"]
    for feature in ascensor:
        if feature in row.index and row[feature] == 1:
            return 1
    return 0

def corregir_parqueadero(row):
    if pd.isna(row):
        return 0
    try:
        parqueadero = int(row)  # Convertir a int para comparación
        if parqueadero > 3:
            return 3
        else:
            return parqueadero
    except ValueError:
        # Si no se puede convertir a número, devolver 0 (aunque no debería suceder aquí)
        return 0

def check_conjunto(row):
    if row != "no":
        return 1
    else:
        return 0
    
    
def check_garajes(col):
    match = re.match(r"(\d+)\s?[a-zA-Z²]*", str(col).strip())
    if match:
        return int(match.group(1))
    else:
        return np.nan

def transform_estado(valor):
    if valor == 'si':
        return 'remodelado'
    else:
        return np.nan
    

def dividir_por_dos(valor, patron_numeros):
    if isinstance(valor, str):
        numeros = patron_numeros.findall(valor)
        numeros_validos = [float(num) for num in numeros if num.replace('.', '', 1).isdigit()]
        if len(numeros_validos) == 1:
            return numeros_validos[0]
        elif len(numeros_validos) >= 2:
            n1, n2 = numeros_validos[:2]
            return (n1 + n2) / 2
        else:
            return None
    else:
        return valor   
