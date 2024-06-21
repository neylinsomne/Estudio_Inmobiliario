
from sklearn.decomposition import PCA
import pandas as pd
from pandas import DataFrame
import geopandas  as gpd
from sklearn.preprocessing import StandardScaler
import pymongo
import numpy as np
import logging
import os
import json


def pca(df,cols):
    df["a"]=df[cols].apply(pCA_01)
    return df


def pCA_01(row):
    return row