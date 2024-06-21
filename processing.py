#!/usr/bin/python3.11
from datetime import datetime
import subprocess
import logging

filename = f'logs/data_processing/data_processing.log'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename, filemode="w")

def run_data_processing():
    logging.info(f'Start data pipeline at {datetime.now()}')

    logging.info('Start data processing')
    #subprocess.run(['python', 'ETL/01_initial_transformations.py'], shell=True)
    logging.info('ETL,01 done ')
    #subprocess.run(['python', 'ETL/02_data_correction.py'], shell=True)
    logging.info('ETL,02 done ')
    #subprocess.run(['python', 'ETL/03_data_enrichment.py'], shell=True)
    logging.info('ETL,03 done ')
    logging.info('End data processing')

    logging.info('Start data analysis')
    subprocess.run(['python', 'ETL/04_analisis_factorial.py'], shell=True)
    logging.info('End data analysis')
    
    logging.info('Start base relacional')
    #subprocess.run(['python', 'ETL/05_Conexion_base_relacional.py'], shell=True)
    logging.info(f'End data pipeline at {datetime.now()}')



if __name__ == '__main__':
    run_data_processing()