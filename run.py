#!/usr/bin/python3.11
import subprocess
import logging
import os
# subprocess.run(["venv/Scripts/activate"], shell=True)
from datetime import datetime
import docker
import platform

filename = f'logs/logs.log'

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filename=filename)

def run_data_pipeline():
    """
    Runs the data pipeline for web scraping, data processing, and data saving.

    This function checks if the Splash container is running and then proceeds to execute
    the web scraping, data processing, and data saving steps.

    Returns:
        None
    """

    container_name = 'selenium-hub'
    #Crear un cliente de Docker
    client = docker.from_env()

    # Verificar si el contenedor está en ejecución
    try:
        container = client.containers.get(container_name)
        print(container.status)
        if container.status == 'running':
            print(f'El contenedor {container_name} ya está en ejecución.')
        elif container.status == 'exited':
            print(f"el container {container_name} ya existe, se va a iniciar:")
            subprocess.run(['docker-compose', 'start'], shell=True)
        else:
            print(f'El contenedor {container_name} no está en ejecución. Iniciando docker-compose up...')
            subprocess.run(['docker-compose', 'up', '-d'], shell=True)
    except docker.errors.NotFound:
        print(f'El contenedor {container_name} no se encontró. O hubo un error :/')
        subprocess.run(['docker-compose', 'up', '-d'], shell=True)

                    
    
    # # check if run splash (sudo docker run -d -p 8050:8050 scrapinghub/splash) 
    # if platform.system() == 'Linux':
    #     if os.system('sudo docker ps | grep selenium/node-chrome:dev') != 0:
    #         logging.error('selenium is not running')
    #         return
    # elif platform.system() == 'Windows':
    #     if os.system('docker ps | findstr selenium/node-chrome:dev') != 0:
    #         logging.error('selenium is not running')
    #         return
    # elif platform.system() == 'Darwin':
    #     if os.system('docker ps | grep sp lselenium/node-chrome:devash') != 0:
    #         logging.error('selenium is not running')
    #         return
    # else:
    #     logging.error('Unsupported operating system')
    #     return

    logging.info(f'Start data pipeline at {datetime.now()}')
    venv_python = os.path.join(os.getcwd(), 'venv', 'Scripts', 'python.exe')
    try:
        # logging.info('Start web scraping HABI')
        # result_habi = subprocess.run([venv_python, 'main.py'], check=True, cwd=".\habi")
        # logging.info(f'Finished web scraping HABI with return code: {result_habi.returncode}')
        
        logging.info('Start web scraping FINCA')
        result_finca = subprocess.run( [venv_python, '-m', 'scrapy', 'crawl', 'loco'], check=True, cwd="./Finca_raiz/finca", capture_output=True)
        print("hola :)")
        logging.info(f'Finished web scraping FINCA with return code: {result_finca.returncode}')
        logging.info(f'Finished web scraping FINCA with return code: {result_finca.returncode}')
        logging.info(f'Standard Output: {result_finca.stdout}')
        logging.info(f'Standard Error: {result_finca.stderr}')
    except subprocess.CalledProcessError as e:
        logging.error(f'Error occurred: {e}')
        logging.error(f'Standard Output: {e.stdout}')
        logging.error(f'Standard Error: {e.stderr}')
    except Exception as e:
        logging.error(f'Unexpected error occurred: {e}')

    logging.info('End web scraping')
    logging.info('Start data processing')
    #subprocess.run(['python3.11', 'ETL/01_initial_transformations.py'])
    #subprocess.run(['python3.11', 'ETL/02_data_correction.py'])
    #subprocess.run(['python3.11', 'ETL/03_data_enrichment.py'])
    #subprocess.run(['python', 'processing.py'])    
    #subprocess.run(['python', 'falto.py'])  
    logging.info('End data prsocessing')
    logging.info('Start data saving')
    #subprocess.run(['python3.11', 'ETL/04_data_save.py'])
    logging.info('End data saving')
    logging.info(f'End data pipeline at {datetime.now()}')


if __name__ == '__main__':
    run_data_pipeline()
