from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Verificar si las variables de entorno se cargaron correctamente
database_url = os.getenv('MONGO_DB_USERNAME')
secret_key = os.getenv('MONGO_DB_PSW')

print(f'DATABASE_URL: {database_url}')
print(f'SECRET_KEY: {secret_key}')
