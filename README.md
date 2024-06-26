# Estudio_Inmobiliario



[![Github](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)]((https://github.com/neylinsomne))
[![Linkedin](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/neyl-peñuela-bernate-a76644209/)

 Es NECESARIO tener una cuenta en MongoDB para poder ver los resultados.
 este tendrá el código de busqueda para Finca raiz o para Habi

 ## Cómo usarlo?-
- Cómo correrlo?
  Para poder hacer uso de este proyecto necesitas tener instalado docker para poder hacer uso de las imagenes de pgadnmin, postgras, y selenium. (Si se desea puedes hacer uso de scrapy splash)
  Ya teniendo esto lo único que debes hacer es escribir en tu consola "python run.py".


  Este proyecto es con finees de ESTUDIO, no se recomienda hacer uso de este como una veracidad. Y a su vez, no está permitido el uso de este en el ambiente comercial. *para más información lea License".
- Archivo .env
  Tienes que crear el archivo .env con los diferentes nobmres de bases de datos de esta manera:
  MONGO_DB_RAW=Finca_inmuebles
MONGO_DB_PRO=procesado
MONGO_COLE_RAW_FINCA=indi_finca
MONGO_COLE_RAW_HABI=habi_newera
MONGO_COLE_PRO=inmuebles
MONGO_COLE_localidad=con_localidad
MONGO_COLE_PREPRO=pre-processed
MONGO_COLE_enriched=enriched
MONGO_COLE_analized_01=analized_01
MONGO_COLE_con_score_base=con_score_base
## Próximas actualizaciones:
  Integracion de bases relacionales y query listas para las peticiones a un backend (end points con flask).
