from typing import Any
import scrapy
from scrapy.http import Response
import re
from collections import Counter
from finca.links import dicc_links_2
from datetime import datetime
import time
import json
class LocoSpider(scrapy.Spider):
    name = "loco"
    allowed_domains = ["www.fincaraiz.com.co"]

    #custom_settings = {
    #    'FEEDS': {
    #        '/Data/inmuebles_csv/inmuebles_%(time)s.csv': {
    #            'format': 'csv',
    #        }#,
            #'/Data/inmuebles_json/inmuebles_%(time)s.json': {
            #    'format': 'json',
            #}
    #    }
   # }

    def start_requests(self):
       # transaccion=['venta']
        keyword_list = ['bogota']
        for keyword in keyword_list:
            #for tran in transaccion:
                search_url = f'https://www.fincaraiz.com.co/finca-raiz/venta/{keyword}?pagina={1}'
                #search_url = f'https://www.fincaraiz.com.co/finca-raiz/arriendos/{keyword}?pagina={1}'
                yield scrapy.Request(url=search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': 1})

    def discover_product_urls(self, response):
        keyword = response.meta.get('keyword')
        page = response.meta.get('page')
        dic_pag = dicc_links_2(response.url)
        #dic_pag ={"pui":["https://www.fincaraiz.com.co/proyecto-de-vivienda/paseo-del-parque/ricaurte/bogota/6903734","https://www.fincaraiz.com.co/proyecto-de-vivienda/izola-zentral/la-felicidad/bogota/5310953"]}
        for key in dic_pag:
            search_products = dic_pag[key]
            for url in search_products:
                yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        if "inmueble" in response.url:
            image_url=response.css("div.jss224 img::attr(src)").get()
            ubicacion = response.css('p.jss65.jss73.jss166::text, p.jss65.jss73.jss156::text').get("")
            ubicacion_asociada=response.css('p.jss65.jss73.jss196.jss126::text, p.jss65.jss73.jss186.jss116::text').get("")
            descripcion=response.css('p.jss65.jss73.jss290.jss265::text, p.jss65.jss73.jss281.jss256::text').get("")
            img_ubicacion = response.css('#location img::attr(src)').get()
            url_parts = response.url.split('/')
            codigo_fr = url_parts[-1]

            
            #categories = response.css('.MuiBox-root.jss331.jss329')
            #for category in categories:
            #    category_name = category.css('.jss65.jss74::text').get()
            #    category_value = category.css('.jss65.jss74.jss.*.jss.*::text').get()


            # tabla donde están las descripciones: 'div.MuiBox-root.jss330.jss328'
            descripciones = {}
            x=response.css("div.MuiBox-root.jss330.jss328 p::text").getall()
            caract = [valor for valor in x if valor != 'al anunciante']
            for i in range(0, len(caract), 2):
                variable = caract[i].strip()  # Eliminar espacios en blanco alrededor del nombre
                valor = caract[i + 1].strip()  # Eliminar espacios en blanco alrededor del valor
                descripciones[variable]=valor
                 
            
            

            precio_scrapeado = response.css('div.jss10 p:nth-child(2)::text').get()
            
            
            #img_ubicacion ->id=location
            caracteristicas={}
            papa = response.css('#characteristics p::text').getall()
            for pollito in papa:
                 if pollito != "Características":
                    caracteristicas[pollito]=1# se coloca 1 en cada uno para confirmar su existencia :)
            
            script_data = response.css('script#__NEXT_DATA__::text').get()
            if script_data:
                # Extrae los datos JSON del script
                try:
                    data = json.loads(script_data)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error al decodificar JSON: {e}")
                    return
                # Accede a los datos de ubicación
                locations = data.get('props', {}).get('pageProps', {}).get('locations', {})
                lat = locations.get('lat')
                lon = locations.get('lng')

            # Crea un diccionario con los valores de latitud y longitud
            
            else:
                self.logger.error("No se encontró el script '__NEXT_DATA__' en la página")
                lat=None
                lon=None
            
            # Accede a los datos que necesitas
            page_props = data.get('props', {}).get('pageProps', {})
            
            #id=characteristics
            item= {
                "ubicacion": ubicacion,
                "ubicacion_asociada":ubicacion_asociada,
                "img_ubicacion":img_ubicacion,
                "codigo_fr":codigo_fr,
                #"precio(COP)": precio,
                "images": image_url,
                "descripcion":descripcion,
                "precio(COP)":precio_scrapeado,
                "latitud":lat,
                "longitud":lon
            }
            item["descripciones"]=descripciones
            #item.update(descripciones)
            #item.update(caracteristicas)
            item["caracteristicas"]=caracteristicas
            #print(result)
            item["fecha"]=datetime.now()
            yield item
        
        elif "proyecto-de-vivienda" in response.url:
            # como son varias "variaciones" pues se cargan todas en una lista, en vez de escrapear la misma página una y otra vez.
            variaciones=[]
            #cora=len(response.css('div#typologies div:nth-child(1)  div:nth-child(1)').get())
            #response.css('div#typologies div:nth-child(1)  div:nth-child(1) div').get()
            x=response.css('div#typologies div::attr(class)').getall()
            #xtextos_necesarios = response.css(f"div#typologies div[class*='{x[2]}'] p::text").getall() 
            #s=[vaby for vaby in xtextos_necesarios if "Hab." in vaby]
            #cant_var=len(s)
            item = [' '.join(string.split()[-2:]) for string in x]
            contador = Counter(item)
            max_value = max(contador, key=contador.get)
            clases=max_value.replace(" ",".")
            tipos=response.css(f"div#typologies div[class*='{max_value}']").getall()
            #response.css("div#typologies[class*='jss336 jss634']").getall()



            script_data = response.css('script#__NEXT_DATA__::text').get()
            if script_data:
                # Extrae los datos JSON del script
                try:
                    data = json.loads(script_data)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error al decodificar JSON: {e}")
                    return
                # Accede a los datos de ubicación
                locations = data.get('props', {}).get('pageProps', {}).get('locations', {})
                lat = locations.get('lat')
                lon = locations.get('lng')

            # Crea un diccionario con los valores de latitud y longitud
            
            else:
                self.logger.error("No se encontró el script '__NEXT_DATA__' en la página")
                lat=None
                lon=None
            


            #for i,tipo in range(1, len(tipos)+1):
            for indice, tipo in enumerate(tipos, start=1):
            #for indice in cant_var:
            #VALORES QUE NO CAMBIAN:
                image_url=response.css("div.jss224 img::attr(src)").get()
                ubicacion = response.css('p.jss65.jss73.jss166::text').get("")
                ubicacion_asociada=response.css('p.jss65.jss73.jss196.jss126::text').get("")
                descripcion=response.css('p.jss65.jss73.jss291.jss266::text').get("")
                img_ubicacion = response.css('#location img::attr(src)').get()
                url_parts = response.url.split('/')
                codigo_fr = url_parts[-1] + "_" + indice
                
                # tabla donde están las descripciones: 'div.MuiBox-root.jss330'
                descripciones = {}
                x=response.css("div.MuiBox-root.jss331.jss329 p::text").getall()
                caract = [valor for valor in x if valor != 'al anunciante']
                for i in range(0, len(caract), 2):
                    variable = caract[i].strip()  # Eliminar espacios en blanco alrededor del nombre
                    valor = caract[i + 1].strip()  # Eliminar espacios en blanco alrededor del valor
                    descripciones[variable]=valor
                    
                caracteristicas={}
                papa = response.css('#characteristics p::text').getall()
                for pollito in papa:
                    if pollito != "Características":
                        caracteristicas[pollito]=1# se coloca 1 en cada uno para confirmar su existencia :)
                    
                #id=characteristics
                item= {
                        "ubicacion": ubicacion,
                        "ubicacion_asociada":ubicacion_asociada,
                        "img_ubicacion":img_ubicacion,
                        "codigo_fr":codigo_fr,
                        "images": image_url,
                        "descripcion":descripcion,
                        "latitud":lat,
                        "longitud":lon
                        #"precio(COP)": precio,
                        
                }
                
                #print(result)
                    


            #Valores que sí cambian:
                valores_sub_proyecto = {}

                #response.css('div#typologies div:nth-child(1) div:nth-child(1) p::text').getall()
                caract_tipos=response.css(f"div#typologies div[class*='{max_value}']:nth-child({indice}) p::text").getall()
                #response.css("div#typologies div[class*='jss336 jss634']:nth-child(1) p::text").getall()
                print(caract_tipos)
                time.sleep(0.5)
                for i, cara in enumerate(caract_tipos):
                        if '$' in cara:
                            # Extraer el valor numérico después del símbolo '$', eliminando puntos y comas
                            precio = int(cara.split('$')[1].replace('.', '').replace(',', '').split()[0])
                            valores_sub_proyecto['Precio']=precio
                            #print(precio)
                        elif 'Á. total' in cara:
                            # Asegurarse de no salir del rango de la lista
                            if i + 1 < len(caract_tipos):
                                # Obtener la siguiente etiqueta <p>
                                next_info = caract_tipos[i + 1]
                                
                                # Aplicar el patrón para extraer el valor numérico
                                pattern = r'(\d+,\d+)\s*m²'
                                numeric_match = re.search(pattern, next_info)

                                # Verificar si se encontró un valor numérico
                                if numeric_match:
                                    numeric_value = numeric_match.group(1).replace(',', '.')
                                    area_total = float(numeric_value)
                                    print(area_total)
                                    valores_sub_proyecto['Área construída']=area_total
                                
                                else:
                                    print(f"No se encontró un valor numérico para 'Á. total'. Texto: {next_info}")
                        elif 'Á. privada' in cara:
                            # Asegurarse de no salir del rango de la lista
                            if i + 1 < len(caract_tipos):
                                # Obtener la siguiente etiqueta <p>
                                next_info = caract_tipos[i + 1]
                                
                                # Aplicar el patrón para extraer el valor numérico
                                pattern = r'(\d+,\d+)\s*m²'
                                numeric_match = re.search(pattern, next_info)

                                # Verificar si se encontró un valor numérico
                                if numeric_match:
                                    numeric_value = numeric_match.group(1).replace(',', '.')
                                    area_privada = float(numeric_value)
                                    valores_sub_proyecto['Área privada']= area_privada
                                    print(area_privada)
                                else:
                                    print(f"No se encontró un valor numérico para 'Á. privada'. Texto: {next_info}")

                        elif 'Hab.' in cara:
                        # Asegurarse de no salir del rango de la lista
                            if i + 1 < len(caract_tipos):
                                # Obtener la siguiente etiqueta <p>
                                next_info = caract_tipos[i + 1]
                                
                                # Verificar si el texto es un número
                                if next_info.isdigit():
                                    # Convertir el texto a un entero y agregarlo a la lista
                                    habitaciones = int(next_info)
                                    valores_sub_proyecto['Habitaciones']=habitaciones
                                else:
                                    print(f"El texto siguiente a 'Hab.' no es un número: {next_info}")
                        elif 'Bañ.' in cara:
                            if i + 1 < len(cara):
                                # Obtener la siguiente etiqueta <p>
                                next_info = caract_tipos[i + 1]
                                
                                # Verificar si el texto es un número
                                if next_info.isdigit():
                                    # Convertir el texto a un entero y agregarlo a la lista
                                    banos = int(next_info)
                                    valores_sub_proyecto['Baños']=banos
                                else:
                                    print(f"El texto siguiente a 'Bañ.' no es un número: {next_info}")
                print(valores_sub_proyecto)                    
                item.update(valores_sub_proyecto)
                #item["valores_sub_proyecto"]=valores_sub_proyecto
                #item.update(descripciones)
                item["descripciones"]=descripciones
                item["caracteristicas"]=caracteristicas
                #item.update(caracteristicas)
                print(item)
                variaciones.append(item)
                item["fecha"]=datetime.now()
            #yield variaciones
            return variaciones  

        
