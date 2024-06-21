from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
import json

# Configura el WebDriver
def dicc_links(url):
    #service = Service(executable_path='C://Users//neylp//OneDrive//Escritorio//LineMeUp//Extracciones//habi//chromedriver.exe')
    #driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub", DesiredCapabilities.CHROME)
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    options = webdriver.ChromeOptions()
    options.headless = False  
    driver = webdriver.Remote(command_executor="http://localhost:4444", options=options)
    #path_to_extension ="C://Users//neylp//OneDrive//Escritorio//LineMeUp//Extracciones//habi//5.20.0_0.crx"
    
    #options.add_extension(path_to_extension)
    
    # Visita la página que contiene los elementos
    driver.get(url)

    tiempoEspera = 1.2
    time.sleep(3)
    ult = '//*[@id="5"]/button'
    elemento = WebDriverWait(driver, tiempoEspera).until(
        EC.presence_of_element_located((By.XPATH, ult))
    )
    cant_total = int(elemento.text) if elemento.text.isdigit() else 0
    print(f"Son estas páginas: {cant_total}")
    dic = {}
    
    # Inicializa la variable initial_url
    initial_url = url

    for i in range(1, cant_total + 1):
        # Espera hasta que el contenedor de listado sea visible
        time.sleep(tiempoEspera)
        cont = WebDriverWait(driver, tiempoEspera).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'properties-cards'))
        )

        try:
                # Extrae los elementos del enlace
                elementos = cont.find_elements(By.XPATH, './/article/div/header/a')
                href_values = [elemento.get_attribute('href') for elemento in elementos]

                # Almacena la lista de enlaces en el diccionario
                dic[f"Pagina {i}"] = href_values

                # Imprime la cantidad de enlaces en la página actual
                cant = len(href_values)
                print(f"\nSe encontraron: {cant} links en la página {i}")

        except StaleElementReferenceException as e:
                print(f"Error: {e}")
        if i < cant_total:
            el= WebDriverWait(driver, tiempoEspera).until(EC.presence_of_element_located((By.XPATH, '//*[@id="gatsby-focus-wrapper"]/main/article/div[1]/section[3]')))
            elemento = WebDriverWait(driver, tiempoEspera).until(EC.presence_of_element_located((By.CLASS_NAME, 'pagination')))
            botones=driver.find_element(By.XPATH,'//*[@id="gatsby-focus-wrapper"]/main/article/div[1]/section[3]/ul')
            button = botones.find_element(By.XPATH, '//button[@name="Siguiente"]')
            driver.execute_script("arguments[0].scrollIntoView();", button)
            driver.execute_script("arguments[0].click();", button)
            

    driver.quit()
    ruta_archivo_json = "C:/Users/neylp/OneDrive/Escritorio/Scrapeo/Selenium/habi/info/bogota_links.json"
    with open(ruta_archivo_json, "w") as archivo:
        json.dump(dic, archivo)

    return dic


#xd=dicc_links("https://habi.co/venta-apartamentos/bogota?page=1")
