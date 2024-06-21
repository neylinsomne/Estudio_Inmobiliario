from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import re 
import time


def cerrar_pop_up(driver):
    popup_xpath = """//*[@id="hs-overlay-cta-167725896369"]"""
    #hs-interactives-modal-overlay
    
    boton_para_cerrar = """//*[@id="interactive-close-button"]"""
    overlay_xpath = """//*[@id="hs-interactives-modal-overlay"]"""

    # Verificar y cerrar el popup si aparece
    try:
        popup_present = EC.presence_of_element_located((By.XPATH, popup_xpath))
        popup = WebDriverWait(driver, 5).until(popup_present)

        if popup:
            print("Popup visible.")
            try:
                close_button_present = EC.element_to_be_clickable((By.XPATH, boton_para_cerrar))
                close_button = WebDriverWait(driver, 3).until(close_button_present)
            
                

                try:
                # Intentar hacer clic en el botón usando Selenium
                    actions = ActionChains(driver)
                    actions.move_to_element(boton_para_cerrar).click().perform()
                    print("Botón clickeado exitosamente usando Selenium.")
                    WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.XPATH, overlay_xpath)))

                except Exception as e_selenium:
                    print(f"Error haciendo clic en el botón usando Selenium: {e_selenium}")

                try:
                    # Intentar hacer clic en el botón usando JavaScript
                    driver.execute_script("arguments[0].scrollIntoView();", button)
                    driver.execute_script("arguments[0].click();", close_button)
                    print("Botón clickeado exitosamente usando JavaScript.")
                except Exception as e_js:
                    print(f"Error haciendo clic en el botón usando JavaScript: {e_js}")

            
            
                try:    
                    button = driver.find_element(By.XPATH, boton_para_cerrar)
                    driver.execute_script("arguments[0].scrollIntoView();", button)
                    driver.execute_script("arguments[0].click();", button)
                        
                    print("Botón clickeado exitosamente ")
                except Exception as e_selenium:
                    print("Botón no se pudo cerrar, fuerza :/ ")
                WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.XPATH, overlay_xpath)))
            except Exception as e_selenium:
                print(f"Error al intentar cerrar el popup: {e_selenium}")

    except Exception as e:
        print(f"Popup no presente o error al intentar cerrar: {e}")


def dicc_links(url):#, keyword, page):
    options = webdriver.ChromeOptions()
    #options.add_extension(path_to_extension)
    #options.add_argument('--disable-popup-blocking')  # Deshabilita el bloqueo de pop-ups
    options.headless = False  # Configúralo en True si no quieres ver la ventana del navegador
    #driver = webdriver.Chrome(service=service, options=options)
    driver = webdriver.Remote(command_executor="http://localhost:4444", options=options)
    tiempoEspera=4
    #hs-overlay-cta-167933320348  
    driver.get(url)
    #time.sleep(20)
    
    ult = """//*[@id="listingResult"]/div[3]/div/nav/ul/li[8]/button"""
    elemento = WebDriverWait(driver, tiempoEspera).until(
        EC.presence_of_element_located((By.XPATH, ult))
    )

    cant_total = int(elemento.text) if elemento.text.isdigit() else 0
    print(f"Son {cant_total} páginas")
    dic = {}

    for i in range(1, cant_total + 1):
        cerrar_pop_up(driver)


        # Espera hasta que el contenedor de listado sea visible
        cont = WebDriverWait(driver, tiempoEspera).until(
            EC.presence_of_element_located((By.ID, "listingContainer"))
        )

        # Extrae los elementos del enlace
        elementos = cont.find_elements(By.XPATH, '//*[@id="listingContainer"]/div/article/a')
        href_values = [elemento.get_attribute('href') for elemento in elementos]

        # Almacena la lista de enlaces en el diccionario
        dic[f"Página {i}"] = href_values

        # Imprime la cantidad de enlaces en la página actual
        cant = len(href_values)
        print(f"\nSe encontraron: {cant} links en la página {i}")

        # Si no es la última página, hacer clic en el botón "Siguiente"
        if i < cant_total:
            siguiente_btn = driver.find_element(By.XPATH, '//*[@id="listingResult"]/div[3]/div/nav/ul/li[9]')
            try:
                siguiente_btn.click()
            # Espera un tiempo para asegurarse de que la página siguiente se cargue completamente
                time.sleep(1)
            except:
                cerrar_pop_up(driver)
                siguiente_btn.click()
                time.sleep(1)

    
    driver.quit()
    return dic


def link(driver,tiempoEspera,url):
        driver.get(url)
     # Espera hasta que el contenedor de listado sea visible
        cont = WebDriverWait(driver, tiempoEspera).until(
            EC.presence_of_element_located((By.ID, "listingContainer"))
        )
        # Extrae los elementos del enlace
        elementos = cont.find_elements(By.XPATH, '//*[@id="listingContainer"]/div/article/a')
        href_values = [elemento.get_attribute('href') for elemento in elementos]
        return href_values


def dicc_links_2(url):#, keyword, page):
    options = webdriver.ChromeOptions()
    #options.add_extension(path_to_extension)
    #options.add_argument('--disable-popup-blocking')  # Deshabilita el bloqueo de pop-ups
    options.headless = False  # Configúralo en True si no quieres ver la ventana del navegador
    #driver = webdriver.Chrome(service=service, options=options)
    driver = webdriver.Remote(command_executor="http://localhost:4444", options=options)
    tiempoEspera=0.1
    #hs-overlay-cta-167933320348  
    driver.get(url)
    
    
    ult = """//*[@id="listingResult"]/div[3]/div/nav/ul/li[8]/button"""
    elemento = WebDriverWait(driver, tiempoEspera).until(
        EC.presence_of_element_located((By.XPATH, ult))
    )

    cant_total = int(elemento.text) if elemento.text.isdigit() else 0
    print(f"Son {cant_total} páginas")
    
    dic = {}

    for i in range(1, cant_total + 1):
        url_nueva=f"https://www.fincaraiz.com.co/finca-raiz/venta/bogota?pagina={i}"
        href_values=link(driver,tiempoEspera, url_nueva)
        cant = len(href_values)
        print(f"\nSe encontraron: {cant} links en la página {i}")
        dic[f"Página {i}"] = href_values
    driver.quit()    
    return dic



if __name__ =="__main__":
    x=dicc_links_2("https://www.fincaraiz.com.co/finca-raiz/venta/bogota?pagina=1")
    print(x)