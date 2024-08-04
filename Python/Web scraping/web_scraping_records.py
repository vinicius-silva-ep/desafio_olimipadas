from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# Configurar o WebDriver usando WebDriver Manager
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

# Acessar a página web
driver.get('https://worldathletics.org/records/by-category/olympic-games-records')

# Função para aceitar cookies
def accept_cookies():
    try:
        accept_cookies_button = driver.find_element(By.XPATH, '//*[@id="CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"]')
        accept_cookies_button.click()
    except Exception as e:
        print("Botão de cookies não encontrado ou já foi clicado", e)

# Função para extrair a tabela e adicionar a coluna 'gender'
def extract_table_and_add_gender(gender):
    # Se não for 'women', clicar no botão da categoria
    if gender != 'women':
        try:
            category_button = driver.find_element(By.XPATH, category_buttons[gender])
            category_button.click()
        except Exception as e:
            print(f"Botão de categoria '{gender}' não encontrado", e)
        
        # Aguarde um pouco para garantir que a tabela foi atualizada
        time.sleep(2)
    
    # Encontrar a tabela pelo seu seletor
    table = driver.find_element(By.CSS_SELECTOR, 'table')
    
    # Extrair o HTML da tabela
    table_html = table.get_attribute('outerHTML')
    
    # Usar o Pandas para ler o HTML da tabela em um DataFrame
    df = pd.read_html(table_html)[0]
    
    # Adicionar a coluna 'gender'
    df['Gender'] = gender
    
    return df

# Aceitar cookies
accept_cookies()

# Definir os XPaths dos botões de categoria
category_buttons = {
    'men': '//*[@id="__next"]/div[3]/div/div/div[2]/ul/li[2]',
    'mixed': '//*[@id="__next"]/div[3]/div/div/div[2]/ul/li[3]'
}

# Extrair as tabelas para cada categoria e concatenar
df_combined = pd.DataFrame()

# Extrair a tabela para 'women' primeiro, pois já está pré-selecionado
df_women = extract_table_and_add_gender('women')
df_combined = pd.concat([df_combined, df_women], ignore_index=True)

# Extrair tabelas para 'men' e 'mixed'
for gender in ['men', 'mixed']:
    df = extract_table_and_add_gender(gender)
    df_combined = pd.concat([df_combined, df], ignore_index=True)

# Fechar o navegador
driver.quit()

# Exibir o DataFrame combinado
print(df_combined)
