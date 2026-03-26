import os
import pandas as pd
import requests
import kaggle
from dotenv import load_dotenv

load_dotenv()

# Pega as configs do seu .env
DIRECTUS_URL = os.getenv("DIRECTUS_URL", "").rstrip('/')
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")

def executar_carga_kaggle():
    try:
        print("Iniciando autenticação no Kaggle...")
        kaggle.api.authenticate()
        
        print("Baixando dataset da Netflix...")
        # O local aqui é a raiz do seu container no Dokploy
        kaggle.api.dataset_download_files('shivamb/netflix-shows', path='.', unzip=True)
        
        print("Lendo CSV e limpando dados...")
        df = pd.read_csv('netflix_titles.csv')
        df = df.fillna('') # Evita erro de nulo no Directus
        
        headers = {
            'Authorization': f'Bearer {DIRECTUS_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        registos = df.to_dict(orient='records')
        url = f'{DIRECTUS_URL}/items/netflix_titles'
        
        print(f"Enviando {len(registos)} registros para o Directus...")
        tamanho_lote = 100
        for i in range(0, len(registos), tamanho_lote):
            lote = registos[i:i+tamanho_lote]
            resposta = requests.post(url, headers=headers, json=lote)
            if resposta.status_code not in [200, 201, 204]:
                print(f"Erro no lote {i}: {resposta.text}")
            else:
                print(f"Lote {i} enviado com sucesso.")
        
        return True
    except Exception as e:
        print(f"ERRO FATAL NO ETL: {e}")
        return False

if __name__ == '__main__':
    executar_carga_kaggle()
