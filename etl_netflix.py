import os
import pandas as pd
import requests
import kaggle
from dotenv import load_dotenv

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_URL", "").rstrip('/')
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")

def executar_carga_kaggle():
    try:
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files('shivamb/netflix-shows', path='.', unzip=True)
        
        df = pd.read_csv('netflix_titles.csv')
        df = df.fillna('')
        
        headers = {
            'Authorization': f'Bearer {DIRECTUS_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        registos = df.to_dict(orient='records')
        url = f'{DIRECTUS_URL}/items/netflix_titles'
        
        # Envia em lotes
        tamanho_lote = 100
        for i in range(0, len(registos), tamanho_lote):
            lote = registos[i:i+tamanho_lote]
            requests.post(url, headers=headers, json=lote)
        
        return True
    except Exception as e:
        print(f"Erro no ETL: {e}")
        return False

if __name__ == '__main__':
    executar_carga_kaggle()
