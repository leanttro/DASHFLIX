import os
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

DIRECTUS_URL = os.getenv("DIRECTUS_URL", "http://localhost:8055").rstrip('/')
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")

def etl_netflix():
    import kaggle
    
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
    
    tamanho_lote = 100
    for i in range(0, len(registos), tamanho_lote):
        lote = registos[i:i+tamanho_lote]
        resposta = requests.post(url, headers=headers, json=lote)
        
        if resposta.status_code not in [200, 204, 201]:
            print(f'Erro no lote {i}: {resposta.text}')
        else:
            print(f'Lote {i} processado com sucesso')

if __name__ == '__main__':
    etl_netflix()
