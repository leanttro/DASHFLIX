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
        print("Autenticando no Kaggle...")
        kaggle.api.authenticate()
        
        print("Baixando dataset...")
        kaggle.api.dataset_download_files('shivamb/netflix-shows', path='.', unzip=True)
        
        # Otimização de Memória: define dtypes e usa apenas colunas necessárias se quiser
        df = pd.read_csv('netflix_titles.csv')
        df = df.fillna('')
        
        headers = {
            'Authorization': f'Bearer {DIRECTUS_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        url = f'{DIRECTUS_URL}/items/netflix_titles'
        
        registros = df.to_dict(orient='records')
        
        tamanho_lote = 100
        for i in range(0, len(registros), tamanho_lote):
            lote = registros[i:i+tamanho_lote]
            r = requests.post(url, headers=headers, json=lote)
            if r.status_code not in [200, 201, 204]:
                print(f"Erro no lote {i}: {r.text}")
        
        print("Carga completa via Pandas!")
        return True
    except Exception as e:
        print(f"Erro no ETL: {e}")
        return False

if __name__ == '__main__':
    executar_carga_kaggle()
