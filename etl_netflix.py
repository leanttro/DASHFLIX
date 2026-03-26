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
        
        df = pd.read_csv('netflix_titles.csv')
        df = df.fillna('')
        
        headers = {
            'Authorization': f'Bearer {DIRECTUS_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        # 1. Buscar todos os show_id já existentes no Directus
        url_existentes = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id&limit=10000"
        r = requests.get(url_existentes, headers=headers)
        if r.status_code != 200:
            print(f"Erro ao buscar IDs existentes: {r.text}")
            return False
        
        existing_ids = set(item['show_id'] for item in r.json().get('data', []))
        print(f"Registros existentes: {len(existing_ids)}")
        
        # 2. Filtrar apenas os registros novos
        novos_registros = []
        for _, row in df.iterrows():
            if row['show_id'] not in existing_ids:
                novos_registros.append(row.to_dict())
        
        print(f"Registros novos a inserir: {len(novos_registros)}")
        if not novos_registros:
            print("Nenhum registro novo. Carga cancelada.")
            return True
        
        url = f'{DIRECTUS_URL}/items/netflix_titles'
        
        tamanho_lote = 100
        for i in range(0, len(novos_registros), tamanho_lote):
            lote = novos_registros[i:i+tamanho_lote]
            r = requests.post(url, headers=headers, json=lote)
            if r.status_code not in [200, 201, 204]:
                print(f"Erro no lote {i}: {r.text}")
            else:
                print(f"Lote {i} inserido com sucesso.")
        
        print("Carga completa!")
        return True
    except Exception as e:
        print(f"Erro no ETL: {e}")
        return False

if __name__ == '__main__':
    executar_carga_kaggle()
