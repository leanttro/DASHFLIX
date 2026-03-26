from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import os
import threading
from dotenv import load_dotenv
from etl_netflix import executar_carga_kaggle

load_dotenv()

app = Flask(__name__)

# CONFIGURAÇÃO DE CORS RADICAL PARA NÃO DAR ERRO
CORS(app, resources={r"/*": {"origins": "*"}})

DIRECTUS_URL = os.getenv("DIRECTUS_URL", "http://localhost:8055").rstrip('/')
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")

def get_headers():
    return {
        "Authorization": f"Bearer {DIRECTUS_TOKEN}",
        "Content-Type": "application/json"
    }

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/painel')
def painel():
    return send_from_directory('.', 'painel.html')

@app.route('/<path:path>')
def static_files(path):
    return send_from_directory('.', path)

@app.route('/api/run-etl', methods=['POST'])
def run_etl():
    try:
        thread = threading.Thread(target=executar_carga_kaggle)
        thread.start()
        return jsonify({"sucesso": True, "mensagem": "Sincronização iniciada em background!"})
    except Exception as e:
        return jsonify({"sucesso": False, "mensagem": str(e)}), 500

@app.route('/api/resumo', methods=['GET'])
def get_resumo():
    url = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[count]=*&groupBy[]=type"
    try:
        r = requests.get(url, headers=get_headers())
        return jsonify(r.json().get('data', []))
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/top-paises', methods=['GET'])
def get_top_paises():
    url = f"{DIRECTUS_URL}/items/netflix_titles?limit=10000&fields=country"
    try:
        r = requests.get(url, headers=get_headers())
        data = r.json().get('data', [])
        contagem = {}
        for item in data:
            pais = item.get('country')
            if pais:
                p = pais.split(',')[0].strip()
                contagem[p] = contagem.get(p, 0) + 1
        ord = sorted(contagem.items(), key=lambda x: x[1], reverse=True)[:10]
        return jsonify([{"pais": k, "total": v} for k, v in ord])
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/lancamentos-ano', methods=['GET'])
def get_lancamentos_ano():
    url = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[count]=*&groupBy[]=release_year&sort=-release_year&limit=20"
    try:
        r = requests.get(url, headers=get_headers())
        return jsonify(r.json().get('data', []))
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/titulos', methods=['GET'])
def get_titulos():
    limit = request.args.get('limit', 100)
    url = f"{DIRECTUS_URL}/items/netflix_titles?limit={limit}&sort=-date_added"
    try:
        r = requests.get(url, headers=get_headers())
        return jsonify(r.json().get('data', []))
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
