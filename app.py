from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import os
import threading
from dotenv import load_dotenv
from etl_netflix import executar_carga_kaggle

load_dotenv()

app = Flask(__name__)
CORS(app)

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

# ROTA PARA RODAR O ETL PELO BOTÃO
@app.route('/api/run-etl', methods=['POST'])
def run_etl():
    thread = threading.Thread(target=executar_carga_kaggle)
    thread.start()
    return jsonify({"sucesso": True, "mensagem": "Carga iniciada em segundo plano. Os dados aparecerão em breve no Directus."})

# ENDPOINTS DE CONFIGURAÇÃO (substituem acesso direto ao Directus)
@app.route('/api/config', methods=['GET'])
def get_config():
    url = f"{DIRECTUS_URL}/items/configuracoes_dashflix/1"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', {}))
        return jsonify({"erro": "Falha no Directus"}), r.status_code
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/config', methods=['PATCH'])
def update_config():
    data = request.json
    url = f"{DIRECTUS_URL}/items/configuracoes_dashflix/1"
    try:
        r = requests.patch(url, headers=get_headers(), json=data)
        if r.status_code in [200, 204]:
            return jsonify({"sucesso": True})
        return jsonify({"erro": "Falha ao atualizar"}), r.status_code
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# DEMAIS ROTAS EXISTENTES (resumo, top-paises, lancamentos-ano, titulos)
@app.route('/api/resumo', methods=['GET'])
def get_resumo():
    url = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[count]=*&groupBy[]=type"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', []))
        return jsonify({"erro": "Falha no Directus"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/top-paises', methods=['GET'])
def get_top_paises():
    url = f"{DIRECTUS_URL}/items/netflix_titles?limit=10000&fields=country"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            data = r.json().get('data', [])
            contagem = {}
            for item in data:
                pais = item.get('country')
                if pais:
                    p = pais.split(',')[0].strip()
                    contagem[p] = contagem.get(p, 0) + 1
            ord = sorted(contagem.items(), key=lambda x: x[1], reverse=True)[:10]
            return jsonify([{"pais": k, "total": v} for k, v in ord])
        return jsonify({"erro": "Falha API"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/lancamentos-ano', methods=['GET'])
def get_lancamentos_ano():
    url = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[count]=*&groupBy[]=release_year&sort=-release_year&limit=20"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', []))
        return jsonify({"erro": "Falha API"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/titulos', methods=['GET'])
def get_titulos():
    limit = request.args.get('limit', 100)
    url = f"{DIRECTUS_URL}/items/netflix_titles?limit={limit}&sort=-date_added"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', []))
        return jsonify({"erro": "Falha API"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
