from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import os
from dotenv import load_dotenv

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

@app.route('/api/resumo', methods=['GET'])
def get_resumo():
    url = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[count]=*&groupBy[]=type"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', []))
        return jsonify({"erro": "Falha de comunicacao com Directus"}), 500
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
                    primeiro_pais = pais.split(',')[0].strip()
                    contagem[primeiro_pais] = contagem.get(primeiro_pais, 0) + 1
            
            ordenado = sorted(contagem.items(), key=lambda x: x[1], reverse=True)[:10]
            resultado = [{"pais": k, "total": v} for k, v in ordenado]
            return jsonify(resultado)
        return jsonify({"erro": "Falha na API"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/lancamentos-ano', methods=['GET'])
def get_lancamentos_ano():
    url = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[count]=*&groupBy[]=release_year&sort=-release_year&limit=20"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', []))
        return jsonify({"erro": "Falha na API"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/titulos', methods=['GET'])
def get_titulos():
    limit = request.args.get('limit', 100)
    page = request.args.get('page', 1)
    url = f"{DIRECTUS_URL}/items/netflix_titles?limit={limit}&page={page}&sort=-date_added"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code == 200:
            return jsonify(r.json().get('data', []))
        return jsonify({"erro": "Falha na API"}), 500
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
