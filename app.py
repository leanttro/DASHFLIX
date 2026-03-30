from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import os
import threading
from dotenv import load_dotenv
from etl_netflix import executar_carga_kaggle

load_dotenv()

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

DIRECTUS_URL = os.getenv("DIRECTUS_URL", "http://localhost:8055").rstrip('/')
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")

def get_headers():
    return {
        "Authorization": f"Bearer {DIRECTUS_TOKEN}",
        "Content-Type": "application/json"
    }

def build_filters(req_args):
    filters = []
    ano = req_args.get('ano')
    pais = req_args.get('pais')
    rating = req_args.get('rating')
    tipo = req_args.get('tipo')
    
    if ano:
        filters.append(f"filter[release_year][_eq]={ano}")
    if rating:
        filters.append(f"filter[rating][_eq]={rating}")
    if tipo:
        filters.append(f"filter[type][_eq]={tipo}")
    if pais:
        filters.append(f"filter[country][_contains]={pais}")
    return filters

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

@app.route('/api/save-config', methods=['POST'])
def save_config():
    try:
        config = request.json
        headers = get_headers()
        url = f"{DIRECTUS_URL}/items/configuracoes_dashflix/1"
        r = requests.patch(url, headers=headers, json=config)
        if r.status_code in (200, 204):
            return jsonify({"sucesso": True, "mensagem": "Configurações salvas"})
        else:
            return jsonify({"sucesso": False, "mensagem": r.text}), r.status_code
    except Exception as e:
        return jsonify({"sucesso": False, "mensagem": str(e)}), 500

@app.route('/api/resumo', methods=['GET'])
def get_resumo():
    try:
        filters = build_filters(request.args)
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id,type&limit=10000"
        if filters:
            url += "&" + "&".join(filters)
            
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            return jsonify({"erro": "Erro ao buscar dados"}), 500
        
        data = r.json().get('data', [])
        unique_by_show = {}
        for item in data:
            sid = item.get('show_id')
            if sid and sid not in unique_by_show:
                unique_by_show[sid] = item.get('type')
        
        total = len(unique_by_show)
        movies = sum(1 for t in unique_by_show.values() if t == 'Movie')
        shows = sum(1 for t in unique_by_show.values() if t == 'TV Show')
        
        return jsonify({
            "Movie": movies,
            "TV Show": shows,
            "Total": total
        })
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/top-paises', methods=['GET'])
def get_top_paises():
    try:
        filters = build_filters(request.args)
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id,country&limit=10000"
        if filters:
            url += "&" + "&".join(filters)
            
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            return jsonify({"erro": "Erro ao buscar dados"}), 500
        
        data = r.json().get('data', [])
        
        pais_count = {}
        processed_shows = set()
        for item in data:
            sid = item.get('show_id')
            if sid in processed_shows:
                continue
            processed_shows.add(sid)
            pais = item.get('country')
            if pais:
                primeiro = pais.split(',')[0].strip()
                pais_count[primeiro] = pais_count.get(primeiro, 0) + 1
        
        top = sorted(pais_count.items(), key=lambda x: x[1], reverse=True)[:20]
        return jsonify([{"pais": k, "total": v} for k, v in top])
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/lancamentos-ano', methods=['GET'])
def get_lancamentos_ano():
    try:
        filters = build_filters(request.args)
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id,release_year,country&limit=10000"
        if filters:
            url += "&" + "&".join(filters)
        
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            return jsonify({"erro": "Erro ao buscar dados"}), 500
        
        data = r.json().get('data', [])
        
        year_count = {}
        processed_shows = set()
        for item in data:
            sid = item.get('show_id')
            if sid in processed_shows:
                continue
            processed_shows.add(sid)
            year = item.get('release_year')
            if year:
                year_count[year] = year_count.get(year, 0) + 1
        
        sorted_years = sorted(year_count.items())
        return jsonify([{"release_year": y, "count": c} for y, c in sorted_years])
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/classificacao', methods=['GET'])
def get_classificacao():
    try:
        filters = build_filters(request.args)
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id,rating&limit=10000"
        if filters:
            url += "&" + "&".join(filters)
            
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            return jsonify({"erro": "Erro ao buscar dados"}), 500
        
        data = r.json().get('data', [])
        
        rating_count = {}
        processed_shows = set()
        for item in data:
            sid = item.get('show_id')
            if sid in processed_shows:
                continue
            processed_shows.add(sid)
            rating = item.get('rating')
            if rating:
                rating_count[rating] = rating_count.get(rating, 0) + 1
                
        top = sorted(rating_count.items(), key=lambda x: x[1], reverse=True)[:10]
        return jsonify([{"rating": k, "count": v} for k, v in top])
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/titulos', methods=['GET'])
def get_titulos():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    filters = build_filters(request.args)
    url = f"{DIRECTUS_URL}/items/netflix_titles?limit={limit}&offset={offset}&sort=-release_year"
    if filters:
        url += "&" + "&".join(filters)
    
    try:
        r = requests.get(url, headers=get_headers())
        return jsonify(r.json().get('data', []))
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/filter-options', methods=['GET'])
def get_filter_options():
    try:
        url_anos = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[groupBy]=release_year&limit=1000"
        r_anos = requests.get(url_anos, headers=get_headers())
        anos = [item['release_year'] for item in r_anos.json().get('data', []) if item.get('release_year')]
        anos = sorted(set(anos), reverse=True)
        
        url_paises = f"{DIRECTUS_URL}/items/netflix_titles?fields=country&limit=10000"
        r_paises = requests.get(url_paises, headers=get_headers())
        paises_raw = [item['country'] for item in r_paises.json().get('data', []) if item.get('country')]
        paises = set()
        for p in paises_raw:
            first = p.split(',')[0].strip()
            if first:
                paises.add(first)
        paises = sorted(paises)
        
        url_ratings = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[groupBy]=rating&limit=100"
        r_ratings = requests.get(url_ratings, headers=get_headers())
        ratings = [item['rating'] for item in r_ratings.json().get('data', []) if item.get('rating')]
        ratings = sorted(set(ratings))
        
        return jsonify({
            "anos": anos,
            "paises": paises,
            "ratings": ratings
        })
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
