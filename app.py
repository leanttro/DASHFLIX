from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import requests
import os
import threading
from dotenv import load_dotenv
from etl_netflix import executar_carga_kaggle

load_dotenv()

app = Flask(__name__)

# CORS liberado
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
    """
    Retorna total de títulos, filmes e séries baseado em show_id único.
    """
    try:
        ano = request.args.get('ano')
        pais = request.args.get('pais')
        rating = request.args.get('rating')
        tipo = request.args.get('tipo')
        
        # Busca todos os registros com show_id e type
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id,type&limit=10000"
        
        filtros = []
        if ano: filtros.append(f"filter[release_year][_eq]={ano}")
        if pais: filtros.append(f"filter[country][_contains]={pais}")
        if rating: filtros.append(f"filter[rating][_eq]={rating}")
        if tipo: filtros.append(f"filter[type][_eq]={tipo}")
        
        if filtros:
            url += "&" + "&".join(filtros)
            
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
    """
    Retorna top 10 países com base nos títulos únicos.
    Parâmetros opcionais: ano (release_year), pais (filtro por país)
    """
    try:
        ano = request.args.get('ano')
        pais_filtro = request.args.get('pais')
        rating = request.args.get('rating')
        tipo = request.args.get('tipo')
        
        # Construir query para Directus
        fields = "show_id,country"
        limit = 10000
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields={fields}&limit={limit}"
        
        filtros = []
        if ano: filtros.append(f"filter[release_year][_eq]={ano}")
        if pais_filtro: filtros.append(f"filter[country][_contains]={pais_filtro}")
        if rating: filtros.append(f"filter[rating][_eq]={rating}")
        if tipo: filtros.append(f"filter[type][_eq]={tipo}")
        
        if filtros:
            url += "&" + "&".join(filtros)
            
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            return jsonify({"erro": "Erro ao buscar dados"}), 500
        
        data = r.json().get('data', [])
        
        # Dicionário para contar países (por show_id único)
        pais_count = {}
        processed_shows = set()
        for item in data:
            sid = item.get('show_id')
            if sid in processed_shows:
                continue
            processed_shows.add(sid)
            pais = item.get('country')
            if pais:
                # Pega o primeiro país (separado por vírgula)
                primeiro = pais.split(',')[0].strip()
                pais_count[primeiro] = pais_count.get(primeiro, 0) + 1
        
        # Ordenar e pegar top
        top = sorted(pais_count.items(), key=lambda x: x[1], reverse=True)[:20]
        return jsonify([{"pais": k, "total": v} for k, v in top])
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/lancamentos-ano', methods=['GET'])
def get_lancamentos_ano():
    """
    Retorna contagem de lançamentos por ano (baseado em show_id único).
    Parâmetros opcionais: ano (filtro por ano), pais (filtro por país)
    """
    try:
        ano_filtro = request.args.get('ano')
        pais_filtro = request.args.get('pais')
        rating = request.args.get('rating')
        tipo = request.args.get('tipo')
        
        fields = "show_id,release_year,country"
        limit = 10000
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields={fields}&limit={limit}"
        
        filtros = []
        if ano_filtro: filtros.append(f"filter[release_year][_eq]={ano_filtro}")
        if pais_filtro: filtros.append(f"filter[country][_contains]={pais_filtro}")
        if rating: filtros.append(f"filter[rating][_eq]={rating}")
        if tipo: filtros.append(f"filter[type][_eq]={tipo}")
        
        if filtros:
            url += "&" + "&".join(filtros)
            
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            return jsonify({"erro": "Erro ao buscar dados"}), 500
        
        data = r.json().get('data', [])
        
        # Contagem por ano baseada em show_id único e filtro de país
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
        
        # Ordenar por ano
        sorted_years = sorted(year_count.items())
        return jsonify([{"release_year": y, "count": c} for y, c in sorted_years])
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/api/classificacao', methods=['GET'])
def get_classificacao():
    """
    Retorna a contagem de titulos por classificacao indicativa
    """
    try:
        ano = request.args.get('ano')
        pais = request.args.get('pais')
        rating_filtro = request.args.get('rating')
        tipo = request.args.get('tipo')
        
        url = f"{DIRECTUS_URL}/items/netflix_titles?fields=show_id,rating&limit=10000"
        
        filtros = []
        if ano: filtros.append(f"filter[release_year][_eq]={ano}")
        if pais: filtros.append(f"filter[country][_contains]={pais}")
        if rating_filtro: filtros.append(f"filter[rating][_eq]={rating_filtro}")
        if tipo: filtros.append(f"filter[type][_eq]={tipo}")
        
        if filtros:
            url += "&" + "&".join(filtros)
            
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
    """
    Retorna lista de títulos com paginação e filtros.
    Parâmetros: limit (padrão 100), offset, ano, pais, rating, categoria.
    """
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    ano = request.args.get('ano')
    pais = request.args.get('pais')
    rating = request.args.get('rating')
    tipo = request.args.get('tipo')
    categoria = request.args.get('categoria')
    
    # Construir filtros do Directus
    filters = []
    if ano:
        filters.append(f"filter[release_year][_eq]={ano}")
    if rating:
        filters.append(f"filter[rating][_eq]={rating}")
    if tipo:
        filters.append(f"filter[type][_eq]={tipo}")
    if categoria:
        filters.append(f"filter[listed_in][_contains]={categoria}")
    if pais:
        # Para país, precisamos verificar se o campo country contém o país (como substring)
        filters.append(f"filter[country][_contains]={pais}")
    
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
    """
    Retorna listas de valores únicos para os filtros: anos, países, ratings, categorias.
    """
    try:
        # Anos únicos
        url_anos = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[groupBy]=release_year&limit=1000"
        r_anos = requests.get(url_anos, headers=get_headers())
        anos = [item['release_year'] for item in r_anos.json().get('data', []) if item.get('release_year')]
        anos = sorted(set(anos), reverse=True)
        
        # Países únicos (primeiro país de cada)
        url_paises = f"{DIRECTUS_URL}/items/netflix_titles?fields=country&limit=10000"
        r_paises = requests.get(url_paises, headers=get_headers())
        paises_raw = [item['country'] for item in r_paises.json().get('data', []) if item.get('country')]
        paises = set()
        for p in paises_raw:
            first = p.split(',')[0].strip()
            if first:
                paises.add(first)
        paises = sorted(paises)
        
        # Ratings únicos
        url_ratings = f"{DIRECTUS_URL}/items/netflix_titles?aggregate[groupBy]=rating&limit=100"
        r_ratings = requests.get(url_ratings, headers=get_headers())
        ratings = [item['rating'] for item in r_ratings.json().get('data', []) if item.get('rating')]
        ratings = sorted(set(ratings))
        
        # Categorias (listed_in) - pegar todas e separar por vírgula
        url_cats = f"{DIRECTUS_URL}/items/netflix_titles?fields=listed_in&limit=10000"
        r_cats = requests.get(url_cats, headers=get_headers())
        cats_raw = [item['listed_in'] for item in r_cats.json().get('data', []) if item.get('listed_in')]
        categories = set()
        for cat in cats_raw:
            for c in cat.split(','):
                c = c.strip()
                if c:
                    categories.add(c)
        categories = sorted(categories)
        
        return jsonify({
            "anos": anos,
            "paises": paises,
            "ratings": ratings,
            "categorias": categories
        })
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
