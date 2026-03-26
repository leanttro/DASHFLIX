FROM python:3.11-slim

# Instala dependências do sistema necessárias para o Pandas (opcional mas seguro)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia e instala as bibliotecas primeiro (melhora o cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto dos arquivos
COPY . .

EXPOSE 5001

# Comando para rodar
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "2", "--timeout", "120", "app:app"]
