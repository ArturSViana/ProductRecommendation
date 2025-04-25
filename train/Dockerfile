# Usar uma imagem base oficial do Python 3.10
FROM python:3.10-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar os requisitos e instalar as dependências
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código da aplicação para o diretório de trabalho
COPY . .

COPY .env /app/.env

# Definir o comando de entrada para rodar o script recommendation_train.py
CMD ["python3", "recommendation-train.py"]
