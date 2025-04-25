import os
import io
import boto3
import pandas as pd
from itertools import permutations
import json
from flask import Flask, request, jsonify
import logging
import ast
from dotenv import load_dotenv
import clickhouse_connect as cc
from functools import wraps

load_dotenv()


file_handler = logging.FileHandler('/var/log/app.log', 'a')
file_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def load_client_tokens():
    tokens = {}
    for key, value in os.environ.items():
        if key.startswith('CLIENT_TOKEN_'):
            client = key[len('CLIENT_TOKEN_'):]
            tokens[client] = value
    return tokens

CLIENT_TOKENS = load_client_tokens()

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        mnemonic = request.headers.get('Client')
        client = provider_checker(mnemonic).upper()
        token = request.headers.get('Authorization')

        if not client or not token:
            return jsonify({'message': 'Client and Token are required'}), 401

        token = token.replace('Bearer ', '', 1)

        if client not in CLIENT_TOKENS or CLIENT_TOKENS[client] != token:
            return jsonify({'message': 'Invalid client or token'}), 403

        return f(*args, **kwargs)
    return decorated

def provider_checker(mnemonic):
    provider_dict = {'cardealevoce': 'cardeal', 'meuminervawhats': 'minerva'}
    if mnemonic != 'cardealevoce' and mnemonic != 'meuminervawhats':
        return mnemonic
    provider_selected = [provider_dict[provider] for provider in provider_dict if mnemonic == provider]
    return provider_selected[0]

def get_buyers(client, host, port, user, password):
    logging.info(f"Iniciando consulta no banco de dados para o cliente: {client}")
    try:
        user_db = cc.get_client(host=host, port=port, username=user, password=password)
        logging.info(f"Dados do cliente {client} carregados com sucesso")
        return user_db.query_df(f'SELECT * FROM bucket_68.tabela_info_buyers_{client}')
    except Exception as e:
        logging.error(f"Falha na consulta ao banco de dados do cliente {client}: {e}")
        raise

def str_to_frozenset(s):
    """
    função criada como auxiliar no carregamento dos arquivos RULES pois ao ser carregada, ela sofre alteração
    no tipo das colunas 'antecedents' e 'consequents' que são frozenset para str. O formato original é necessário para o bom funcionamento
    do script
    """
    return frozenset(ast.literal_eval(s))

def load_data(pedidos_bucket, rules_bucket, client):
    """
    Carrega os dados dos arquivos CSV dos buckets S3 e retorna DataFrames pandas.
    """
    logger.info(f"Loading data for client: {client}")
    s3 = boto3.client('s3')
    file_key_pedidos = f'pedidos/pedidos_{client}.csv'
    file_key_rules = f'rules/rules_{client}.csv'
    try:
        pedidos_obj = s3.get_object(Bucket=pedidos_bucket, Key=file_key_pedidos)
        rules_obj = s3.get_object(Bucket=rules_bucket, Key=file_key_rules)

        pedidos_df = pd.read_csv(io.BytesIO(pedidos_obj['Body'].read()), dtype=str)
        rules_df = pd.read_csv(io.BytesIO(rules_obj['Body'].read()))

        logger.info("Data loaded successfully")
        return pedidos_df, rules_df
    except Exception as e:
        logger.error(f"An error occurred while loading data: {e}")
        return None, None

def get_top_products_by_buyer(pedidos_df, buyer, n=5):
    """
    Retorna os N produtos mais comprados pelo comprador especificado.
    """
    logger.info(f"Getting top products for buyer: {buyer}")
    pedidos_buyer_df = pedidos_df[pedidos_df['buyer'] == buyer]
    top_products_buyer = pedidos_buyer_df['product'].value_counts().head(n).reset_index()
    top_products_buyer.columns = ['product', 'frequency']
    return top_products_buyer

def generate_combinations(products):
    """
    Gera todas as combinações possíveis de produtos.
    """
    combinations = []
    lista_sets = []
    for r in range(1, len(products) + 1):
        combinations.extend(list(permutations(products, r)))
    comb_produtos = [list(item) for item in combinations]
    for prod in comb_produtos:
        produto_set = set(prod)
        produto_str = str(produto_set)
        lista_sets.append(produto_str)
    return lista_sets

def filter_rules_by_products(rules, products):
    """
    Filtra as regras de associação para incluir apenas aquelas relacionadas aos produtos fornecidos.
    """
    selected_rules = rules[rules['antecendents'].isin(products)]
    recommended_products = selected_rules['consequents'].str.replace(r"[{}']", "", regex=True).str.split(', ').explode().unique()
    return pd.DataFrame({'product': recommended_products})

def main(client, buyer, pedidos_bucket, rules_bucket, buyers):
    logger.info("Starting recommendation process")
    pedidos_df, rules = load_data(pedidos_bucket, rules_bucket, client)
    if pedidos_df is None or rules is None:
        return jsonify({'error': 'Failed to load data'}), 500

    top_products_buyer = get_top_products_by_buyer(pedidos_df, buyer)
    product_combinations = generate_combinations(top_products_buyer['product'].tolist())
    recommended_products = filter_rules_by_products(rules, product_combinations)
    logging.info(f"recommend_products 0: {recommended_products}")

    recommended_products['buyer'] = buyer
    logging.info(f"recommend_products 1: {recommended_products}")
    recommended_products = recommended_products.merge(buyers, how='inner', on='buyer').drop_duplicates()
    logging.info(f"recommend_products 2: {recommended_products}")
    supplier_df = pedidos_df[['product', 'supplier']]
    logging.info(f"supplier: {supplier_df}")
    recommended_products = recommended_products.merge(supplier_df, how='inner', on='product').drop_duplicates()
    logging.info(f"recommend_products 3: {recommended_products}")
    output = recommended_products[['buyer', 'seller', 'product', 'supplier']].to_json(orient='records')
    logging.info(f"output: {output}")

    return output


@app.route('/recommendation/', methods=['GET'])
@token_required
def recommendation():
    mnemonic = request.headers.get("Client")
    buyer = request.args.getlist("buyer")  # Obtém o valor de 'buyer' dos parâmetros da URL
    seller = request.args.get("seller")  # Obtém o valor de 'seller' dos parâmetros da URL
    pedidos_bucket = os.getenv('PEDIDOS_BUCKET')
    host_cc = os.getenv('HOST_CC')
    port_cc = os.getenv('PORT_CC')
    user_cc = os.getenv('USER_CC')
    password_cc = os.getenv('PASSWORD_CC')
    rules_bucket = os.getenv('RULES_BUCKET')

    client = provider_checker(mnemonic)
    buyers = get_buyers(client, host_cc, port_cc, user_cc, password_cc)

    if not buyer:
        results = []
        buyer_list = buyers["buyer"].tolist()
        buyer_list = buyer_list[0:10]
        for buyer in buyer_list:
            logging.info(f"Processing buyer: {buyer}")
            logger.info(f"Received recommendation request for client: {client}, buyer: {buyer}, seller: {seller}")
            result = main(client, buyer, pedidos_bucket, rules_bucket, buyers)
            if isinstance(result, str):
                results.extend(json.loads(result))
        return jsonify(results)

    else:
        if len(buyer) == 1:
            logger.info(f"Received recommendation request for client: {client}, buyer: {buyer}, seller: {seller}")
            output = main(client, buyer[0], pedidos_bucket, rules_bucket, buyers)
            return jsonify(json.loads(output))
        elif len(buyer) > 1:
            results = []
            for byer in buyer:
                logging.info(f"Processing buyer: {byer}")
                logger.info(f"Received recommendation request for client: {client}, buyer: {byer}, seller: {seller}")
                result = main(client, byer, pedidos_bucket, rules_bucket, buyers)
                if isinstance(result, str):
                    results.extend(json.loads(result))
            return jsonify(results)

if __name__ == "__main__":
    logger.info("Starting Flask application")
    app.run(host='0.0.0.0', port=5000, debug=True)
