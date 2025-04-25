import os
import io
import pandas as pd
from fpgrowth_py import fpgrowth
import clickhouse_connect as cc
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import logging
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dotenv import load_dotenv

load_dotenv()

file_handler = logging.FileHandler('/var/log/app.log', 'a')
file_handler.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler]
)

# Definir conexões globais para reutilização
global_s3_client = boto3.client('s3')
global_db_client = None

def get_db_client():
    global global_db_client
    if global_db_client is None:
        global_db_client = cc.get_client(host=os.getenv("HOST"), port=os.getenv("PORT"),
                                         username=os.getenv("USERNAME"), password=os.getenv("PASSWORD"))
    return global_db_client

def query_db(client):
    logging.info(f"Iniciando consulta no banco de dados para o cliente: {client}")
    try:
        db_client = get_db_client()
        query = f'SELECT * FROM bucket_68.tabela_pedidos_{client}'
        data = db_client.query_df(query)
        logging.info(f"Dados do cliente {client} carregados com sucesso")
        return data
    except Exception as e:
        logging.error(f"Falha na consulta ao banco de dados do cliente {client}: {e}")
        raise

def pivot_data(client, df):
    logging.info(f"Iniciando a criação da tabela pivotada ({client}).")
    try:
        df_pivot = df.pivot_table(index='external_order_reference',
                                  columns=df.groupby('external_order_reference').cumcount(),
                                  values='product', aggfunc='first')
        df_pivot.columns = [f'item_{col}' for col in df_pivot.columns]
        df_pivot.fillna('0', inplace=True)
        logging.info(f"Tabela pivotada criada com sucesso ({client}).")
        return df_pivot.reset_index(drop=True), df
    except Exception as e:
        logging.error(f"Erro ao criar a tabela pivotada ({client}): {e}")
        raise

def preprocess_data(client, pivot_df):
    logging.info(f"Iniciando pré-processamento dos dados ({client}).")
    try:
        transactions = pivot_df.apply(lambda row: list(filter(lambda x: x != '0', row)), axis=1).tolist()
        logging.info(f"Pré-processamento concluído com sucesso ({client}).")
        return transactions
    except Exception as e:
        logging.error(f"Erro no pré-processamento dos dados ({client}): {e}")
        raise


def generate_association_rules(client, transactions, min_support=0.05, min_confidence=0.6):
    logging.info(f"Iniciando geração de regras de associação ({client}).")
    try:
        freqItemSet, rules = fpgrowth(transactions, minSupRatio=min_support, minConf=min_confidence)
        logging.info(f"Regras de associação geradas com sucesso ({client}).")
        rules_df = pd.DataFrame(rules, columns=['antecendents', 'consequents', 'confidence']).sort_values(by='confidence', ascending=False).reset_index()
        return rules_df
    except Exception as e:
        logging.error(f"Erro ao gerar regras de associação ({client}): {e}")
        raise

def upload_to_s3(client, df, bucket_name, s3_file_name):
    logging.info(f"Iniciando upload do arquivo para o S3 ({client}): {s3_file_name}")
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        global_s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=csv_buffer.getvalue())
        logging.info(f"Upload Successful ({client}): {s3_file_name}")
    except FileNotFoundError:
        logging.error(f"O arquivo não foi encontrado ({client}).")
    except NoCredentialsError:
        logging.error(f"Credenciais não disponíveis ({client}).")
    except PartialCredentialsError:
        logging.error(f"Credenciais incompletas fornecidas ({client}).")
    except Exception as e:
        logging.error(f"Erro ao fazer upload para o S3 ({client}): {e}")
        raise

def make_directories(client, name_dir, df, bucket_name):
    s3_file_name = f'{name_dir}/{name_dir}_{client}.csv'
    upload_to_s3(client, df, bucket_name, s3_file_name)
    return None

def process_client(client):
    logging.info(f"Processando cliente: {client}")
    try:
        data = query_db(client)
        pivot_df, pedidos_df = pivot_data(client, data)
        transactions = preprocess_data(client, pivot_df)
        rules = generate_association_rules(client, transactions)
        make_directories(client, 'rules', rules, os.getenv("BUCKET_NAME"))
        make_directories(client, 'pedidos', pedidos_df, os.getenv("BUCKET_NAME"))
    except Exception as e:
        logging.error(f"Erro ao processar o cliente {client}: {e}")

def main():
    logging.info("Início do processo de ingestão e treinamento.")
    clients = os.getenv("LIST_CLIENTS").split(',')
    with ProcessPoolExecutor(max_workers=6) as executor:
        executor.map(process_client, clients)
    logging.info("Processo de ingestão e treinamento concluído.")

if __name__ == "__main__":
    main()
