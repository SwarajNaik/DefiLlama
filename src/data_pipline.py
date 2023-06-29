import os
import yaml
import requests
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

headers = {"Content-Type": "application/json"}

def call_data(url, headers):
    response = requests.get(url=url, headers=headers)
    return response.json()

def clean_up_pools(json):
    selected_values = {
        "chain":[item["chain"] for item in json],
        "project":[item["project"] for item in json],
        "symbol":[item["symbol"] for item in json],
        "pool":[item["pool"] for item in json],
        "stablecoin":[item["stablecoin"] for item in json],
        "count":[item["count"] for item in json],
        "volumeUsd7d":[item["volumeUsd7d"] for item in json]
    }
    df = pd.DataFrame(selected_values)
    df = df[df["project"]== "uniswap-v3"]
    df = df.reset_index(drop=True)
    return df

def get_pool_id(data_of_list_of_pools):
    df = data_of_list_of_pools[data_of_list_of_pools["symbol"] == "USDC-WETH"]
    df = df[df["chain"]== "Ethereum"]
    first_row = df.head(1)
    pool_id = first_row['pool']
    
    return str(pool_id.iloc[0])

def get_pools_data(pool_id, headers):
    response = requests.get(f"https://yields.llama.fi/chart/{pool_id}")
    return response.json()

def get_the_configs():
    
    current_dir = Path.cwd()
    parent_directory = current_dir.parent
    os.chdir(parent_directory)
    
    with open('docker-compose.yaml') as f:
        config = yaml.safe_load(f)
    return config


def push_data_to_ps(data, name_of_table, configs):
    pg = configs['services']['db']['environment']
    
    pg_user = pg['POSTGRES_USER']
    pg_pass = pg['POSTGRES_PASSWORD']
    pg_db = pg['POSTGRES_DB']
    pg_port = configs['services']['db']['ports'][0]
    pg_port = pg_port[:4]
    
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@localhost:{pg_port}/{pg_db}")
    conn = engine.connect()
    
    data.to_sql(f"{name_of_table}", con=conn, if_exists= 'replace')
    pass

if __name__=="__main__":
    result = call_data(url="https://yields.llama.fi/pools", headers=headers)
    df_main = clean_up_pools(result['data'])
    
    configs = get_the_configs()
    push_data_to_ps(data=df_main, name_of_table="pools_defillama", configs=configs)
    
    id = get_pool_id(data_of_list_of_pools=df_main)
    pool_df = get_pools_data(pool_id=id, headers=headers)
    df = pd.DataFrame(pool_df['data'])
    # df = clean_up_pool_data(data=pool_df['data'])
    push_data_to_ps(data=df, name_of_table="historic_pools_data", configs=configs)
    
    # configs = get_the_configs()
