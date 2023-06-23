import requests
import pandas as pd
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
        # "symbol":[item["symbol"] for item in json],
    }
    df = pd.DataFrame(selected_values)
    df = df[df["project"]== "uniswap-v3"]
    df = df.reset_index(drop=True)
    return df

def clean_up_pool_data(data):
    return pd.DataFrame(data)


def get_pool_id(data_of_list_of_pools):
    df = data_of_list_of_pools[data_of_list_of_pools["symbol"] == "USDC-WETH"]
    df = df[df["chain"]== "Ethereum"]
    first_row = df.head(1)
    pool_id = first_row['pool']
    
    
    return str(pool_id.iloc[0])

def get_pools_data(pool_id, headers):
    response = requests.get(f"https://yields.llama.fi/chart/{pool_id}")
    return response.json()


def push_data_to_ps(data, name_of_table):
    pg_user = "Defillama"
    pg_pass = "Qaws!234"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "Defillama"
    
    engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
    conn = engine.connect()
    
    data.to_sql(f"{name_of_table}", con=conn, if_exists= 'replace')
    pass
    # return "data pushed"
    

if __name__=="__main__":
    result = call_data(url="https://yields.llama.fi/pools", headers=headers)
    df_main = clean_up_pools(result['data'])
    push_data_to_ps(data=df_main, name_of_table="pools_defillama")
    
    id = get_pool_id(data_of_list_of_pools=df_main)
    pool_df = get_pools_data(pool_id=id, headers=headers)
    
    df = clean_up_pool_data(data=pool_df['data'])
    push_data_to_ps(data=df, name_of_table="USDC/WETH_pool")