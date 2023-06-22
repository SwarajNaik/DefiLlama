import requests
import pandas as pd
import psycopg2

headers = {"Content-Type": "application/json"}

def conn():
    conn = psycopg2.connect(
    database="defillama",
    user="defillama",
    password="defillama",
    host="0.0.0.0"
    )
    return conn

def call_data(url, headers):
    response = requests.get(url=url, headers=headers)
    return response.json()

def clean_up(json):
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
    return df


def get_pools_data(pool_id, headers):
    response = requests.get(f"https://yields.llama.fi/chart/{pool_id}")
    return response.json()

def push_data_to_ps(conn, df):
    
    cur = conn.cursor()
    
    df.to_sql('pools', conn, if_exists= 'replace')
    sql1='''select * from pools limit 100;'''
    cur.execute(sql1)
    for i in cur.fetchall():
        print(i)

    conn.commit()
    cur.close()
    conn.close()
    
if __name__=="__main__":
    result = call_data(url="https://yields.llama.fi/pools", headers=headers)
    df_main = clean_up(result['data'])
    push_data_to_ps(conn=conn(), df=df_main)
    # df = pd.DataFrame(result['data'])