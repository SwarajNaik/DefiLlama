import requests
import pandas as pd

headers = {"Content-Type": "application/json"}

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

if __name__=="__main__":
    result = call_data(url="https://yields.llama.fi/pools")
    df_main = clean_up(result['data'])
    df = pd.DataFrame(result['data'])