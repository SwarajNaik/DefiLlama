import requests
import pandas as pd


def call_data(url):
    headers = {
        "Content-Type": "application/json"
    }
    
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

if __name__=="__main__":
    result = call_data(url="https://yields.llama.fi/pools")
    df_main = clean_up(result['data'])
    df = pd.DataFrame(result['data'])