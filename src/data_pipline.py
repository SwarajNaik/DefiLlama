import logging
import os
from pathlib import Path

import pandas as pd
import requests
import yaml
from sqlalchemy import create_engine

logging.basicConfig(
    filename="data_activites.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)


headers = {"Content-Type": "application/json"}


def call_data(url, headers):
    response = requests.get(url=url, headers=headers)
    return response.json()


def clean_up_pools(json):
    selected_values = {
        "chain": [item["chain"] for item in json],
        "project": [item["project"] for item in json],
        "symbol": [item["symbol"] for item in json],
        "pool": [item["pool"] for item in json],
        "stablecoin": [item["stablecoin"] for item in json],
        "count": [item["count"] for item in json],
        "volumeUsd7d": [item["volumeUsd7d"] for item in json],
    }
    df = pd.DataFrame(selected_values)
    df = df[df["project"] == "uniswap-v3"]
    df = df.reset_index(drop=True)
    return df


def get_pool_id(pools_data):
    pools_data = pools_data[pools_data["chain"] == "Ethereum"]
    pools_data = pools_data.sort_values("volumeUsd7d", ascending=False)
    pools_df_50 = pools_data.head(50)
    pools_list = pools_df_50[["symbol", "pool"]]
    return pools_list


def get_pools_data(list_of_pools):
    main_df = pd.DataFrame()
    for pool_id in list_of_pools["pool"].tolist():
        response = requests.get(f"https://yields.llama.fi/chart/{pool_id}")
        json = response.json()
        df = pd.DataFrame(json["data"])
        selected_row = list_of_pools[list_of_pools["pool"] == pool_id]
        symbol = selected_row["symbol"]
        df["symbol"] = symbol.to_string(index=False)
        main_df = main_df.append(df, ignore_index=True)
    return main_df


def get_the_configs():
    current_dir = Path.cwd()
    parent_directory = current_dir.parent
    os.chdir(parent_directory)
    with open("docker-compose.yaml") as f:
        config = yaml.safe_load(f)
    return config


def push_data_to_ps(data, name_of_table, configs):
    pg = configs["services"]["db"]["environment"]
    pg_user = pg["POSTGRES_USER"]
    pg_pass = pg["POSTGRES_PASSWORD"]
    pg_db = pg["POSTGRES_DB"]
    pg_port = configs["services"]["db"]["ports"][0]
    pg_port = pg_port[:4]
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@localhost:{pg_port}/{pg_db}"
    )
    conn = engine.connect()
    logging.info(f"Connection successful with this configs :- {pg}")
    data.to_sql(f"{name_of_table}", con=conn, if_exists="replace")
    logging.info(f"Successfully pushed '{name_of_table}' on the {pg_db} db")
    pass


if __name__ == "__main__":
    result = call_data(url="https://yields.llama.fi/pools", headers=headers)
    df_main = clean_up_pools(result["data"])

    configs = get_the_configs()
    push_data_to_ps(data=df_main, name_of_table="pools_defillama", configs=configs)

    list_of_id = get_pool_id(pools_data=df_main)
    df = get_pools_data(list_of_pools=list_of_id)
    push_data_to_ps(data=df, name_of_table="Historical_50pools", configs=configs)
