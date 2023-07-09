import asyncio
import logging
import os
from pathlib import Path

import pandas as pd
import requests
import yaml
from sqlalchemy import create_engine


class DefiLlama:
    def __init__(self):
        self.headers = {"Content-Type": "application/json"}
        self.configs = None
        self.loop = asyncio.get_event_loop()

    async def _get_the_configs(self):
        current_dir = Path.cwd()
        parent_directory = current_dir.parent
        os.chdir(parent_directory)
        with open("docker-compose.yaml") as f:
            self.configs = yaml.safe_load(f)

    async def _call_data(self, url):
        response = requests.get(url=url, headers=self.headers)
        return response.json()

    async def _clean_up_pools(self, json):
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

    async def _get_pool_id(self, pools_data):
        pools_data = pools_data[pools_data["chain"] == "Ethereum"]
        pools_data = pools_data.sort_values("volumeUsd7d", ascending=False)
        pools_df_50 = pools_data.head(50)
        pools_list = pools_df_50[["symbol", "pool"]]
        return pools_list

    async def _get_pools_data(self, list_of_pools):
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

    async def _push_data_to_ps(self, data, name_of_table):
        pg = self.configs["services"]["db"]["environment"]
        pg_user = pg["POSTGRES_USER"]
        pg_pass = pg["POSTGRES_PASSWORD"]
        pg_db = pg["POSTGRES_DB"]
        pg_port = self.configs["services"]["db"]["ports"][0]
        pg_port = pg_port[:4]
        engine = create_engine(
            f"postgresql://{pg_user}:{pg_pass}@localhost:{pg_port}/{pg_db}"
        )
        conn = engine.connect()
        logging.info(f"Connection successful with these configs: {pg}")
        data.to_sql(f"{name_of_table}", con=conn, if_exists="replace")
        logging.info(f"Successfully pushed '{name_of_table}' to the '{pg_db}' db")

        return "data_pushed"
    
    async def _pull_data_from_ps(self, name_of_table):
        pg = self.configs["services"]["db"]["environment"]
        pg_user = pg["POSTGRES_USER"]
        pg_pass = pg["POSTGRES_PASSWORD"]
        pg_db = pg["POSTGRES_DB"]
        pg_port = self.configs["services"]["db"]["ports"][0]
        pg_port = pg_port[:4]
        engine = create_engine(
            f"postgresql://{pg_user}:{pg_pass}@localhost:{pg_port}/{pg_db}"
        )
        conn = engine.connect()
        query = f"SELECT * FROM {name_of_table} LIMIT 10"
        df = pd.read_sql_query(sql=query, con=conn)
        
        return df
        

    async def process_data(self):
        await self._get_the_configs()

        result = await self._call_data(url="https://yields.llama.fi/pools")
        df_main = await self._clean_up_pools(result["data"])
        await self._push_data_to_ps(data=df_main, name_of_table="pools_defillama")

        list_of_id = await self._get_pool_id(pools_data=df_main)
        df = await self._get_pools_data(list_of_pools=list_of_id)
        await self._push_data_to_ps(data=df, name_of_table="historical_50pools")

        return "Done"

    async def main(self):
        await asyncio.gather(self.process_data())


if __name__ == "__main__":
    logging.basicConfig(
        filename="data_activities.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )

    processor = DefiLlama()
    loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(processor.main(), loop=loop)
