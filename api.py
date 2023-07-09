from pathlib import Path
import os
import yaml
import pandas as pd
from sqlalchemy import create_engine


async def _get_the_configs():
    # current_dir = Path.cwd()
    # parent_directory = current_dir.parent
    # os.chdir(parent_directory)
    with open("docker-compose.yaml") as f:
        configs = yaml.safe_load(f)
    return configs

async def _pull_data_from_ps(configs, name_of_table, limit):
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
    query = f"SELECT * FROM {name_of_table} LIMIT {limit}"
    df = pd.read_sql_query(sql=query, con=conn)
        
    return df

async def call_data(name_of_table, limit):
    configs = await _get_the_configs()
    df = await _pull_data_from_ps(configs=configs, name_of_table=name_of_table, limit=limit)
    return df.to_dict('index')


if __name__=="__main__":
    data = call_data("pools_defillama")
     

