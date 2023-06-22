from sqlalchemy import create_engine
import pandas as pd


data = {
    'Name': ['John', 'Emma', 'Michael', 'Sophia'],
    'Age': [25, 28, 32, 29],
    'City': ['New York', 'London', 'Paris', 'Sydney']
}

# Create the DataFrame
df = pd.DataFrame(data)

pg_user = "Defillama"
pg_pass = "Qaws!234"
pg_host = "localhost"
pg_port = 5432
pg_db = "Defillama"

engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
conn = engine.connect()


df.to_sql('df', con=conn, if_exists= 'replace')