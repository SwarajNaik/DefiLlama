import psycopg2
from sqlalchemy import create_engine
import pandas as pd
# from data_fecting import df_main

# Connect to existing database
conn = psycopg2.connect(
    database="defillama",
    user="defillama",
    password="defillama",
    host="0.0.0.0"
)

cur = conn.cursor()

data = {
    'Name': ['John', 'Emma', 'Michael', 'Sophia'],
    'Age': [25, 28, 32, 29],
    'City': ['New York', 'London', 'Paris', 'Sydney']
}

# Create the DataFrame
df = pd.DataFrame(data)

df.to_sql('df', conn, if_exists= 'replace')


sql1='''select * from name limit 10;'''
cur.execute(sql1)
for i in cur.fetchall():
    print(i)
  
conn.commit()
conn.close()
# Open cursor to perform database operation
cur = conn.cursor()

# Query the database 
cur.execute("SELECT * FROM test")
rows = cur.fetchall()

# df = df_main

# df.to_sql('pools', conn, if_exists= 'replace')

# sql1='''select * from airlines_final limit 100;'''
# cur.execute(sql1)
# for i in cur.fetchall():
# #     print(i)

# conn.commit()
# cur.close()
# conn.close()