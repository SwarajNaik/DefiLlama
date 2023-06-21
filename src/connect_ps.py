import psycopg2
import pandas as pd



conn1 = psycopg2.connect(
    database="test_db",
  user='root', 
  password='root', 
  host='172.21.0.3', 
  port= '5050'
)
  
conn1.autocommit = True
cursor = conn1.cursor()
  
# drop table if it already exists
cursor.execute('drop table if exists airlines_final')
  
sql = '''CREATE TABLE airlines_final(id int ,
day int ,airline char(20),destination char(20));'''

sql1='''select * from airlines_final;'''
cursor.execute(sql1)
for i in cursor.fetchall():
    print(i)
  
conn1.commit()
conn1.close()
  
# cursor.execute(sql)