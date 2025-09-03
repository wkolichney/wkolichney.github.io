import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:root@localhost:3306/finance?charset=utf8mb4")

df = pd.read_excel('C:/Users/wikku/personal_finance/bank.xlsx')

df.to_sql("accounts", engine, if_exists="append", index=False, chunksize=1000, method='multi')