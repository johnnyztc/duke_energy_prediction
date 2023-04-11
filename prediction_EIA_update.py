import pandas as pd
import numpy as np
import requests
import pymysql
import xgboost as xgb
import sklearn
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String, Boolean, Numeric,DateTime
from datetime import date
from datetime import timedelta
from datetime import datetime
from decimal import Decimal

engine = create_engine('mysql+pymysql://admin:ztc971110@duke-test.c6gn8p6i9qvw.us-east-2.rds.amazonaws.com:3306/duke',echo = False)
metadata_obj = MetaData()
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()
class Prediction_EIA(Base):
    __tablename__ = 'prediction_EIA'
    Datetime = Column(DateTime(),primary_key=True)
    prediction_EIA = Column(Numeric(12,2))
db = pd.read_sql_query('SELECT * FROM duke.prediction_EIA',engine)
db = db.tail(200)
db.set_index('Datetime',inplace = True)

start_date = db.index[-36].date()
end_date = date.today() + timedelta(days=1)

t = start_date
t0 = str(t)
t1 = str((t + timedelta(days=1)))
url = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=local-hourly&data[0]=value&facets[respondent][]=DUK&start='+t0+'&end='+t1+'&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key=phRLIs3z4GNYWKHtB5d2zunfICyqTsUSpnRJvq2S'
r = requests.get(url)
d = r.json()
df = pd.json_normalize(d, record_path=['response', 'data'])
df0 = df[df['type-name'] == 'Day-ahead demand forecast']
df0 = df0.reset_index()
df0.drop('index',axis=1,inplace = True)
df0['Datetime'] = df0['period']
for i in range(len(df0)):
    dt=datetime.strptime(df0['period'][i][:-3], "%Y-%m-%dT%H")
    df0['Datetime'][i] = dt
    i = i+1
df0 = df0.set_index('Datetime')
df0 = df0[['value']]
df0 = df0.rename(columns={'value':'prediction_EIA'})

for i in range((end_date - start_date).days):
    t = start_date + timedelta(days=i)
    t0 = str(t)
    t1 = str(t + timedelta(days=1))
    url = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=local-hourly&data[0]=value&facets[respondent][]=DUK&start='+t0+'&end='+t1+'&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key=phRLIs3z4GNYWKHtB5d2zunfICyqTsUSpnRJvq2S'
    r = requests.get(url)
    d = r.json()
    df = pd.json_normalize(d, record_path=['response', 'data'])
    df3 = df[df['type-name'] == 'Day-ahead demand forecast']
    df3 = df3.reset_index()
    df3.drop('index',axis=1,inplace = True)
    df3['Datetime'] = df3['period']
    for i in range(len(df3)):
        dt=datetime.strptime(df3['period'][i][:-3], "%Y-%m-%dT%H")
        df3['Datetime'][i] = dt
        i = i+1
    df3 = df3.set_index('Datetime')
    df3 = df3[['value']]
    df3 = df3.rename(columns={'value':'prediction_EIA'})
    df3 = df3[df3.index > df0.index[-1]]
    df0 = pd.concat([df0,df3],axis = 0)
    
if df0.index[-1] <= db.index[-1]:
    print('There is nothing to update')
else:
    df = df0[df0.index > db.index[-1]]
    df = df.reset_index()

    df['Datetime'] = df['Datetime'].astype(str)
    df.iloc[:,1:] = df.iloc[:,1:].applymap(lambda x: round(Decimal(x),2))
    df.fillna(0.00, inplace=True)
    print(df)
    for i in range(df.shape[0]):
        appendix = df.iloc[i, 0].replace(' ','-')
        item_name = f'item_{appendix}'
        a0 = df.iloc[i,0]
        a1 = df.iloc[i,1]
        item_name = Prediction_EIA(Datetime=a0, prediction_EIA=a1)
        session.add(item_name)

    session.commit()
    session.close()