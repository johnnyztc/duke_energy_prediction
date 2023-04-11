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

class Weather_Update(Base):
    __tablename__ = 'weather_forecast'
    Datetime = Column(DateTime(),primary_key=True)
    temp_max = Column(Numeric(12,2))
    temp_min = Column(Numeric(12,2))

db = pd.read_sql_query('SELECT * FROM duke.prediction_EIA',engine)
db.set_index('Datetime',inplace = True)
if date.today() <=  db.index[-1].date():
    print('There is nothing to update')
else:
    url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/spartanburg,sc?unitGroup=us&include=days&key=D5FAK4DB3LVUJGWCTFZRXA2T6&contentType=csv'
    dfM1 = pd.read_csv(url)
    dfM1 = dfM1[['datetime','name','tempmax','tempmin']]
    dfM1 = dfM1.rename(columns={'datetime':'Datetime'})
    dfM1 = dfM1.set_index('Datetime')
    city_list = ['GuilfordCounty,NC', 'GreenvilleCounty,SC', 'ForsythCounty,NC', 'SpartanburgCounty,SC', 'DurhamCounty,NC', 'YorkCounty,SC', 'IndianTrail,NC', 'GastonCounty,NC',
              'CabarrusCounty,NC', 'AndersonCounty,SC']

    for i in city_list:
        url = url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'+i+'?unitGroup=us&include=days&key=D5FAK4DB3LVUJGWCTFZRXA2T6&contentType=csv'
        df0 = pd.read_csv(url)
        df0 = df0[['datetime','name','tempmax','tempmin']]
        df0 = df0.rename(columns={'datetime':'Datetime'})
        df0 = df0.set_index('Datetime')
        dfM1 = pd.concat([dfM1,df0],axis=1)
        dfM1.dropna(inplace = True)

    Tem15d = dfM1[['tempmax','tempmin']]
    Tem15d['temp_max']= Tem15d.tempmax.mean(axis=1)
    Tem15d['temp_min']= Tem15d.tempmin.mean(axis=1)
    Tem15d = Tem15d[['temp_max','temp_min']]
    Tem15d = Tem15d.reset_index()
    Tem15d['Datetime'] = Tem15d['Datetime'].astype(str)
    Tem15d.iloc[:,1:] = Tem15d.iloc[:,1:].applymap(lambda x: round(Decimal(x),2))
    Tem15d.fillna(0.00, inplace=True)
    print(Tem15d)

    for i in range(Tem15d.shape[0]):
        appendix = Tem15d.iloc[i, 0].replace(' ','-')
        item_name = f'item_{appendix}'
        a0 = Tem15d.iloc[i,0]
        a1 = Tem15d.iloc[i,1]
        a2 = Tem15d.iloc[i,2]
        item_name = Weather_Update(Datetime=a0, temp_max=a1, temp_min=a2)
        session.add(item_name)

    session.commit()
    session.close()