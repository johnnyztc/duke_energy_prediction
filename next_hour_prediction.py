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


reg_new = xgb.XGBRegressor()
reg_new.load_model('model1.json')


engine = create_engine('mysql+pymysql://admin:ztc971110@duke-test.c6gn8p6i9qvw.us-east-2.rds.amazonaws.com:3306/duke',echo = False)
db = pd.read_sql_query('SELECT * FROM duke.actual_demand',engine)
db = db.tail(600)
db = db.set_index('Datetime')

db1 = db.tail(600)

now = datetime.now()
now = now.replace(minute=0, second=0, microsecond=0)
next_hour = now + timedelta(hours=1)
next_hour = pd.Timestamp(next_hour)

future = pd.date_range(db1.index[-1] + timedelta(hours=1),next_hour,freq = '1h')

future_df = pd.DataFrame(index=future)

future_df['DUK_MW'] = np.nan

def get_weather(loc, start, end):
    #main URL
    BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
    
    #user API key
    ApiKey='D5FAK4DB3LVUJGWCTFZRXA2T6'
    
    #location, concatenate if two-word city 
    if len(loc.split()) > 1:
        loc = loc.replace(" ", "")
    Location = loc
    
    #start date, end date inputs
    StartDate = start
    EndDate= end
    
    #choosing csv instead of json
    ContentType="csv"
    
    #selecting hourly data instead of daily
    Include="hours"
    
    #US system instead of metric
    UnitGroup='us'
    
    #incorporating location with API query
    ApiQuery=BaseURL + Location
    
     #accounting for start/end dates selected
    if (len(StartDate)):
        ApiQuery+="/"+StartDate
        if (len(EndDate)):
            ApiQuery+="/"+EndDate

    #adding '?' at end of API query
    ApiQuery+="?"

    #accounting for units, csv or json, and type of information 
    if (len(UnitGroup)):
        ApiQuery+="&unitGroup="+UnitGroup

    if (len(ContentType)):
        ApiQuery+="&contentType="+ContentType

    if (len(Include)):
        ApiQuery+="&include="+Include

    #adding user API key to query
    ApiQuery+="&key="+ApiKey
    
    #change CSV to dataframe
    df0=pd.read_csv(ApiQuery)

    #switch 'datetime' column to 1st column
    df0 = df0[['datetime', 'name', 'temp', 'humidity', 'precip', 'precipprob', 'preciptype','snow','snowdepth', 'windgust', 'windspeed','winddir', 'sealevelpressure', 'cloudcover', 'visibility', 'visibility', 'solarradiation', 'solarenergy', 'uvindex', 'severerisk', 'conditions', 'icon'  ]]

    #remove 'T' that is in every 'datetime' row
    df0['datetime'] = df0['datetime'].map(lambda x: x.replace('T',' '))

    #ensure that 'datetime' column matches EIA API pull for merge
    df0.rename(columns={'datetime': 'Datetime'}, inplace=True)
    
    #set 'datetime' column as datetime object and set as dataframe index
    df0['Datetime'] = pd.to_datetime(df0['Datetime'])
    df0.set_index('Datetime', drop=True, inplace=True)
    df0.index = pd.to_datetime(df0.index)
    
     #remove unwanted weather variables
    df0.drop('preciptype', axis=1, inplace=True) 
    df0.drop('conditions', axis=1, inplace=True) 
    df0.drop('icon', axis=1, inplace=True) 
    df0.drop('visibility', axis=1, inplace=True)
    df0.drop('snowdepth', axis=1, inplace=True)
   
    
    
    
    return df0

#function to call API multiple times (look further back) & combine dataframes
#NEED TO MAKE DATES DYNAMIC

def full_df(city):
    first_df = get_weather(city,str(db1.index[-1].date()),str(future_df.index[-1].date()))
    return first_df

# Choose 11 counties with DUK Carolinas as a main provider and with biggest population in DUK Carolinas service territory
test_list_3 = ['Mecklenburg County, NC', 'Guilford County, NC', 'Greenville County, SC', 'Forsyth County, NC', 'Spartanburg County, SC', 'Durham County, NC', 'York County, SC', 'Indian Trail, NC', 'Gaston County, NC',
              'Cabarrus County, NC', 'Anderson County, SC']

#function to create dictionary of weather dataframes whose names and corresponding weather are a given list of cities
def create_dfs(names):
    dfs = {}
    for x in names:
        dfs[x] = full_df(x)
    return dfs

#THIS CELL TAKES LONG TIME TO RUN
#create dictionary of weather data dataframes 
dfs_1 = create_dfs(test_list_3) 

#create one dataframe of historical weather for counties in Duke Energy Carolinas coverage area
weatherdf = pd.concat(dfs_1,axis=1)

weatherdf.columns = weatherdf.columns.droplevel(0)    

weatherdf['avg_temp'] = weatherdf[['temp', 'temp', 'temp', 'temp','temp', 'temp','temp', 'temp','temp', 'temp','temp' ]].mean(axis=1)
weatherdf['avg_humidity'] = weatherdf[['humidity', 'humidity', 'humidity', 'humidity','humidity', 'humidity','humidity', 'humidity','humidity', 'humidity','humidity' ]].mean(axis=1)
weatherdf['avg_precip'] = weatherdf[['precip', 'precip', 'precip', 'precip','precip', 'precip','precip', 'precip','precip', 'precip','precip' ]].mean(axis=1)
weatherdf['avg_precipprob'] = weatherdf[['precipprob', 'precipprob', 'precipprob', 'precipprob','precipprob', 'precipprob','precipprob', 'precipprob','precipprob', 'precipprob','precipprob' ]].mean(axis=1)
weatherdf['avg_snow'] = weatherdf[['snow', 'snow', 'snow', 'snow','snow', 'snow','snow', 'snow','snow', 'snow','snow' ]].mean(axis=1)
weatherdf['avg_windgust'] = weatherdf[['windgust', 'windgust', 'windgust', 'windgust','windgust', 'windgust','windgust', 'windgust','windgust', 'windgust','windgust' ]].mean(axis=1)
weatherdf['avg_windspeed'] = weatherdf[['windspeed', 'windspeed', 'windspeed', 'windspeed','windspeed','windspeed','windspeed', 'windspeed','windspeed', 'windspeed','windspeed' ]].mean(axis=1)
weatherdf['avg_winddir'] = weatherdf[['winddir', 'winddir', 'winddir', 'winddir','winddir', 'winddir','winddir', 'winddir','winddir', 'winddir','winddir' ]].mean(axis=1)
weatherdf['avg_sealevelpressure'] = weatherdf[['sealevelpressure', 'sealevelpressure', 'sealevelpressure', 'sealevelpressure','sealevelpressure', 'sealevelpressure','sealevelpressure', 'sealevelpressure','sealevelpressure', 'sealevelpressure','sealevelpressure' ]].mean(axis=1)
weatherdf['avg_cloudcover'] = weatherdf[['cloudcover', 'cloudcover', 'cloudcover', 'cloudcover','cloudcover', 'cloudcover','cloudcover', 'cloudcover','cloudcover', 'cloudcover','cloudcover' ]].mean(axis=1)
weatherdf['avg_solarradiation'] = weatherdf[['solarradiation', 'solarradiation', 'solarradiation', 'solarradiation','solarradiation', 'solarradiation','solarradiation', 'solarradiation','solarradiation', 'solarradiation','solarradiation' ]].mean(axis=1)
weatherdf['avg_solarenergy'] = weatherdf[['solarenergy', 'solarenergy', 'solarenergy', 'solarenergy','solarenergy', 'solarenergy','solarenergy', 'solarenergy','solarenergy', 'solarenergy','solarenergy' ]].mean(axis=1)
weatherdf['avg_uvindex'] = weatherdf[['uvindex', 'uvindex', 'uvindex', 'uvindex','uvindex', 'uvindex','uvindex', 'uvindex','uvindex', 'uvindex','uvindex' ]].mean(axis=1)
weatherdf['avg_severerisk'] = weatherdf[['severerisk', 'severerisk', 'severerisk', 'severerisk','severerisk', 'severerisk','severerisk', 'severerisk','severerisk', 'severerisk','severerisk' ]].mean(axis=1)
weatherdf.head(2)

# Drop column by index using DataFrame.iloc[] and drop() methods.
avg_wdf = weatherdf.drop(weatherdf.iloc[:, 1:165],axis = 1)

avg_wdf = avg_wdf[(avg_wdf.index >= future_df.index[0]) & (avg_wdf.index <= future_df.index[-1])]

df = pd.merge(future_df,avg_wdf,left_index=True,right_index=True)

df = df.drop(columns='avg_severerisk')

df = pd.concat([db1,df])

def create_features(df):
    """
    Create time series features based on time series index.
    """
    df = df.copy()
    df['hour'] = df.index.hour
    df['dayofweek'] = df.index.dayofweek
    df['quarter'] = df.index.quarter
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['dayofyear'] = df.index.dayofyear
    df['dayofmonth'] = df.index.day
    df['weekofyear'] = df.index.isocalendar().week
    return df

def add_lags(duk):
    #target_map = duk['DUK_MW'].to_dict()
    #duk['duk_1_hrs_lag'] = duk['DUK_MW'].shift(1)
    #duk['duk_6_hrs_lag'] = duk['DUK_MW'].shift(6)
    #duk['duk_12_hrs_lag'] = duk['DUK_MW'].shift(12)
    duk['duk_24_hrs_lag'] = duk['DUK_MW'].shift(24) 
    #duk['duk_168_hrs_lag'] = duk['DUK_MW'].shift(168)
    
    #duk['duk_6_hrs_mean'] = duk['DUK_MW'].rolling(window = 6).mean()
    duk['duk_12_hrs_mean'] = duk['DUK_MW'].rolling(window = 12).mean().shift(24)
    duk['duk_24_hrs_mean'] = duk['DUK_MW'].rolling(window = 24).mean().shift(24)  
    
    #duk['duk_6_hrs_std'] = duk['DUK_MW'].rolling(window = 6).std()  
    duk['duk_12_hrs_std'] = duk['DUK_MW'].rolling(window = 12).std().shift(24)
    duk['duk_24_hrs_std'] = duk['DUK_MW'].rolling(window = 24).std().shift(24)
    
    #duk['duk_6_hrs_max'] = duk['DUK_MW'].rolling(window = 6).max()
    duk['duk_12_hrs_max'] = duk['DUK_MW'].rolling(window = 12).max().shift(24)
    duk['duk_24_hrs_max'] = duk['DUK_MW'].rolling(window = 24).max().shift(24)
    duk['duk_168_hrs_max'] = duk['DUK_MW'].rolling(window = 168).max().shift(24)
    
    #duk['duk_6_hrs_min'] = duk['DUK_MW'].rolling(window = 6).min()
    #duk['duk_12_hrs_min'] = duk['DUK_MW'].rolling(window = 12).min()
    #duk['duk_24_hrs_min'] = duk['DUK_MW'].rolling(window = 24).min()
    return duk

df = create_features(df)
df = add_lags(df)
df['weekofyear']=df['weekofyear'].apply(np.int64)
df['prediction'] = reg_new.predict(df.drop('DUK_MW',axis = 1))
print(df.tail(24))