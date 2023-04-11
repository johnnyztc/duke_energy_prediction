import pandas as pd
import numpy as np
import requests
import pymysql
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
db = pd.read_sql_query('SELECT * FROM duke.actual_demand',engine)
db = db.tail(200)
metadata_obj = MetaData()
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()



class Demand(Base):
    __tablename__ = 'actual_demand'
    
    Datetime = Column(DateTime(),primary_key=True)
    DUK_MW = Column(Numeric(12,2))
    avg_temp = Column(Numeric(12,2))
    avg_humidity = Column(Numeric(12,2))
    avg_precip = Column(Numeric(12,2))
    avg_precipprob = Column(Numeric(12,2))
    avg_snow = Column(Numeric(12,2))
    avg_windgust = Column(Numeric(12,2))
    avg_windspeed = Column(Numeric(12,2))
    avg_winddir = Column(Numeric(12,2))
    avg_sealevelpressure = Column(Numeric(12,2))
    avg_cloudcover = Column(Numeric(12,2))
    avg_solarradiation = Column(Numeric(12,2))
    avg_solarenergy = Column(Numeric(12,2))
    avg_uvindex = Column(Numeric(12,2))
    hour = Column(Numeric(12,0))
    dayofweek = Column(Numeric(12,0))
    quarter = Column(Numeric(12,0))
    month = Column(Numeric(12,0))
    year = Column(Numeric(12,0))
    dayofyear = Column(Numeric(12,0))
    dayofmonth = Column(Numeric(12,0))
    weekofyear = Column(Numeric(12,0))
    duk_24_hrs_lag = Column(Numeric(12,2))
    duk_12_hrs_mean = Column(Numeric(12,2))
    duk_24_hrs_mean = Column(Numeric(12,2))
    duk_12_hrs_std = Column(Numeric(12,2))
    duk_24_hrs_std = Column(Numeric(12,2))
    duk_12_hrs_max = Column(Numeric(12,2))
    duk_24_hrs_max = Column(Numeric(12,2))
    duk_168_hrs_max = Column(Numeric(12,2))

db = db.set_index('Datetime')

def update_demand():
    t0 = str(date.today() + timedelta(hours=24))
    t1 = str((db.index[-1] - timedelta(hours=24)).date())
    api_key = 'phRLIs3z4GNYWKHtB5d2zunfICyqTsUSpnRJvq2S'
    url = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=local-hourly&data[0]=value&facets[respondent][]=DUK&start='+t1+'&end='+t0+'&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key=phRLIs3z4GNYWKHtB5d2zunfICyqTsUSpnRJvq2S'
    r = requests.get(url)
    d = r.json()
    df = pd.json_normalize(d, record_path=['response', 'data'])
    df1 = df[df['type-name'] == 'Demand']
    df1 = df1.reset_index()
    df1.drop('index',axis=1,inplace = True)
    df1['Datetime'] = df1['period']

    for i in range(len(df1)):
        dt=datetime.strptime(df1['period'][i][:-3], "%Y-%m-%dT%H")
        df1['Datetime'][i] = dt
        i = i+1
    
    df1 = df1.set_index('Datetime')
    df1 = df1[['value']]
    df1 = df1.rename(columns={'value':'DUK_MW'})
    return df1

df1 = update_demand()


if df1.index[-1] <= db.index[-1]:
    print('There is nothing to update')

else:

    df1 = df1[df1.index > db.index[-1]]

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
        first_df = get_weather(city,str(db.index[-1].date()),str(df1.index[-1].date()))
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

    avg_wdf = avg_wdf[(avg_wdf.index >= df1.index[0]) & (avg_wdf.index <= df1.index[-1])]

    df = pd.merge(df1,avg_wdf,left_index=True,right_index=True)

    df = df.drop(columns='avg_severerisk')

    df = pd.concat([db,df])


    def create_features(df):
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
        
        duk['duk_24_hrs_lag'] = duk['DUK_MW'].shift(24) 
        duk['duk_12_hrs_mean'] = duk['DUK_MW'].rolling(window = 12).mean().shift(24)
        duk['duk_24_hrs_mean'] = duk['DUK_MW'].rolling(window = 24).mean().shift(24)  
        duk['duk_12_hrs_std'] = duk['DUK_MW'].rolling(window = 12).std().shift(24)
        duk['duk_24_hrs_std'] = duk['DUK_MW'].rolling(window = 24).std().shift(24)
        duk['duk_12_hrs_max'] = duk['DUK_MW'].rolling(window = 12).max().shift(24)
        duk['duk_24_hrs_max'] = duk['DUK_MW'].rolling(window = 24).max().shift(24)
        duk['duk_168_hrs_max'] = duk['DUK_MW'].rolling(window = 168).max().shift(24)
        return duk


    df = create_features(df)

    df = add_lags(df)

    df = df[df.index > db.index[-1]]

    df['weekofyear']=df['weekofyear'].apply(np.int64)

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
        a2 = df.iloc[i,2]
        a3 = df.iloc[i,3]
        a4 = df.iloc[i,4]
        a5 = df.iloc[i,5]
        a6 = df.iloc[i,6]
        a7 = df.iloc[i,7]
        a8 = df.iloc[i,8]
        a9 = df.iloc[i,9]
        a10 = df.iloc[i,10]
        a11 = df.iloc[i,11]
        a12 = df.iloc[i,12]
        a13 = df.iloc[i,13]
        a14 = df.iloc[i,14]
        a15 = df.iloc[i,15]
        a16 = df.iloc[i,16]
        a17 = df.iloc[i,17]
        a18 = df.iloc[i,18]
        a19 = df.iloc[i,19]
        a20 = df.iloc[i,20]
        a21 = df.iloc[i,21]
        a22 = df.iloc[i,22]
        a23 = df.iloc[i,23]
        a24 = df.iloc[i,24]
        a25 = df.iloc[i,25]
        a26 = df.iloc[i,26]
        a27 = df.iloc[i,27]
        a28 = df.iloc[i,28]
        a29 = df.iloc[i,29]
        a30 = df.iloc[i,30]
        item_name = Demand(Datetime=a0, DUK_MW=a1, avg_temp=a2, avg_humidity=a3, avg_precip=a4, 
                        avg_precipprob=a5, avg_snow=a6, avg_windgust=a7, avg_windspeed=a8,
                        avg_winddir=a9, avg_sealevelpressure=a10, avg_cloudcover=a11, avg_solarradiation=a12, 
                        avg_solarenergy=a13, avg_uvindex=a14, hour=a15, dayofweek=a16, 
                        quarter=a17, month=a18, year=a19, dayofyear=a20, dayofmonth=a21,
                        weekofyear=a22, duk_24_hrs_lag=a23, duk_12_hrs_mean=a24, duk_24_hrs_mean=a25, 
                        duk_12_hrs_std=a26, duk_24_hrs_std=a27, duk_12_hrs_max=a28, duk_24_hrs_max=a29, 
                        duk_168_hrs_max=a30)
        session.add(item_name)

    session.commit()

    session.close()

