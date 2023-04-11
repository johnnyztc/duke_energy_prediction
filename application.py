import pandas as pd
import requests
import numpy as np
import sklearn
from sklearn.metrics import mean_squared_error
from datetime import date
from datetime import datetime
import matplotlib
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import matplotlib.dates as mdates
import seaborn as sns
import pytz
import csv
from pathlib import Path
import statsmodels as sm                 
from statsmodels.tools.eval_measures import rmse
import warnings
from datetime import datetime
from datetime import timedelta
import xgboost as xgb
import pytz
from datetime import datetime
import csv
from datetime import timedelta
import plotly.express as px
warnings.filterwarnings("ignore")
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import numpy as np
import requests
import pymysql
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, Integer, String, Boolean, Numeric,DateTime
from datetime import date
from datetime import timedelta
from datetime import datetime
from decimal import Decimal


TEMPLATE = 'plotly_white'
engine = create_engine('mysql+pymysql://admin:ztc971110@duke-test.c6gn8p6i9qvw.us-east-2.rds.amazonaws.com:3306/duke',echo = False)

Tem15d = pd.read_sql_query('SELECT * FROM duke.weather_forecast',engine)
Tem15d = Tem15d.set_index('Datetime')
Tem15d = Tem15d.tail(15)

fig2 = make_subplots(specs=[[{"secondary_y": True}]])
fig2.add_trace(go.Scatter(mode="lines+markers",x=Tem15d.index, y=Tem15d.temp_max,name='temp_max',yaxis="y1",
                         line=dict(color='red', width=2),marker=dict(symbol='circle',size=8)))


fig2.add_trace(go.Scatter(mode="lines+markers",x=Tem15d.index, y=Tem15d.temp_min,name='temp_min',yaxis='y1',
                         line=dict(color='rosybrown', width=2),marker=dict(symbol='circle',size=8)))

fig2.update_layout(template=TEMPLATE)

fig2.update_layout(title = 'Next 15 days average weather forecast') 

fig2.update_layout(hovermode="x unified")





data = pd.read_sql_query('SELECT * FROM duke.actual_demand',engine)
data = data.set_index('Datetime')
db = data.tail(12)
df_latest_36 = db[['DUK_MW']]
data1 = pd.read_sql_query('SELECT * FROM duke.prediction',engine)
db1 = data1.set_index('Datetime')
db1 = db1[db1.index >= df_latest_36.index[0]]
df_latest_36 = pd.merge(df_latest_36,db1,left_index=True,right_index=True,how='right')
data2= pd.read_sql_query('SELECT * FROM duke.prediction_EIA',engine)
db2 = data2.set_index('Datetime')
db2 = db2[db2.index >= df_latest_36.index[0]]
df_latest_36 = pd.merge(df_latest_36,db2,left_index=True,right_index=True,how='left')
df_latest_36 = df_latest_36.rename(columns={'DUK_MW':'actual_demand','avg_temp':'temperature'})

db3 = data
data1 = data1.set_index('Datetime')
db3 = db3.tail(720)
db3 = db3[['DUK_MW','avg_temp']]
data1 = data1[['prediction']]
df = pd.merge(db3,data1,left_index=True,right_index=True,how='left')
data2 = data2.set_index('Datetime')
df = pd.merge(df,data2,left_index=True,right_index=True,how='left')
df = df.rename(columns={'DUK_MW':'actual_demand','avg_temp':'temperature'})
df0 = df_latest_36[(df_latest_36.index > df.index[-1])]
df = pd.concat([df,df0])



duk_annual_peak = data.loc[(data.index>=str(datetime.today().year)+'-01-01')&(data.index<str(datetime.today().year+1)+'-01-01')].dropna().sort_values(by = 'DUK_MW',ascending = False).head(10)
duk_annual_peak = duk_annual_peak.rename(columns={'DUK_MW':'actual_demand','avg_temp':'temperature'})
duk_annual_peak = duk_annual_peak[['actual_demand','temperature']]
annual_peak = duk_annual_peak.actual_demand[0]

duk_annual_peak = duk_annual_peak.reset_index()





fig3 = px.bar(duk_annual_peak, x='actual_demand', y=duk_annual_peak.index,
              orientation = 'h',text = 'Datetime',hover_name='Datetime',hover_data=['temperature']                                                            
             )
fig3.update_layout(yaxis= dict(title = 'ranking'))

fig3.update_layout(xaxis= dict(title = 'demand'))

fig3.update_layout(template=TEMPLATE)

fig3.update_traces(opacity=0.75)

fig3.update_layout(title = 'Top 10 demands of year 2023 ') 



fig1 = make_subplots(specs=[[{"secondary_y": True}]])



fig1.add_trace(go.Scatter(mode="lines+markers",x=df_latest_36.index, y=df_latest_36.actual_demand, name='actual_demand',yaxis="y3",
                         line = dict(color='royalblue', width=1),marker=dict(symbol='triangle-up-dot',size=10)))

fig1.add_trace(go.Scatter(mode="lines+markers",x=df_latest_36.index, y=df_latest_36.prediction, name='prediction',yaxis="y3",
                         line = dict(color='lime', width=1, dash='dot'), marker=dict(symbol='triangle-up-dot',size=10)))

fig1.add_trace(go.Scatter(x=df_latest_36.index, y=df_latest_36.prediction_EIA,visible='legendonly',name='prediction_EIA',yaxis="y3",
                         line=dict(color='mediumpurple', width=3,dash='dot')))


fig1.add_trace(go.Scatter(mode="lines+markers", x=df_latest_36.index, y=df_latest_36.temperature,name='temperature',yaxis="y1",
                         line=dict(color='rosybrown', width=1),marker=dict(symbol='circle',size=5)))



fig1.add_hline(y = 70, line_dash="dot",line_color='black',line_width=0,
              annotation_text=str(datetime.today().year)+" annual demand peak", 
              annotation_position="bottom right")

fig1.add_shape(type="line",
    x0=df_latest_36.index[0], y0=annual_peak, x1=df_latest_36.index[-1], y1=annual_peak,
    line=dict(color="black",width=1,dash="dot",),yref="y3"
)



fig1.update_layout(

    yaxis3=dict(
        title="DUK demand (megawatthours)",
        overlaying="y",
        side="left",
        range=[8000,20000],
    ),
    yaxis1=dict(
        title="Temperature (°F)",
        side="right",
        range=[0,84]
    ))


fig1.update_layout(template=TEMPLATE)

fig1.update_layout(hovermode="x unified")





fig = make_subplots(specs=[[{"secondary_y": True}]])


fig.add_trace(go.Scatter(x=df.index, y=df.actual_demand, name='actual_demand',yaxis="y3",
                         line = dict(color='royalblue', width=3)))

fig.add_trace(go.Scatter(x=df.index, y=df.prediction, name='prediction',yaxis="y3",
                         line = dict(color='lime', width=3, dash='dot')))

fig.add_trace(go.Scatter(x=df.index, y=df.prediction_EIA,visible='legendonly',name='prediction_EIA',yaxis="y3",
                         line=dict(color='mediumpurple', width=3,dash='dot')))


fig.add_trace(go.Scatter(x=df.index, y=df.temperature,name='temperature',yaxis="y1",
                         line=dict(color='rosybrown', width=1)))



fig.add_hline(y = 70, line_dash="dot",line_color='black',line_width=0,
              annotation_text=str(datetime.today().year)+" annual demand peak", 
              annotation_position="bottom right")

fig.add_shape(type="line",
    x0=df.index[0], y0=annual_peak, x1=df.index[-1], y1=annual_peak,
    line=dict(color="black",width=1,dash="dot",),yref="y3"
)



fig.update_layout(

    yaxis3=dict(
        title="DUK demand (megawatthours)",
        overlaying="y",
        side="left",
        range=[8000,20000],
    ),
    yaxis1=dict(
        title="Temperature (°F)",
        side="right",
        range=[0,84]
    ))


fig.update_layout(template=TEMPLATE)

fig.update_layout(hovermode="x unified")

fig.update_layout(title = 'Past 30 days demand ') 

# Create timezone objects for UTC and EDT
utc = pytz.utc
edt = pytz.timezone('US/Eastern')

# Get the current time in UTC
current_time_utc = datetime.now(utc)

# Convert the current time to EDT
current_time_edt = current_time_utc.astimezone(edt)

# Print the current date and hour in EDT
current_time_edt = current_time_edt.strftime('%Y-%m-%d %H')


description = 'Duke Energy Carolinas is a subsidiary of Duke Energy, one of the largest electric power holding companies in the United States.\
Duke Energy Carolinas serves approximately 2.6 million customers in North Carolina and South Carolina.\
The company provides electric service to residential, commercial, and industrial customers,\
as well as wholesale customers such as municipalities and electric cooperatives.'

model_description= 'The DUK forecasting model was trained on historical load and weather data\
from 2015/7-2023/2. Weather readings were from VisualCrossing.'



app = dash.Dash(
    external_stylesheets=[dbc.themes.LUX],
    suppress_callback_exceptions=True
)
server = app.server

application = app.server


app.layout = html.Div([
        html.Div(id='duk-content'),
        html.Br(),

        dbc.Row([
            dbc.Col(html.H1('Duke Energy Carolinas (DUK)'), width=9),
            dbc.Col(width=2),
        ], justify='center'),
    dbc.Row([
            dbc.Col(
            html.Div(children=description), width=9),
            dbc.Col(width=2)
        ], justify='center'),
    html.Br(),
        dbc.Row([
            dbc.Col(
                html.H3('DUK electricity demand prediction'), width=9
            ),
            dbc.Col(width=2),
        ], justify='center'),
        dbc.Row([
            dbc.Col(
            html.Div(f"CURRENT RUN: {current_time_edt}:00"), width=9),
            dbc.Col(width=2)
        ], justify='center'),

    dcc.Graph(id='duk-graph',
             figure=fig1),
        html.Br(),
        dbc.Row([
            dbc.Col(html.H3('Training Data'), width=9),
            dbc.Col(width=2)
        ], justify='center'),
        dbc.Row([
            dbc.Col(
                    html.Div(children=model_description), width=9
            ),
            dbc.Col(width=2)
        ], justify='center'),
        html.Br(),
        dbc.Row(
            [
                dbc.Col(
                    html.Div([
                        dcc.Graph(
                            figure=fig3
                        ),
                    ]), width=4),
                dbc.Col(
                    html.Div([
                        dcc.Graph(
                            figure=fig2
                        ),]), width=4),
                dbc.Col(
                    html.Div([
                        dcc.Graph(
                            figure=fig
                        ),]), width=4)
])                
])

if __name__ == '__main__':
    app.run_server(debug = True,use_reloader=False)
