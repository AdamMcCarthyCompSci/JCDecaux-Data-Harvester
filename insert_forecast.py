from sqlalchemy import Table,Column,Integer,Float,String,MetaData,DateTime,create_engine,types
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
from IPython.display import JSON
import time

username = "DublinBikesApp"
password = "dublinbikesapp"
endpoint = "dublinbikesapp.cynvsd3ef0ri.us-east-1.rds.amazonaws.com"
port = "3306"
db = "DublinBikesApp"

engine = create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".format(username, password, endpoint, port, db), echo=True)

meta = MetaData(engine)
conn = engine.connect()
forecast_wx = Table(
    'forecast', meta,
    Column('last_update', DateTime, primary_key = True),
    Column('weather', String(30)),
    Column('temp', String(30))
)

meta.create_all(conn)

NoneType = type(None)

weatherNAME = "7778677"
weatherKEY = "b5082b06a1653481cc7a8a9a533aafc3"
weatherUNITS = "metric"
forecastURI = "https://api.openweathermap.org/data/2.5/forecast?id={}&appid={}&units={}".format(weatherNAME, weatherKEY, weatherUNITS)

iterator = 0

while True:
        try:
                
                sql_Delete_query = """Delete from forecast"""
                conn.execute(sql_Delete_query)


                f = requests.get(forecastURI)
                data = json.loads(f.text)
                for dt in data["list"]:
                #datetime = data["dt"]
                #weather = data["weather"][0]["main"]
                #temp = data["main"]["temp"]
                        data_wx={
                       'last_update':dt["dt_txt"],
                       'weather':dt["weather"][0]["main"],
                       'temp':dt["main"]["temp"]
                        }
                        ins = forecast_wx.insert().values(data_wx)
                        conn.execute(ins)
                        #JSON(f.json())
                
                #ins = forecast_wx.insert().values(data_wx)
                #conn.execute(ins)
                time.sleep(180*60)

                iterator += 1
        except Exception as e:
                print(e)
                iterator += 1
                time.sleep(5*60)


