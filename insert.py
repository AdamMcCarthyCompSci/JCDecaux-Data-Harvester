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

dynamicDataV6 = Table(
    'dynamicDataV6', meta,
    Column('Insert_ID', Integer, primary_key = True),
    Column('number', Integer, primary_key = True),
    Column('bike_stands', Integer),
    Column('available_bike_stands', Integer),
    Column('available_bikes', Integer),
    Column('last_update', DateTime)
)

meta.create_all(conn)

NoneType = type(None)

def get_station(obj):
        print("before")
        try:
                x = datetime.datetime.fromtimestamp( int(obj['last_update'] / 1e3) )
        except:
                x = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        return {'number': obj['number'],'bike_stands': obj['bike_stands'],'available_bike_stands': obj['available_bike_stands'],'available_bikes': obj['available_bikes'],'last_update': x}
 
NAME = "Dublin"
STATIONS_URI = "https://api.jcdecaux.com/vls/v1/stations"
KEY = "53fa78aead76e2416050fc002610856adf0b2cee"

iterator = 211
r = requests.get(STATIONS_URI, params = {"apiKey": KEY, "contract": NAME})
JSON(r.json())

while True:
        try:
                print('Starting Loop')
                r = requests.get(STATIONS_URI, params = {"apiKey": KEY, "contract": NAME})
                JSON(r.json())
                print('request to map')
                values = list(map(get_station, r.json()))
                for value in values:
                        value['Insert_ID'] = iterator
                print('map to insert')
                ins = dynamicDataV6.insert().values(values)
                print('insert to execute')
                conn.execute(ins)
                print('Finishing execute')
                time.sleep(5*60)
                print('Restarting Loop')

                iterator += 1
        except Exception as e:
                print(e)
                iterator += 1
                time.sleep(5*60)

