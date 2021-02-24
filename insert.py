from sqlalchemy import Table,Column,Integer,Float,String,MetaData,DateTime,create_engine
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

dynamicDataV2 = Table(
    'dynamicDataV2', meta,
    Column('Insert_ID', Integer, primary_key = True),
    Column('number', Integer, primary_key = True),
    Column('bike_stands', Integer),
    Column('available_bike_stands', Integer),
    Column('available_bikes', Integer),
    Column('last_update', DateTime)
)

meta.create_all(conn)

# select * from stations, availability where availability.number = stations.number
# Then you can attach some GROUP BY aggregations with the other table for the machine learning model

def get_station(obj):
        return {'number': obj['number'],
        'Insert_ID': 0,
        'bike_stands': obj['bike_stands'],
        'available_bike_stands': obj['available_bike_stands'],
        'available_bikes': obj['available_bikes'],
        'last_update': datetime.datetime.fromtimestamp( int(obj['last_update'] / 1e3) )
        }

NAME = "Dublin"
STATIONS_URI = "https://api.jcdecaux.com/vls/v1/stations"
KEY = "e47f3963b98124079388f63783dc9c319b0ae443"

iterator = 0

while True:
        try:     
                print('Starting Loop')
                r = requests.get(STATIONS_URI, params = {"apiKey": KEY, "contract": NAME})
                JSON(r.json())

                values = list(map(get_station, r.json()))
                for value in values:
                        value['Insert_ID'] = iterator
                        print(value['Insert_ID'])

                ins = dynamicDataV2.insert().values(values)
                conn.execute(ins)
                print('Finishing execute')
                time.sleep(5*60)
                print('Restarting Loop')

                iterator += 1
        except:
                print(sys.exc_info()[0])
                iterator += 1
                time.sleep(5*60)
