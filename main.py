from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import urllib
import json
import pandas as pd
import requests
from typing import Any
from datetime import datetime

host_server = os.environ.get('host_server', 'localhost')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '5432')))
database_name = os.environ.get('database_name', 'fastapi')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'postgres')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'secret')))
DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}'.format(db_username, db_password, host_server, db_server_port, database_name)

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData(schema='hackatrain2020')

stations = sqlalchemy.Table(
    "stations",
    metadata,
    sqlalchemy.Column("uiccode", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("stncode", sqlalchemy.String),
    sqlalchemy.Column("lat", sqlalchemy.Float),
    sqlalchemy.Column("lon", sqlalchemy.Float),
    sqlalchemy.Column("bike_capacity", sqlalchemy.Integer),
    sqlalchemy.Column("stnname", sqlalchemy.String),
)

reservations = sqlalchemy.Table(
    "reservations",
    metadata,
    sqlalchemy.Column("reserve_id", sqlalchemy.BigInteger, primary_key=True),
    sqlalchemy.Column("user", sqlalchemy.String),
    sqlalchemy.Column("timestamp", sqlalchemy.DateTime),
    sqlalchemy.Column("uiccode", sqlalchemy.Integer),
    sqlalchemy.Column("reserve_start", sqlalchemy.DateTime),
    sqlalchemy.Column("reserve_end", sqlalchemy.DateTime),
)

engine = sqlalchemy.create_engine(
    DATABASE_URL, pool_size=3, max_overflow=0
)
metadata.create_all(engine)


class Reservation(BaseModel):
    reserve_id: int
    user: str
    timestamp: datetime
    uiccode: int
    reserve_start: datetime
    reserve_end: datetime


class ReservationIn(BaseModel):
    timestamp: datetime
    user: str
    uiccode: int
    reserve_start: datetime
    reserve_end: datetime


class Station(BaseModel):
    uiccode: int
    stncode: str
    lat: float
    lon: float
    bike_capacity: int
    stnname: str


class Journey(BaseModel):
    direction: str
    name: str
    plannedDateTime: datetime
    trainCategory: str
    departureStatus: str
    plannedTrack: Any


app = FastAPI(title="REST API using FastAPI PostgreSQL Async EndPoints")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.post("/reservation/", response_model=Reservation)
async def create_reservation(reservation: ReservationIn):
    # HOTFIX: Remove timezone info
    reservation.timestamp = reservation.timestamp.replace(tzinfo=None)
    reservation.reserve_start = reservation.reserve_start.replace(tzinfo=None)
    reservation.reserve_end = reservation.reserve_end.replace(tzinfo=None)
    query = reservations.insert().values(timestamp=reservation.timestamp,
                                         user=reservation.user,
                                         uiccode=reservation.uiccode,
                                         reserve_start=reservation.reserve_start,
                                         reserve_end=reservation.reserve_end,
                                         )
    last_record_id = await database.execute(query)
    return {**reservation.dict(), "reserve_id": last_record_id}


@app.put("/reservation/{reservation_id}/", response_model=Reservation)
async def update_reservation(reservation_id: int, payload: ReservationIn):
    # HOTFIX: Remove timezone info
    payload.timestamp = payload.timestamp.replace(tzinfo=None)
    payload.reserve_start = payload.reserve_start.replace(tzinfo=None)
    payload.reserve_end = payload.reserve_end.replace(tzinfo=None)
    query = reservations.update().where(reservations.c.reserve_id == reservation_id).values(timestamp=payload.timestamp,
                                                                                            user=payload.user,
                                                                                            uiccode=payload.uiccode,
                                                                                            reserve_start=payload.reserve_start,
                                                                                            reserve_end=payload.reserve_end,
                                                                                            )
    await database.execute(query)
    return {**payload.dict(), "reserve_id": reservation_id}


@app.get("/reservation/", response_model=List[Reservation])
async def read_reservations(skip: int = 0, take: int = 20):
    query = reservations.select().offset(skip).limit(take)
    return await database.fetch_all(query)


@app.get("/reservation/{reservation_id}/", response_model=Reservation)
async def read_reservations(reservation_id: int):
    query = reservations.select().where(reservations.c.reserve_id == reservation_id)
    return await database.fetch_one(query)


@app.delete("/reservation/{reservation_id}/")
async def update_reservation(reservation_id: int):
    query = reservations.delete().where(reservations.c.reserve_id == reservation_id)
    await database.execute(query)
    return {"message": "Reservation with id: {} deleted successfully!".format(reservation_id)}


@app.get("/user/{user}/", response_model=List[Reservation])
async def read_user_reservations(user: str, skip: int = 0, take: int = 20):
    query = reservations.select().where(reservations.c.user == user).offset(skip).limit(take)
    return await database.fetch_all(query)


@app.get("/station/", response_model=List[Station])
async def read_stations(skip: int = 0, take: int = 20):
    query = stations.select().offset(skip).limit(take)
    return await database.fetch_all(query)


@app.get("/ns_departures/", response_model=List[Journey])
def ns_departures(date_time: datetime = datetime(2020, 10, 1, 1, 32), uic_code: int = 8400530, max_journeys: int = 5):
    headers = {
        # Request headers
        'Ocp-Apim-Subscription-Key': os.environ.get('ns_api', '<NS_API_KEY>')
    }

    params = urllib.parse.urlencode({
        'uicCode': uic_code,
        'dateTime': date_time,
        'lang': 'nl',
        'maxJourneys': max_journeys,
    })

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    url = f"https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures?{params}"
    response = session.get(url=url, headers=headers)
    content = json.loads(response.text)

    df = pd.DataFrame(content['payload']['departures'])
    df = df[['direction', 'name', 'plannedDateTime', 'trainCategory', 'departureStatus', 'plannedTrack']]
    # HOTFIX: Planned track sometimes is NULL
    df['plannedTrack'] = df['plannedTrack'].astype(str)
    df['plannedTrack'] = df['plannedTrack'].fillna('')
    # HOTFIX: Remove timezon to timestamp. Only UTC
    df['plannedDateTime'] = df['plannedDateTime'].str.split('+', expand=True)[0]
    df['plannedDateTime'] = pd.to_datetime(df['plannedDateTime'])

    out = df.to_dict(orient='records')
    return out
