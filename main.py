from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import urllib

from datetime import datetime

#DATABASE_URL = "sqlite:///./test.db"

host_server = os.environ.get('host_server', 'localhost')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '5432')))
database_name = os.environ.get('database_name', 'fastapi')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'postgres')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'secret')))
DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}'.format(db_username, db_password, host_server, db_server_port, database_name)

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

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
    sqlalchemy.Column("timestamp", sqlalchemy.DateTime, primary_key=True),
    sqlalchemy.Column("user", sqlalchemy.String, primary_key=True),
    sqlalchemy.Column("stnname", sqlalchemy.String),
    sqlalchemy.Column("reserve_start", sqlalchemy.DateTime),
    sqlalchemy.Column("reserve_end", sqlalchemy.DateTime),
    sqlalchemy.Column("reserve_id", sqlalchemy.Integer),
)

engine = sqlalchemy.create_engine(
    #DATABASE_URL, connect_args={"check_same_thread": False}
    DATABASE_URL, pool_size=3, max_overflow=0
)
metadata.create_all(engine)


class Reservation(BaseModel):
    timestamp: datetime
    user: str
    stnname: str
    reserve_start: datetime
    reserve_end: datetime
    reserve_id: int


class ReservationIn(BaseModel):
    timestamp: datetime
    user: str
    stnname: str
    reserve_start: datetime
    reserve_end: datetime


class Station(BaseModel):
    uiccode: int
    stncode: str
    lat: float
    lon: float
    bike_capacity: int
    stnname: str


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
async def create_note(note: ReservationIn):
    query = reservations.insert().values(text=note.text,
                                         completed=note.completed)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}


@app.put("/reservation/{note_id}/", response_model=Reservation)
async def update_note(note_id: int, payload: ReservationIn):
    query = reservations.update().where(reservations.c.id == note_id).values(text=payload.text,
                                                                             completed=payload.completed)
    await database.execute(query)
    return {**payload.dict(), "id": note_id}


@app.get("/reservation/", response_model=List[Reservation])
async def read_notes(skip: int = 0, take: int = 20):
    query = reservations.select().offset(skip).limit(take)
    return await database.fetch_all(query)


@app.get("/reservation/{note_id}/", response_model=Reservation)
async def read_notes(note_id: int):
    query = reservations.select().where(reservations.c.id == note_id)
    return await database.fetch_one(query)


@app.delete("/reservation/{note_id}/")
async def update_note(note_id: int):
    query = reservations.delete().where(reservations.c.id == note_id)
    await database.execute(query)
    return {"message": "Reservation with id: {} deleted successfully!".format(note_id)}
