#!/usr/bin/env python2
import datetime
from pyModbusTCP.client import ModbusClient
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import requests

def insertInto(place, value):
    sessionCassandra.execute(
        """
        INSERT INTO temp (event_time, place, value)
        VALUES (%s, %s, %s)
        """,
        (datetime.datetime.now(), place, value)
    )
    
def getAllTemp():
    values = sessionCassandra.execute(
        """
        SELECT * FROM temp
        """
    )
    return values

# Cassandra 
def cassandraConnection(addr):

    print("-------Cassandra Connection-------")
    print("add :", addr)
    cluster = Cluster([addr])

    session = cluster.connect()

    # create keyspace
    session.execute("create keyspace IF NOT EXISTS dev with replication = {'class':'SimpleStrategy','replication_factor':1}")
    session.set_keyspace('dev')

    # Create table
    session.execute("create table IF NOT EXISTS temp (PRIMARY KEY(event_time, place), event_time timestamp, place varchar, value int)")

    return session

# Modbus
def modbusConnection(addr, port):
    print("-------MODBUS-------")
    print("add:", addr)
    print("port:", port)
    return ModbusClient(host=addr, port=port, auto_open=True, auto_close=True)

# Get Data from modbus sessions
def getModbusData(add, nb):
    return sessionModbus.read_holding_registers(add, nb)

def getOpenWeatherData():
    r = requests.get(url = "http://api.openweathermap.org/data/2.5/weather?q=Nanterre,FR&appid=02132a45faa49f09ad163493f1980378") 
  
    # extracting data in json format 
    data = r.json() 

    return int(data["main"]["temp"])


# Main
sessionCassandra = cassandraConnection("localhost")

sessionModbus = modbusConnection("localhost", 5007)


regs = getModbusData(17, 4)

print("Inserting Modbus")

if (regs) :
    # Insert Table
    insertInto("Room 1", regs[0])
    insertInto("Room 2", regs[1])
    insertInto("Room 3", regs[2])
    insertInto("Room 4", regs[3])

print("Inserting OpenWeather")

data = getOpenWeatherData()

insertInto("Outside", data)

# Read Cassandra insertion
print("----READING----")

items = getAllTemp()

for item in items:
    print(item.event_time, item.place, item.value)
