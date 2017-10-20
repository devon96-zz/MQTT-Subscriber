import json
import sqlite3
import re
from time import mktime, gmtime
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
from base64 import decodestring
from hexdump import hexdump


def on_connect(client, userdata, flags, rc):
    client.subscribe("+/devices/+/up")


def on_message(client, userdata, msg):

    m = re.search('.?({.*)', msg.payload)

    connection = sqlite3.connect('mydb.sqlite')
    c = connection.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS first_stage (id INTEGER PRIMARY KEY, TimeStamp INTEGER, ProcessedFlag INTEGER, String TEXT)')
    c.execute('''INSERT INTO first_stage(TimeStamp, ProcessedFlag, String)
                    VALUES(?,?,?)''', (mktime(gmtime()), 0, m.group(1)))

    time_elapsed_7days = mktime(
        gmtime()) - mktime((datetime.now() - timedelta(days=7)).timetuple())

    c.execute('''DELETE FROM first_stage
                WHERE (? - TimeStamp >= ?)''', (mktime(gmtime()), time_elapsed_7days))
    connection.commit()
    connection.close()


with open('config.json') as data_file:
    credentials = json.load(data_file)

client = mqtt.Client()
client.username_pw_set(
    credentials["username"], credentials["password"])

client.on_connect = on_connect
client.on_message = on_message

client.connect("eu.thethings.network")

client.loop_forever()
