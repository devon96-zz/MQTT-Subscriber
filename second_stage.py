import json
import sqlite3
import re
from time import mktime, gmtime, sleep
from datetime import datetime, timedelta
from base64 import decodestring

while True:

    connection = sqlite3.connect('mydb.sqlite')
    c = connection.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS second_stage
                (id INTEGER PRIMARY KEY, 
                TimeStamp INTEGER, 
                ProcessedFlag INTEGER,
                Port TEXT, 
                Dev_ID TEXT, 
                Payload BLOB)''')

    c.execute(
        'SELECT id, ProcessedFlag, String FROM first_stage WHERE ProcessedFlag=0')

    for row in c:
        json_data = json.loads(row[2])

        c2 = connection.cursor()
        c2.execute('''INSERT INTO second_stage(TimeStamp, ProcessedFlag, Port, Dev_ID, Payload)
        VALUES(?,?,?,?,?)''',
                   (mktime(gmtime()),
                    1,
                    json_data["port"],
                    json_data["dev_id"],
                    decodestring(json_data["payload_raw"])))

        c2.execute(
            'UPDATE first_stage SET ProcessedFlag = 1 WHERE id = ?', (str(row[0]),))

    time_elapsed_30days = mktime(
        gmtime()) - mktime((datetime.now() - timedelta(days=30)).timetuple())

    c.execute('''DELETE FROM second_stage
            WHERE (? - TimeStamp >= ?)''', (mktime(gmtime()), time_elapsed_30days))

    connection.commit()
    connection.close()
    sleep(5)
