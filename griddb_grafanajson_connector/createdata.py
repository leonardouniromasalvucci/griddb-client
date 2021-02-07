#!/usr/bin/python3
import time
import griddb_python as griddb
import sys
import datetime
import calendar
import random

factory = griddb.StoreFactory.get_instance()

argv = sys.argv

blob = bytearray([65, 66, 67, 68, 69, 70, 71, 72, 73, 74])
update = True

try:
    gridstore = factory.get_store(host="239.0.0.1", port=31999, cluster_name="defaultCluster", username="admin", password="admin")

	#Create Collection
    conInfo = griddb.ContainerInfo("zstsample",
            [["timestamp", griddb.Type.TIMESTAMP],
            ["motion", griddb.Type.BOOL],
            ["temperature", griddb.Type.FLOAT],
            ["humidity", griddb.Type.FLOAT],
            ["illuminance", griddb.Type.FLOAT],
            ["month", griddb.Type.LONG],
            ["day", griddb.Type.LONG],
            ["dayofweek", griddb.Type.LONG],
            ["hour", griddb.Type.LONG]],
            griddb.ContainerType.TIME_SERIES, True)
    col = gridstore.put_container(conInfo)

    #Change auto commit mode to false
    col.set_auto_commit(False)

    start = int((time.time()-3600*24*30)*1000)
    end = 	int(time.time()*1000)
    cur = start
    state = False
    while cur < end:
        td = datetime.datetime.fromtimestamp(cur/1000)
        month = int(td.strftime("%m"))
        day = int(td.strftime("%d"))
        dayofweek = int(td.strftime("%w"))
        hour = int(td.strftime("%H"))

        if hour == 15:
            temperature = 90 + random.randrange(-5, 10)

        elif hour == 14 or hour == 16:
            temperature = 85 + random.randrange(-5, 5)
        elif hour == 13 or hour == 17:
            temperature = 80 + random.randrange(-5, 5)
        elif hour == 12 or hour == 18:
            temperature = 75 + random.randrange(-5, 5) 
        elif hour == 11 or hour == 19:
            temperature = 70 + random.randrange(-5, 5)
        elif hour == 10 or hour == 20:
            temperature = 65 + random.randrange(-5, 5)
        elif hour == 9 or hour == 21:
            temperature = 60 + random.randrange(-5, 5)
        elif hour == 8 or hour == 22:
            temperature = 55 + random.randrange(-5, 5)
        else:
            temperature = 50 + random.randrange(-5, 5)


        if hour == 16:
            humidity = 80 + random.randrange(-10, 10)
        elif hour == 15:
            humidity = 75 + random.randrange(-10, 10)
        elif hour == 14 or hour == 17:
            humidity = 70 + random.randrange(-10, 10)
        elif hour == 13:
            humidity = 65 + random.randrange(-10, 10)
        else:
            humidity = 60 + random.randrange(-10, 10)


        if hour > 7 and hour <= 19:
            illuminance = 80 + random.randrange(-10, 20)
        elif hour > 19 and hour < 23:
            illuminance = 50 + random.randrange(-20, 20)
        else:
            illuminance = 10 + random.randrange(-10, 20)


        if random.randrange(0, 100) > 98:
            motion = True
        else:
            motion = False

        data = [td, motion, temperature, humidity, illuminance, month, day, dayofweek, hour]
        print(data)
        col.put(data)
        cur = cur + 10*1000
        col.commit()

except griddb.GSException as e:
    for i in range(e.get_error_stack_size()):
        print("[", i, "]")
        print (e.get_error_code(i))
        print(e.get_location(i))
        print(e.get_message(i))
