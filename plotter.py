import sqlite3
from datetime import datetime, date
import dateutil.parser
import matplotlib.pyplot as plt
import numpy as np
import pytz
import six
from github_traffic_tracker import create


utc = pytz.UTC
tzinfo = datetime.now().tzinfo


def timestamp_parser(val):
    return dateutil.parser.parse(val, tzinfo).replace(tzinfo=utc)


# create / open DB
sqlite3.register_adapter(datetime, timestamp_parser)
sqlite3.register_adapter(date, timestamp_parser)
db = sqlite3.connect('traffic.db')
c = db.cursor()

# create schema
create(c)

# get clone data
c.execute('SELECT time, unique_count from CLONES')

dates = {}
for row in c:
    date = timestamp_parser(row[0])
    dates[date] = row[1]

# get sorted
dates, vals = zip(*sorted(six.iteritems(dates), key=lambda x: x[0]))
vals = np.cumsum(vals)
plt.plot(dates, vals)
plt.ylabel('Total pyJac Clones')
plt.xlabel('Date')
plt.show()


# get clone data
c.execute('SELECT time, unique_views from TRAFFIC')

dates = {}
for row in c:
    date = timestamp_parser(row[0])
    dates[date] = row[1]

# get sorted
dates, vals = zip(*sorted(six.iteritems(dates), key=lambda x: x[0]))
vals = np.cumsum(vals)
plt.plot(dates, vals)
plt.ylabel('Total pyJac Views')
plt.xlabel('Date')
plt.show()
