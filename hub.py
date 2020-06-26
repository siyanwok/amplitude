import datetime as dt
import os
from pytz import timezone
import pytz

today = dt.datetime.now(timezone('Asia/Shanghai')).strftime('%Y%m%d')
yesterday = (dt.datetime.now(timezone('Asia/Shanghai')) + dt.timedelta(days=-1)).strftime('%Y%m%d')

print("Now in China", today)
print("Yesterday in China", yesterday)

#start_time_debug = dt.datetime.fromisoformat('2020-06-26 09:30:00')
end_date = (dt.datetime.now() + dt.timedelta(days=+1))
print("DB QUERY RANGE:")
print("BJ Time:", today.astimezone(timezone('Asia/Shanghai')),"-",end_date.astimezone(timezone('Asia/Shanghai')), "UTC:", today.astimezone(timezone('UTC')),"-",end_date.astimezone(timezone('UTC')))