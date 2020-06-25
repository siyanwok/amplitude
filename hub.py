import datetime as dt
import os

#epoch = dt.datetime.utcfromtimestamp(0)

#def unix_time_millis(dt):
 #   return (dt - epoch).total_seconds() * 1000.0



#time = unix_time_millis(dt.datetime.now())
#print(time)
#print(int(time))
#start_date = (dt.datetime.now()).strftime('%Y%m%d')
#end_date = (dt.datetime.now() + dt.timedelta(days=-2)).strftime('%Y%m%d')
#print('Quering DB for data from', end_date, 'to', start_date)


print("This is my key: " + str(os.environ.get("test_key")))
API_KEY = str(os.environ.get("AMPLITUDE_KEY"))
ENDPOINT = 'https://api.amplitude.com/batch'
accessID = str(os.environ.get("ODPSID"))
accessKey = str(os.environ.get("ODPSKEY"))
test = str(os.environ.get("PROXY"))
print(API_KEY, accessID, test)