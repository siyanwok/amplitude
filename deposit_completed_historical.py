import hashlib
import json
import logging
import os
import time
from concurrent import futures
from concurrent.futures import as_completed
import datetime as dt

import certifi
import urllib3

from odps import options
from odps import ODPS
from pytz import timezone
import pytz

event_type = "deposit_complete"

# Settings
debug = False
API_KEY = str(os.environ.get("AMPLITUDE_KEY"))
ENDPOINT = 'https://api.amplitude.com/batch'
options.api_proxy = str(os.environ.get("PROXY"))
options.data_proxy = str(os.environ.get("DATA_PROXY"))
accessID = str(os.environ.get("ODPSID"))
accessKey = str(os.environ.get("ODPSKEY"))
odps = ODPS(accessID, accessKey, 'okex_offline', endpoint='http://service.cn-hongkong.maxcompute.aliyun.com/api')

sql = '''
select distinct u.tk_user_id, a.timestamp, a.channel, a.symbol,a.channel_name
from
((select user_id, completed_on as timestamp
,   case
         when channel_id = 0 then 'Wire Transfer'
         when channel_id = 24 then 'ACH'
         when channel_id = 4 then 'Epay'
         when channel_id in (3,27) then 'SEN'
         when channel_id in (9,21) then 'PrimeX'
         else 'other'
         end as channel, symbol, channel_name
from v_com_cash_deposit_order_4_usa_team
where status=3
  and pt=${yesterday}
  and completed_on<to_date("${today}",'yyyymmdd')
  and completed_on>=to_date("${yesterday}",'yyyymmdd'))
union all
(select user_id, created_date as timestamp,  "Token" as channel, symbol, "Token" as channel_name
from asset_ods_okcoin_deposit_transaction a
    inner join out_com_currency_price_new o
      on a.currency_id = o.currency_id and o.pt = to_char(a.created_date, 'yyyymmdd')
where status=2
  and a.pt=${yesterday}
  and created_date<to_date("${today}",'yyyymmdd')
  and created_date>=to_date("${yesterday}",'yyyymmdd')
)) a
join v3_btc_user_uniform_4_usa_team u
  on a.user_id=u.user_id
order by timestamp
limit 100000;

'''


#bj_today = dt.datetime.now(timezone('Asia/Shanghai')).strftime('%Y%m%d')
bj_today = (dt.datetime.now(timezone('Asia/Shanghai')) + dt.timedelta(days=-3)).strftime('%Y%m%d')
bj_yesterday = (dt.datetime.now(timezone('Asia/Shanghai')) + dt.timedelta(days=-5)).strftime('%Y%m%d')


# subclass of ThreadPoolExecutor that provides:
#   - proper exception logging from futures
#   - ability to track futures and get results of all submitted futures
#   - failure on exit if a future throws an exception (when futures are tracked)
class Executor(futures.ThreadPoolExecutor):

    def __init__(self, max_workers):
        super(Executor, self).__init__(max_workers)
        self.track_futures = False
        self.futures = []

    def submit(self, fn, *args, **kwargs):
        def wrapped_fn(args, kwargs):
            return fn(*args, **kwargs)

        future = super(Executor, self).submit(wrapped_fn, args, kwargs)
        if self.track_futures:
            self.futures.append(future)
        return future

    def results(self):
        if not self.track_futures:
            raise Exception('Cannot get results from executor without future tracking')
        return (future.result() for future in as_completed(self.futures))

    def __enter__(self):
        self.track_futures = True
        return super(Executor, self).__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            for future in as_completed(self.futures):
                future.result()
            self.futures = []
            self.shutdown(wait=False)
            return False
        finally:
            super(Executor, self).__exit__(exc_type, exc_val, exc_tb)


def run_with_retry(f, tries, failure_callback=None):
    while True:
        try:
            return f()
        except Exception as e:
            print(e)
            tries -= 1
            if tries <= 0:
                logging.info('[%s] Failed to run %s Encountered %s (0 tries left, giving up)', os.getpid(), f,
                             e.__class__.__name__)
                break
            else:
                if failure_callback:
                    failure_callback()
                logging.info(
                    '[%s] Raised %s, retrying (%s tries left)',
                    os.getpid(), e.__class__.__name__, tries)


def send_req(json_events):
    data = {'api_key': API_KEY, 'events': json_events}

    def do_send():
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        response = http.request('POST', ENDPOINT, body=json.dumps(data).encode('utf-8'))
        response.read()
        response.close()
        if response.status != 200:
            raise Exception('Bad response: ' + str(response.status) + ', body: ' + str(response.data))

    run_with_retry(do_send, tries=10, failure_callback=lambda: time.sleep(10))


def upload(events):
    start = time.time()
    with Executor(max_workers=64) as executor:
        for i in range(0, len(events), 10):
            executor.submit(send_req, events[i:i + 10])
    diff = time.time() - start
    logging.info('uploading %s events took %s', len(events), diff)


def fetch_data_from_db(sql):
    sql = sql.replace('${yesterday}', bj_yesterday)
    sql = sql.replace('${today}', bj_today)

    print('DB QUERY BJ TIME:', bj_today, 'to', bj_yesterday)

    sql_res = odps.execute_sql(sql)

    reader = sql_res.open_reader()

    cur_events = []
    for row in reader:
        print(row)
        user_id = row[0]
        time = int(row[1].timestamp())
        channel = row[2]
        symbol = row[3]
        channel_name = row[4]

        insert_id = hashlib.md5((str(event_type)+str(user_id) + str(time) + str(channel) + str(symbol) + str(channel_name)).encode()).hexdigest()

        cur_events.append(
            {
                'event_type': event_type,
                'user_id': user_id,
                'time': time,
                "event_properties": {
                    'channel': channel,
                    'symbol': symbol,
                    'channel_name': channel_name,
                },
                'insert_id': insert_id
            }
        )

    return cur_events


def main():
    import sys
    cur_events = fetch_data_from_db(sql)
    # print (cur_events)

    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    rootLogger.addHandler(ch)

    if API_KEY == 'YOUR_API_KEY':
        logging.info('Must set API_KEY')
        return

    rownum = 0
    upl_events = []

    for line in cur_events:
        print(line)
        rownum += 1
        event = line
        if (
                'event_type' not in event or
                ('user_id' not in event and 'device_id' not in event)
        ):
            continue
        upl_events.append(event)
        if len(upl_events) >= 1000:
            if not debug:
                logging.info('uploading %s events, row %s', len(upl_events), rownum)
                upload(upl_events)
            print("BATCH OBJECT:")
            print(upl_events)
            upl_events = []
    if cur_events:
        if not debug:
            logging.info('uploading %s events, row %s', len(upl_events), rownum)
            upload(upl_events)
        print("BATCH OBJECT:")
        print(upl_events)


if __name__ == '__main__':
    main()
