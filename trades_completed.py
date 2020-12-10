import hashlib
import json
import logging
import os
import time
from concurrent import futures
from concurrent.futures import as_completed
import datetime as dt
import os
import certifi
import urllib3

from odps import options
from odps import ODPS

from pytz import timezone
import pytz


event_type = "trades_completed"

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
with widget_order as
(select distinct id, 'Quicktrade' as order_type
  from dwd_transaction_okcoin_spot_order_finish
where pt='${yesterday}' and source in (16,17)),

trade as
(select a.user_id, a.currency_id, a.size,a.fee, a.pt, b.order_type, a.create_time
from
((select user_id, currency_id, size, fee, pt, refer_id, create_time
from v3_user_bill_com_4_usa_team
where type in (7,8) and pt='${yesterday}')
union all
(select user_id, currency_id, size, fee, pt, refer_id, create_time
from v3_user_margin_bill_com_4_usa_team
where type in (7,8) and pt='${yesterday}')) a
left join widget_order b on a.refer_id=b.id)

select a.tk_user_id, b.create_time as timestamp,
       case when order_type is null then 'Other' else order_type end as order_type,
       sum(abs(size)*c.price)/2, sum(fee*c.price) as fee
from v3_btc_user_uniform_4_usa_team a join trade b
on a.user_id=b.user_id
join out_com_currency_price_new c
on b.currency_id=c.currency_id and b.pt=c.pt
group by a.tk_user_id, b.create_time, order_type;
'''

bj_today = dt.datetime.now(timezone('Asia/Shanghai')).strftime('%Y%m%d')
bj_yesterday = (dt.datetime.now(timezone('Asia/Shanghai')) + dt.timedelta(days=-1)).strftime('%Y%m%d')

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
    sql = sql.replace('${start_date}', bj_today)
    sql = sql.replace('${yesterday}', bj_yesterday)
    print('DB QUERY BJ TIME:', bj_yesterday, 'to', bj_today)

    sql_res = odps.execute_sql(sql)

    reader = sql_res.open_reader()

    cur_events = []
    for row in reader:
        user_id = row[0]
        time = int(row[1].timestamp())
        order_source = row[2]
        value = row[3]
        revenue = row[4]

        insert_id = hashlib.md5((str(event_type)+str(user_id) + str(time) + str(revenue) + str(order_source) + str(value)).encode()).hexdigest()

        cur_events.append(
            {
                'event_type': event_type,
                'user_id': user_id,
                'time': time,
                "event_properties": {
                    'order_source': order_source,
                    'value': value,
                    'revenue': -revenue
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
