# coding: utf-8

import json
import csv
# from cell_towers_spider import AsyncSpider, DataStoreMixin

from datetime import timedelta, datetime
from tornado import httpclient, gen, ioloop, queues
import traceback
import os


class AsyncSpider(object):
    """A base class of async spider."""
    def __init__(self, urls=list(), concurrency=10, **kwargs):
        urls.reverse()
        self.urls = urls
        self.concurrency = concurrency
        self._q = queues.Queue()
        self._fetching = set()
        self._fetched = set()

    def fetch(self, url, **kwargs):
        fetch = getattr(httpclient.AsyncHTTPClient(), 'fetch')
        return fetch(url, **kwargs)

    def handle_html(self, url, html):
        """handle html page"""
        print('AsyncSpider', url)

    def handle_response(self, url, response):
        if response.code == 200:
            self.handle_html(url, response.body)

        elif response.code == 599:    # retry
            self._fetching.remove(url)
            self._q.put(url)

    @gen.coroutine
    def get_page(self, url):
        try:
            response = yield self.fetch(url)
            print('######fetched %s' % url)
        except Exception as e:
            print('Exception: %s %s' % (e, url))
            raise gen.Return(e)
        raise gen.Return(response)

    @gen.coroutine
    def _run(self):
        @gen.coroutine
        def fetch_url():
            current_url = yield self._q.get()
            try:
                if current_url in self._fetching:
                    return

                print('fetching****** %s' % current_url)
                self._fetching.add(current_url)

                response = yield self.get_page(current_url)
                self.handle_response(current_url, response)    # handle reponse

                self._fetched.add(current_url)

                for i in range(self.concurrency):
                    if self.urls:
                        yield self._q.put(self.urls.pop())

            finally:
                self._q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        # comment test
        # self._q.put(self.urls.pop())    # add first url
        #TODO 需要将 传入urls的动作放到方法中， 解决构造函数不传递urls时 run 空 直接退出的问题。
        # Start workers, then wait for the work queue to be empty.
        for _ in range(self.concurrency):
            worker()

        yield self._q.join(timeout=timedelta(seconds=300000))
        assert self._fetching == self._fetched
        self.done()

    def done(self):
        print('Done!')

    def run(self):
        io_loop = ioloop.PollIOLoop.current()
        io_loop.run_sync(self._run)

    def put(self, urls):
        self.urls.extend(urls)


class DataStoreMixin(object):

    def __init__(self):
        self.fname = os.path.join(os.path.curdir,
                                  '../',
                                  'results/cell_towers_{}.csv'.format(datetime.now().strftime('%Y%m%d%H%M%S')))
        self.file = open(self.fname, mode='a', encoding='utf-8')

    def store(self, data):
        if not data:
            return
        if not self.file:
            self.file = open(self.fname, mode='a', encoding='utf-8')

        self.file.write(str(data))
        self.file.flush()

    def close(self):
        self.file.close()
        self.file = None


class CellocationComSpiderUrlMixin(object):
    OUT_PUT = 'json'  # or 'csv'
    BASE_URL = 'http://www.cellocation.com/cell/?coord=gcj02&output=%s&mcc={}&mnc={}&lac={}&ci={}' % OUT_PUT
    ERR_CODES = {
        10000: '查询参数错误',
        10001: '无基站数据',
    }

    def handle_url(self, cell_codes):
        def gen_url(cell_code):
            return self.BASE_URL.format(cell_code['mcc'], cell_code['mnc'], cell_code['lac'], cell_code['cid'])
        return list(map(gen_url, cell_codes))

    def handle_errcode(self, url, s):
        if self.OUT_PUT == 'json':
            data = json.loads(s, encoding='utf-8')
        elif self.OUT_PUT == 'csv':
            result = s.strip().split(',')
            data = dict()
            data['errcode'] = int(result[0])
            data['lat'] = result[1]
            data['lon'] = result[2]
            data['radius'] = result[3]
            data['addr'] = result[4]

        errcode = data.pop('errcode')
        if errcode != 0 and errcode in self.ERR_CODES.keys():
            print('!!!!!!DataError: ', 'url={}, errcode={}, errmsg={}'
                  .format(url, errcode, self.ERR_CODES[errcode]))
            return None
        return data


class CellocationComSpider(AsyncSpider, CellocationComSpiderUrlMixin, DataStoreMixin):  # DataStoreMixin
    """URL: http://www.cellocation.com/cell/?coord=gcj02&output=csv&mcc=460&mnc=0&lac=18755&ci=36293"""
    def __init__(self, concurrency=1, **kwargs):

        super(CellocationComSpider, self).__init__(concurrency=concurrency, kwargs=kwargs)
        DataStoreMixin.__init__(self)

    def put(self, cell_codes):
        urls = self.handle_url(cell_codes)
        super(CellocationComSpider, self).put(urls)

    def handle_response(self, url, response):
        super(CellocationComSpider, self).handle_response(url, response)
        if response.code == 403:
            self._fetching.remove(url)
            self._q.put(url)
            self.overrun()

    def overrun(self):
        print('CellocationComSpider', '每日查询超限。')
        # TODO 这里需要解除限制屏蔽。
        # exit(0)

    def fetch(self, url, **kwargs):
        """重写父类fetch方法可以添加cookies，headers，timeout等信息"""
        headers = {
            'User-Agent': 'mozilla/5.0 (compatible; baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
            # 'cookie': cookies_str
        }
        return super(CellocationComSpider, self).fetch(
            url, headers=headers, request_timeout=1
        )

    def handle_html(self, url, html):
        # print(url, html)
        body = html.decode(encoding='utf-8')
        data = self.handle_errcode(url, body)
        print(data)
        self.store(data)

    def done(self):
        self.close()
        print('Done, closed.')


class Manager(object):

    def load_csv(self, filename):
        job_block = []
        count = 0
        with open(filename, mode='r') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=['mcc', 'mnc', 'lac', 'cid'])
            for line in reader:
                if count >= 2:
                    return
                if len(job_block) < 2:
                    job_block.append(line)
                else:
                    yield job_block
                    count += 1
                    job_block.clear()


def main():

    cell_codes = [
        {"cid": 5465, "mcc": 460, "lac": 2, "mnc": 0},
        {"cid": 4198, "mcc": 460, "lac": 12, "mnc": 0},
        {"cid": 12175, "mcc": 460, "lac": 12, "mnc": 0},
        {"cid": 10, "mcc": 460, "lac": 17, "mnc": 0},
        {"cid": 8, "mcc": 460, "lac": 18, "mnc": 0},
        {"cid": 108186371, "mcc": 460, "lac": 23, "mnc": 0},
    ]
    # spider = CellocationComSpider(cell_codes=cell_codes)
    # spider.run()


if __name__ == '__main__':
    # main()
    m = Manager()
    spider = CellocationComSpider(concurrency=10)
    spider.run()
    for d in m.load_csv('/home/mi/tmpdata/mongodb/cell_key_china.csv'):
        spider.put(d)
