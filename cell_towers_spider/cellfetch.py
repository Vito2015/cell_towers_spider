# coding: utf-8

import csv
import json
import time
from datetime import datetime
import os
import sys
from urllib.request import Request, urlopen
from urllib.error import URLError
from queue import Queue


class SpiderCursor(object):
    def __init__(self):
        if not os.path.exists('spider.lock'):
            open('spider.lock', mode='w').close()
        self.f = open('spider.lock', mode='r+')

    def read(self):
        return int(self.f.readline() or 0)

    def write(self, offset):
        self.f.seek(0)
        self.f.write(str(offset))
        self.f.flush()

    def close(self):
        self.f.close()


_headers = {
    'User-Agent': 'mozilla/5.0 (compatible; baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
    # 'cookie': cookies_str
}

OUT_PUT = 'json'  # or 'csv'
BASE_URL = 'http://www.cellocation.com/cell/?coord=gcj02&output=%s&mcc={}&mnc={}&lac={}&ci={}' % OUT_PUT
ERR_CODES = {
    10000: '查询参数错误',
    10001: '无基站数据',
    403: '每日查询超限',
}

_sleep_time = 1 * 60
_source_file = '/home/mi/tmpdata/mongodb/test.csv'  # '/home/mi/tmpdata/mongodb/cell_key_china.csv'
_target_file = os.path.join(os.path.curdir,  # '../',
                            'results/cell_towers_{}.csv'.format(datetime.now().strftime('%Y%m%d')))

_cursor = SpiderCursor()


def spider(cell_code, headers=_headers):

    def gen_url(code):
        return BASE_URL.format(code['mcc'], code['mnc'], code['lac'], code['cid'])

    def verify_data(url, s):
        if OUT_PUT == 'json':
            data = json.loads(s, encoding='utf-8')
        elif OUT_PUT == 'csv':
            result = s.strip().split(',')
            data = dict()
            data['errcode'] = int(result[0])
            data['lat'] = result[1]
            data['lon'] = result[2]
            data['radius'] = result[3]
            data['address'] = result[4]

        errcode = data.pop('errcode')
        if errcode != 0 and errcode in ERR_CODES.keys():
            print('!!!!!!DataError: ', 'url={}, errcode={}, errmsg={}'
                  .format(url, errcode, ERR_CODES[errcode]))
            return None
        return data

    def merge(result, code):
        return dict(result, **code)

    url = gen_url(cell_code)
    req = Request(url, headers=headers)
    try:
        response = urlopen(req)
    except URLError as e:
        if hasattr(e, 'reason'):
            print('We failed to reach a server. reason={}'.format(e.reason))
        elif hasattr(e, 'code'):
            if e.code in ERR_CODES.keys():
                print('!!!!!!RequestError: status={}, errmsg={}'.format(e.code, ERR_CODES[e.code]))
                return e.code
            else:
                print('!!!!!!RequestError: The server couldn\'t fulfill the request. status={}'.format(e.code))
    else:
        data = response.read().decode("utf8")
        data = verify_data(url, data)
        return merge(data, cell_code) if data is not None else None


def test():
    cell_codes = [
        {"cid": 5465, "mcc": 460, "lac": 2, "mnc": 0},
        {"cid": 4198, "mcc": 460, "lac": 12, "mnc": 0},
        {"cid": 12175, "mcc": 460, "lac": 12, "mnc": 0},
        {"cid": 10, "mcc": 460, "lac": 17, "mnc": 0},
        {"cid": 8, "mcc": 460, "lac": 18, "mnc": 0},
        {"cid": 108186371, "mcc": 460, "lac": 23, "mnc": 0},
    ]

    q = Queue()

    for code in cell_codes:
        q.put(code)
        # print('qsize={}'.format(q.qsize()))
        while q.qsize() > 0:
            current_code = q.get()
            print('>>>>>>current_code={}'.format(current_code))
            d = spider(current_code)
            if isinstance(d, dict):
                print(d)
            elif d == 403 and d in ERR_CODES.keys():
                q.put(current_code)
                print('sleep {}s'.format(_sleep_time))
                time.sleep(_sleep_time)


def csv_reader(filename):
    pos = _cursor.read()
    with open(filename, mode='r+') as f:
        if pos > 0:
            f.seek(pos)
        print('begin pos %s' % pos)
        reader = csv.DictReader(f, fieldnames=['mcc', 'mnc', 'lac', 'cid'])
        for line in reader:
            # if reader.line_num == 1:
            #     pos += len(''.join(line.values()))
            #     print('current pos %s' % pos)
            #     continue
            yield reader.line_num, line
            pos += len(','.join(line.values()))+1  # +1 ,because has \n
            print('current pos %s' % pos)
            _cursor.write(pos)
        _cursor.write(f.tell())
        print('end pos %s' % f.tell())


def csv_writer(filename):
    if not os.path.exists(os.path.dirname(filename)):
        os.mkdir(os.path.dirname(filename))
    f = open(filename, mode='a+')
    writer = csv.DictWriter(f, fieldnames=['mcc', 'mnc', 'lac', 'cid', 'lat', 'lon', 'address', 'radius'])
    if f.tell() == 0:
        writer.writeheader()
    return f, writer


def main():
    q = Queue()
    reader = csv_reader(_source_file)
    f_w, writer = csv_writer(_target_file)
    for idx, code in reader:
        # print(code)
        q.put(code)
        # print('qsize={}'.format(q.qsize()))
        while q.qsize() > 0:
            current_code = q.get()
            print('>>>>>>NO.{} current_code={}'.format(idx, current_code))
            d = spider(current_code)
            if isinstance(d, dict):
                print(d)
                writer.writerow(d)
                f_w.flush()
            elif d == 403 and d in ERR_CODES.keys():
                q.put(current_code)
                print('sleep {}s'.format(_sleep_time))
                time.sleep(_sleep_time)
    print('Done')
    f_w.close()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        _source_file = sys.argv[1]
    print('source file: %s' % _source_file)
    main()
