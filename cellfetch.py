# coding: utf-8
"""
    cellfetch.py
    ~~~~~~~~~~~~

    cell towers location spider.

    Data From: cellocation.com
    Coord Type: gcj02
    see link <http://baike.baidu.com/link?url=Q_CyKgQIOdKwdF0FOMIuGY2UbrlYtClUrYny73GNM4lcGQuDGw6GaBzU5ja_qti_pqZFRE2mFiyNosqDwLZasK>
    for more details.
    Author: Vito
    Date: 2016-04-20 20:51
"""

import os
import re
import sys
import csv
import json
import time
import argparse
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import URLError
from queue import Queue
from pyextend.core import log
from pyextend.core.itertools import merge

log.set_logger(filename=os.path.join(os.path.curdir, './logs/cellfetch.log'), with_filehandler=True)

PY_VERSION = sys.version_info[0]

_default_outfile = os.path.join(os.path.curdir, 'results/cell_towers_{}.csv'.format(datetime.now().strftime('%Y%m%d')))

parser = argparse.ArgumentParser(usage='cellfetch <options>',
                                 description='a web spider used for fetching cell towers coordinates')

normal_options = parser.add_argument_group('normal options')

normal_options.add_argument('-f', '--file', type=str,
                            help="source csv file with 'mcc','mnc','lac','cid' columns. ")

normal_options.add_argument('--line', type=int, default=-1, help="a integer number; read [--file] begin at [--line].")

normal_options.add_argument('-o', '--out', type=str,
                            default=_default_outfile,
                            help="output file;\
                            result csv file will with 'mcc','mnc','lac','cid','lat','lon' and other columns.\
                            (default '{}')".format(_default_outfile))

normal_options.add_argument('--key', action='store_true',
                            default=False,
                            help="can generate key field at output file ")

normal_options.add_argument('-s', '--sleep-time', type=int, default=1*60,
                            help="a integer number; when api response '403',\
                            cellfetch process will sleep some seconds (default 60s).")

check_options = parser.add_argument_group('check options')

check_options.add_argument('-c', '--check-line', type=int, default=0,
                           help="a integer number; used for checking [-c] line's position at the [--file].")

args, remaining = parser.parse_known_args(sys.argv)


class SpiderCursor(object):
    def __init__(self):
        if not os.path.exists('spider.lock'):
            open('spider.lock', mode='w').close()
        self.f = open('spider.lock', mode='r+')

    def read(self):
        self.f.close()
        self.f = open('spider.lock', mode='r+')

        lines = self.f.readlines()
        lines.extend(['0' for _ in range(2 - len(lines))])
        if PY_VERSION >= 3:
            line_no, pos, *other = lines
        else:
            line_no = lines[0]
            pos = lines[1]
        return int(line_no or 0), int(pos or 0)

    def write(self, ln, pos):
        self.f.seek(0)
        self.f.truncate(0)
        self.f.writelines([str(ln), '\n', str(pos)])  # TODO need clear file body then write
        self.f.flush()

    def close(self):
        self.f.close()


_headers = {
    'User-Agent': 'mozilla/5.0 (compatible; baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
    # 'cookie': cookies_str
}

COORD = 'wgs84'
OUT_PUT = 'json'  # or 'csv' or 'xml

# usual conditions we use 'BASE_URL' , and 'BASE_URL2' for back up
# the 'BASE_URL2' param-> 96 (default) is ASU or dBm
# 坐标类型(wgs84/gcj02/bd09)，默认wgs84
BASE_URL = 'http://api.cellocation.com/cell/?coord=%s&output=%s&mcc={}&mnc={}&lac={}&ci={}' % (COORD, OUT_PUT)
BASE_URL2 = 'http://api.cellocation.com/loc/?coord=%s&output=%s&cl={},{},{},{},96' % (COORD, OUT_PUT)

ERR_CODES = {
    10000: '查询参数错误',
    10001: '无基站数据',
    403: '每日查询超限',
}

ENCODE_STR = '%03x%04x%08x%08x'
DECODE_REG = re.compile('^([0-9a-f]{3})([0-9a-f]{4})([0-9a-f]{8})([0-9a-f]{8})$')

_is_gen_key = False
_sleep_time = 1 * 60
_source_file = None
# '/home/mi/tmpdata/mongodb/cell_key_china.csv'
#  '/home/mi/tmpdata/mongodb/cell_key_china.csv' test.csv
_target_file = os.path.join(os.path.curdir,  # '../',
                            'results/cell_towers_{}.csv'.format(datetime.now().strftime('%Y%m%d')))

_cursor = SpiderCursor()


def spider(cell_code, headers=_headers, **kwargs):

    def gen_url(code):
        def get_code(name):
            return code[name] or ''
        url_fmt = kwargs.get('url') or BASE_URL
        try:
            return url_fmt % (get_code('mcc'), get_code('mnc'), get_code('lac'), get_code('cid'))
        except TypeError:
            return url_fmt.format(get_code('mcc'), get_code('mnc'), get_code('lac'), get_code('cid'))

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

        # if 'radio' not in data.keys():
        #     data['radio'] = ''

        try:
            errcode = data.pop('errcode')
        except KeyError:
            return data
        if errcode != 0 and errcode in ERR_CODES.keys():
            # print('\n!!!!!!DataError: ', 'url={}, errcode={}, errmsg={}'.format(url, errcode, ERR_CODES[errcode]))
            log.error('!!!!!!DataError: url={}, errcode={}, errmsg={}'.format(url, errcode, ERR_CODES[errcode]))
            return None
        return data

    # def merge(result, code):
    #     return dict(result, **code)

    url = gen_url(cell_code)
    req = Request(url, headers=headers)
    try:
        response = urlopen(req)
    except URLError as e:
        if hasattr(e, 'code'):
            if e.code in ERR_CODES.keys():
                log.error('!!!!!!RequestError: status={}, errmsg={}'.format(e.code, ERR_CODES[e.code]))
                return e.code
            else:
                log.error('!!!!!!RequestError: The server couldn\'t fulfill the request. status={}'.format(e.code))
        elif hasattr(e, 'reason'):
            log.error('We failed to reach a server. reason={}'.format(e.reason))
    else:
        data = response.read().decode("utf8")
        data = verify_data(url, data)
        return merge(data, cell_code) if data is not None else None


class ParseCellKeyError(AttributeError):
    pass


def cell_pk(**kwargs):
    return ENCODE_STR % (int(kwargs['mcc']), int(kwargs['mnc']), int(kwargs['lac']), int(kwargs['cid']))


def parse_cell_pk(string):
    try:
        mcc, mnc, lac, cid = DECODE_REG.match(string).groups()
    except AttributeError:
        raise ParseCellKeyError('Wrong cell key format: {0}'.format(string))
    mcc = int(mcc, base=16)
    mnc = int(mnc, base=16)
    lac = int(lac, base=16)
    cid = int(cid, base=16)
    return mcc, mnc, lac, cid


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
    ln, pos = _cursor.read()
    tmp_ln = ln

    with open(filename, mode='r+') as f:
        if pos > 0:
            f.seek(pos)
        log.info('begin line_no=%s, position=%s' % (ln, pos))

        next_row_len = len(f.readline())
        f.seek(pos)
        reader = csv.DictReader(f, fieldnames=['mcc', 'mnc', 'lac', 'cid'])
        for line in reader:
            tmp_ln = (ln-1) + reader.line_num
            if tmp_ln > 0:
                d = dict()
                d['mcc'] = line['mcc']
                d['mnc'] = line['mnc']
                d['lac'] = line['lac']
                d['cid'] = line['cid']
                yield tmp_ln, d
            pos += next_row_len
            tmp_ln += 1
            log.info('next line=%s, position=%s' % (tmp_ln, pos))
            _cursor.write(tmp_ln, pos)
            next_row_len = len(f.readline())
            f.seek(pos)
        log.info('end pos %s' % f.tell())


def csv_writer(filename):
    if not os.path.exists(os.path.dirname(filename)):
        os.mkdir(os.path.dirname(filename))
    f = open(filename, mode='a+')
    writer = csv.DictWriter(f, fieldnames=['key', 'mcc', 'mnc', 'lac', 'cid', 'lat', 'lon', 'address', 'radius'])
    if f.tell() == 0:
        writer.writeheader()
    return f, writer


def run_fetching():
    log.info('source file: %s' % _source_file)
    log.info('output file: %s' % _target_file)
    log.info('sleep time: %ss' % _sleep_time)

    q = Queue()
    reader = csv_reader(_source_file)
    f_w, writer = csv_writer(_target_file)

    def write_data(data, is_gen_key):
        if is_gen_key:
            key = cell_pk(**data)
            data['key'] = key
        log.debug(data)
        writer.writerow(data)
        f_w.flush()

    for idx, code in reader:
        # print(code)
        q.put(code)
        # print('qsize={}'.format(q.qsize()))
        while q.qsize() > 0:
            current_code = q.get()
            log.info('>>>>>>NO.{} current_code={}'.format(idx, current_code))
            d = spider(current_code)
            if isinstance(d, dict):
                write_data(d, is_gen_key=_is_gen_key)
                continue
            elif d == 403 and d in ERR_CODES.keys():
                # try backup url
                d = spider(current_code, url=BASE_URL2)
                if isinstance(d, dict):
                    write_data(d, is_gen_key=_is_gen_key)
                    continue
                q.put(current_code)
                log.warning('sleep {}s'.format(_sleep_time))
                time.sleep(_sleep_time)
    log.info('Done')
    f_w.close()


def get_position(line):
    """Returns a integer; the position of source file at specified line"""
    ln = 0
    with open(_source_file, mode='r') as f:
        while ln < line:
            f.readline()  # if used next(f) then cannot tell() it
            ln += 1
        return f.tell()


def run_checking(check_line):
    log.debug('>>>>>>checking source file: %s' % _source_file)
    log.debug('>>>>>>checking line: %s' % check_line)
    log.info('line: %s ---> position: %s' % (check_line, get_position(check_line)))


def run():
    # print(args)
    global _source_file, _target_file, _sleep_time, _is_gen_key

    _source_file = args.file
    _target_file = args.out
    _sleep_time = args.sleep_time

    begin_line = args.line
    check_line = args.check_line

    _is_gen_key = args.key

    if _source_file is None and check_line == 0:
        parser.print_help()
        exit(0)

    if check_line != 0:
        if check_line < 0:
            parser.print_help()
            parser.error("[--check-line] need > 0.")  # exit with code 2
        run_checking(check_line)
    else:
        if begin_line >= 0:
            pos = get_position(begin_line)
            _cursor.write(begin_line, pos)
        elif begin_line != -1:
            parser.print_help()
            parser.error("[--line] need >= 0.")  # exit with code 2

        run_fetching()

if __name__ == '__main__':
    run()
