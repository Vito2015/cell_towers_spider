# coding: utf-8
"""
    dataparse.py
    ~~~~~~~~~~~~

    用于转换 cellfetch.py 收集的 results 数据集格式到指定的标准格式。
     key | lat | lng
"""
import re
import os
import argparse
from pyextend.core import log

log.set_logger(filename=os.path.join(os.path.curdir, './logs/dataparse.log'), with_filehandler=True)

parser = argparse.ArgumentParser(usage='dataparse <options>',
                                 description='cell towers data parser.')
# TODO need add some commands.


ENCODE_STR = '%03x%04x%08x%08x'
DECODE_REG = re.compile('^([0-9a-f]{3})([0-9a-f]{4})([0-9a-f]{8})([0-9a-f]{8})$')


class ParseCellKeyError(AttributeError):
    pass


def cell_pk(**kwargs):
    return ENCODE_STR % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])


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

# TODO 用于转换 cellfetch.py 收集的 results 数据集格式到指定的标准格式。
