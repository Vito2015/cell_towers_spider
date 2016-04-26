# coding: utf-8
"""
    dataparse.py
    ~~~~~~~~~~~~

    用于转换 cellfetch.py 收集的 results 数据集格式到指定的标准格式。
     key | lat | lng
"""
import re
import os
import sys
from datetime import datetime
import argparse
import json
from multiprocessing.pool import Pool
# from multiprocessing.dummy import ThreadPool
from pyextend.core import log

log.set_logger(filename=os.path.join(os.path.curdir, './logs/dataparse.log'), with_filehandler=True)


parser = argparse.ArgumentParser(usage='dataparse <options>',
                                 description='cell towers data parser.')
normal_options = parser.add_argument_group('normal options')
normal_options.add_argument('-f', '--file', dest='source_file', type=str, help='source file.')
normal_options.add_argument('--type', dest='type', type=str, default='json',
                            choices=['json', 'csv'], help='source file type.')

pk_options = parser.add_argument_group('cell pk options')
pk_options.add_argument('-g', '--gen', dest='gen_key', action='store_true',
                        default=False, help='generate key from \'mcc, mnc, lac, cid\'.')
pk_options.add_argument('--genFields', dest='gen_key_fields',  action='store',
                        default='{"mcc":"mcc","mnc":"mnc","lac":"lac","cid":"cid"}', type=str)

pk_options.add_argument('-p', '--parse', dest='parse_key', action='store_true',
                        default=False, help='parse \'mcc, mnc, lac, cid\' from \'key\'.')
pk_options.add_argument('--keyField', dest='key', default='key', type=str)

output_options = parser.add_argument_group('output options')
output_options.add_argument('-o', '--out', dest='output', type=str,
                            help='output file')
output_options.add_argument('--out-type', type=str, choices=['json', 'csv'], default='csv',
                            dest='output_type',
                            help='output file type')
output_options.add_argument('--outFields', dest='out_fields', default='key,mcc,mnc,lac,cid', type=str)

args, remaining = parser.parse_known_args(sys.argv)

ENCODE_STR = '%03x%04x%08x%08x'
DECODE_REG = re.compile('^([0-9a-f]{3})([0-9a-f]{4})([0-9a-f]{8})([0-9a-f]{8})$')


_source_file = normal_options.get_default('source_file')
_gen_key = pk_options.get_default('gen_key')
_parse_key = pk_options.get_default('parse_key')
_type = normal_options.get_default('type')
_key_field_name = pk_options.get_default('key')
_output_file = output_options.get_default('output')
_output_type = output_options.get_default('output_type')
_output_fields = str(output_options.get_default('out_fields')).split(',')
_gen_key_fields = json.loads(str(output_options.get_default('gen_key_fields')))


class ParseCellKeyError(AttributeError):
    pass


def cell_pk(**kwargs):
    return ENCODE_STR % (int(kwargs[_gen_key_fields['mcc']]), int(kwargs[_gen_key_fields['mnc']]),
                         int(kwargs[_gen_key_fields['lac']]), int(kwargs[_gen_key_fields['cid']]))


def parse_cell_pk(string):
    try:
        mcc, mnc, lac, cid = DECODE_REG.match(string).groups()
    except AttributeError:
        raise ParseCellKeyError('Wrong cell key format: {0}'.format(string))
    mcc = int(mcc, base=16)
    mnc = int(mnc, base=16)
    lac = int(lac, base=16)
    cid = int(cid, base=16)
    return {'mcc': mcc, 'mnc': mnc, 'lac': lac, 'cid': cid}


def parse_args():
    global _source_file, _parse_key, _gen_key, _type, _key_field_name, \
        _output_file, _output_type, _output_fields, _gen_key_fields

    _source_file = args.source_file
    _gen_key = args.gen_key
    _parse_key = args.parse_key
    _type = args.type
    _key_field_name = args.key
    _output_file = args.output
    _output_type = args.output_type
    _output_fields = args.out_fields.split(',')
    _gen_key_fields = json.loads(str(args.gen_key_fields))

    if not _source_file:
        parser.print_help()
        log.error('--file cannot None.')
        exit(2)

    if _output_file is None:
        _output_file = args.output = os.path.join(os.path.dirname(_source_file), 'results/%s_%s.%s' %
                                                  (os.path.basename(_source_file), datetime.now().strftime('%Y%m%d'),
                                                   _output_type))

    if not _output_file:
        parser.print_help()
        log.error('--out cannot None.')
        exit(2)
    log.info(';'.join(['%s=%s' % (x, getattr(args, x)) for x in args.__dict__]))

    return any([_parse_key, _gen_key])


def data_reader():
    with open(_source_file, mode='r') as f:
        if _type.strip().lower() == 'json':
            log.warning('not implement json type')
            parser.print_help()
            exit(0)
        elif _type.strip().lower() == 'csv':
            import csv
            reader = csv.DictReader(f)
            for line in reader:
                yield line


def data_writer():
    if not os.path.exists(os.path.dirname(_output_file)):
        os.mkdir(os.path.dirname(_output_file))
    f = open(_output_file, 'w')
    if _output_type.strip().lower() == 'csv':
        import csv
        writer = csv.DictWriter(f, fieldnames=_output_fields)
        writer.writeheader()
    elif _output_type.strip().lower() == 'json':
        writer = None
    return f, writer


def parse_key(line):
    keys = parse_cell_pk(line[_key_field_name])
    fields = _output_fields.copy()
    d = dict()
    if _key_field_name in fields:
        d[_key_field_name] = line[_key_field_name]
        del fields[fields.index(_key_field_name)]
    elif 'key' in fields:
        d['key'] = line[_key_field_name]
        del fields[fields.index('key')]
    for field in fields:
        d[field] = keys.get(field)
    return d


def generate_key(line):
    key = cell_pk(**line)
    line['key'] = key
    return line


def main():
    if not parse_args():
        log.warning('use one of \'-g\' or \'-p\'.')
        parser.print_help()
    w_f, writer = data_writer()
    pool = Pool(processes=os.cpu_count())
    # for idx, line in enumerate(data_reader(), start=1):
    #     if _parse_key:
    #         row = parse_key(line)
    #         writer.writerow(row)
    #         w_f.flush()
    #         log.info('No.%d %s ---> %s' % (idx, line[_key_field_name], row))
    if _parse_key:
        for idx, row in enumerate(pool.map(parse_key, data_reader())):
            if _output_type.strip().lower() == 'csv':
                writer.writerow(row)
            elif _output_type.strip().lower() == 'json':
                import json
                jsonstr_row = json.dumps(row)
                w_f.write(jsonstr_row)
                w_f.write('\n')
            w_f.flush()
            # log.info('No.%d %s ---> %s' % (idx, row.get('key') or row.get(_key_field_name), row))
            log.info('No.%d %s' % (idx, row.get('key') or row.get(_key_field_name)))
    if _gen_key:
        for idx, row in enumerate(pool.map(generate_key, data_reader())):
            if _output_type.strip().lower() == 'csv':
                writer.writerow(row)
            elif _output_type.strip().lower() == 'json':
                import json
                jsonstr_row = json.dumps(row)
                w_f.write(jsonstr_row)
                w_f.write('\n')
            w_f.flush()
            log.info('No.%d %s' % (idx, row.get('key') or row.get(_key_field_name)))

    w_f.close()

if __name__ == '__main__':
    main()
