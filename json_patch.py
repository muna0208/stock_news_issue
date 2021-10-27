#!/usr/bin/env python3
# -*- coding: utf8 -*-


'''
- 기존 jsonlib 모듈 대신에 built-in json 모듈을 사용한 버젼
- 더불어 python2 모듈을 python3 모듈로 변환 (python3 에서는 str 타입은 기본적으로 utf8 인코딩 사용)
- 장점: 몇가지 출력 옵션을 선택 가능 (partial_indent, auto_indent 등)
- 단점: 기존 jsonlib 모듈에 비해 처리 속도가 느리며, 탭문자로 indenting 할 수 없음
'''


import json
import decimal
import re
from datetime import datetime


###########################################################################################################
# public function
###########################################################################################################
def print_json(json_obj, ind='\t', max_indent='auto'):
    print(dump_json(json_obj, ind=ind, max_indent=max_indent))


def load_json(json_str):
    '''Build object from json string'''

    try:
        json_obj = json.loads(json_str)
    except:
        json_obj = json.loads(_remove_invalid_escape(json_str))

    return json_obj


def dump_json(json_obj, ind=None, max_indent=0):
    '''Build json string from json object'''

    if isinstance(ind, int):
        if (ind <= 0):
            ind = None
    elif isinstance(ind, str):
        if ind == '\t':
            ind = 4
        else:
            ind = len(ind)
    else:
        ind = None

    if (ind == None) or (max_indent == 0):
        return _basic_dump(json_obj, ind)

    if max_indent == 'auto':
        return _auto_indent(json_obj, ind)

    return _partial_indent(json_obj, ind, max_indent)


###########################################################################################################
# private function/class
###########################################################################################################


class PatchedJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return datetime.strftime(obj, '%Y-%m-%d %H:%M:%S')
        if callable(obj):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def _basic_dump(json_obj, ind=None):
    '''Build json string from basic object'''

    return json.dumps(json_obj, ensure_ascii=False, sort_keys=True, cls=PatchedJSONEncoder, indent=ind)


def _need_indent(json_obj):
    '''Check whether json_obj is needed to indent'''

    return isinstance(json_obj, (list, tuple, dict)) and len(json_obj) > 0


def _partial_indent(json_obj, ind=4, max_indent=2, depth=0):
    '''Build json string from json object with partial indentation'''

    if (depth >= max_indent) or not _need_indent(json_obj):
        return _basic_dump(json_obj)

    ind_str1 = '\n' + ' ' * ind * depth
    ind_str2 = '\n' + ' ' * ind * (depth+1)

    if isinstance(json_obj, dict):
        children = map(lambda k: _basic_dump(k)+': '+_partial_indent(json_obj[k], ind, max_indent, depth+1), sorted(json_obj))
        return ''.join(('{', ind_str2, (','+ind_str2).join(children), ind_str1, '}'))
    else: # list, tuple
        children = map(lambda x: _partial_indent(x, ind, max_indent, depth+1), json_obj)
        return ''.join(('[', ind_str2, (','+ind_str2).join(children), ind_str1, ']'))


def _auto_indent(json_obj, ind=4, depth=0):
    '''Build json string from json object with smart indentation'''

    def need_auto_indent(json_obj):
        if not _need_indent(json_obj):
            return False
        if isinstance(json_obj, dict):
            return any(map(_need_indent, json_obj.values()))
        return any(map(_need_indent, json_obj))

    if not need_auto_indent(json_obj):
        return _basic_dump(json_obj)

    ind_str1 = '\n' + ' ' * ind * depth
    ind_str2 = '\n' + ' ' * ind * (depth+1)
    comma_str = ','+ind_str2

    if isinstance(json_obj, dict):
        children = map(lambda k: _basic_dump(k)+': '+_auto_indent(json_obj[k], ind, depth+1), sorted(json_obj))
        return ''.join(('{', ind_str2, comma_str.join(children), ind_str1, '}'))
    else: # list, tuple
        children = map(lambda x: _auto_indent(x, ind, depth+1), json_obj)
        return ''.join(('[', ind_str2, comma_str.join(children), ind_str1, ']'))


def _remove_invalid_escape(text):

    text = text.replace('\b', '')

    while True:
        #m = re.search(r'[^\\](\\)[^\\"/bfnrtu]', text)
        m = re.search(r'[^\\](?:\\\\)*(\\)[^\\"/bfnrtu]', text)
        if not m or not m.groups():
            break
        text = text[:m.start(1)]+'\\\\'+text[m.end(1):]

    return text


def _test():

    sys.stdout.write(json.dumps('가', ensure_ascii=False)+'\n')
    sys.stdout.write(json.dumps(u'가', ensure_ascii=False)+'\n')
    sys.stdout.write(json.dumps({'가': '나'}, ensure_ascii=False)+'\n')

    sys.stdout.write(json.dumps(decimal.Decimal(10), ensure_ascii=False)+'\n')
    sys.stdout.write(json.dumps(decimal.Decimal(12345678901234567890), ensure_ascii=False)+'\n')
    sys.stdout.write(json.dumps(decimal.Decimal(12345678901234567890.1234567890), ensure_ascii=False)+'\n')

    print(type(json.loads("10")))
    print(type(json.loads("12345678901234567890")))
    print(type(json.loads("12345678901234567890.1234567890")))

    sys.stdout.write(json.dumps(json.loads("10"))+'\n')
    sys.stdout.write(json.dumps(json.loads("12345678901234567890"))+'\n')
    sys.stdout.write(json.dumps(json.loads("12345678901234567890.1234567890"))+'\n')
    sys.stdout.write(json.dumps(float(json.loads("12345678901234567890.1234567890")))+'\n')


###########################################################################################################
# main
###########################################################################################################


def _parse_params(argv):
    '''parse indent_size, max_depth, per_line'''

    if len(argv) == 1:
        return 4, 'auto', 'auto'

    indent_size = int(argv[1])

    max_depth = argv[2]
    if max_depth != 'auto':
        max_depth = int(max_depth)

    per_line = (argv[3] == 'line')

    return indent_size, max_depth, per_line


if __name__ == '__main__':

    import sys
    import logging
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

    #_test()

    usage = '''
        usage1: %s indent_size max_depth per_line
            - indent_size: 몇 칸씩 들여쓸지
            - max_depth:   상위 몇 depth까지만 들여쓸지
                           특이값:  "auto" (차일드가 복잡한 경우에만 들여씀)
            - per_line:    stdin 에서의 인풋을 라인단위로 변환할지, 전체를 하나로 변환할지
                           "line" or "file" or "auto"(line 단위 파싱 시도후 실패시 파일 단위 파싱)
        usage2: %s
            ==> indent_size: 4, max_depth: auto, per_line: auto
    ''' % (sys.argv[0], sys.argv[0])

    try:
        indent_size, max_depth, per_line = _parse_params(sys.argv)
    except:
        sys.stderr.write(usage)
        sys.exit(1)

    if per_line == True:
        for line in sys.stdin:
            print(dump_json(load_json(line), indent_size, max_depth))

    elif per_line == False:
        print(dump_json(load_json(sys.stdin.read()), indent_size, max_depth))

    else: # line 단위 파싱 시도후 실패시 파일 단위 파싱
        lines = tuple(map(lambda line: line, sys.stdin))
        try:
            logging.debug('trying to parse per_line')
            print(dump_json(load_json(lines[0]), indent_size, max_depth))
        except:
            logging.debug('failed to parse per_line')
            logging.debug('trying to parse per_file')
            print(dump_json(load_json(' '.join(lines)), indent_size, max_depth))
            sys.exit(0)

        for line in lines[1:]:
            print(dump_json(load_json(line), indent_size, max_depth))
        logging.debug('completed parsing per_line')
