# text_mining_utils.py
# **2019-2021 [FinanceData.KR]()**

import re
import requests
import pandas as pd
from IPython.display import display, HTML 

def contains_text(io, expr):
    '''
    text: 대상 텍스트 (혹은 문자열 리스트)
    expr: 문자열 (단어의 포함 여부 표현식: "내재 +포함 -제외")
    '''
    # expr 문자열 파싱 ("내재 +포함 -제외")
    tokens = expr.split()

    includes, excludes, contains = [], [], []
    for t in tokens:
        if t.startswith('+'):
            includes.append(t[1:])
        elif t.startswith('-'):
            excludes.append(t[1:])
        else:
            contains.append(t)

    # 내재(contain), 포함(include), 제외(exclude) 정규식 연산
    re_con = re.compile('|'.join([c for c in contains]))
    re_exc = re.compile('^('  + ''.join(['(?!%s)' % e for e in excludes]) + '.)*$' if excludes else '')
    re_inc = re.compile(''.join(['(?=.*%s)' % i for i in includes]))

    if type(io) is str:
        text = io
        text = re.sub('\n|\r', ' ', text)
        b_con = bool(re_con.search(text))
        b_exc = bool(re_exc.search(text))
        b_inc = bool(re_inc.search(text))
        return b_con and b_exc and b_inc
    elif type(io) is list:
        result = []
        for text in io:
            b_con = bool(re_con.search(text))
            b_exc = bool(re_exc.search(text))
            b_inc = bool(re_inc.search(text))
            result.append(b_con and b_exc and b_inc)
        return result
    return False


def extract_table(io, tab_match, row_match=0, col_match=-1, encoding=None, verbose=False):
    '''
    * io: 대상 페이지 URL, 혹은 HTML 텍스트 문자열
    * tab_match: 테이블 매칭 문자열
    * row_match: 로우 매칭 문자열 (기본값: 0, 첫 행)
    * col_match: 컬럼 매칭 문자열 (기본값: -1, 가장 오른쪽 컬럼)
    * verbose: 과정 상세 출력 (기본값:False)
    '''
    result = ''

    # 1) 테이블(table) 식별하기
    the_table = None
    for df in pd.read_html(io, encoding=encoding):
        if contains_text(''.join(df.to_string().split()), tab_match):
            the_table = df
            break
    if the_table is None:
        display(HTML(f'table not found for `{tab_match}`')) if verbose else ''
        return ''

    display(HTML(f'<h2>Table found</h2>{the_table.to_html()}<hr>')) if verbose else ''
    
    # 2) 로우(row) 식별하기 (ix: int, row: pandas.Series)
    the_row = None
    if type(row_match) is int: # 정수를 지정하면, 지정한 행
        the_row = the_table.iloc[row_match] if len(the_table) > row_match else None
    elif type(row_match) is str: # 문자열을 지정하면 매칭된 행
        for ix, row in the_table.iterrows():
            if contains_text(''.join(str(row).split()), row_match):
                the_row = row
                break
    if the_row is None:
        display(HTML(f'Unsupprted `row_match`')) if verbose else ''
        return ''
    else:
        display(HTML(f'<h2>Row found</h2>{the_row.values}<hr>')) if verbose else ''

    # 3) 컬럼(column) 식별하기
    result = ''
    if type(col_match) is int: # 정수를 지정하면, 지정한 컬럼값
        result = the_row.iloc[col_match] if the_row is not None else ''
    elif type(col_match) is str: # 문자열을 지정하면 베스트 매칭된 컬럼값
        for col_key in the_row.keys():
            if contains_text(''.join(str(col_key).split()), col_match):
                the_col = col_key
                break
        result = the_row[the_col]
        display(HTML(f'<h2>Column found</h2><b>key=</b>{found},<b>value=</b>{result}({str(type(result))})<hr>')) if verbose else ''
    else:
        display(HTML(f'Unsupprted `col_match`')) if verbose else ''

    return int(result) if type(result) is str and result.isdigit() else result
  