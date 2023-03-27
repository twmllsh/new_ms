#-*- coding: utf-8 -*-
# dart_utils.py
# (c) 2018 ~ 2022 FinaceData.KR

import json
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import difflib

USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

matching_pattern = '''
\t+var node\d = {};
\t+node\d\['text'\] = "(.*?)";
\t+node\d\['id'\] = "(\d+)";[ \t]*
\t+node\d\['rcpNo'\] = "(\d+)";
\t+node\d\['dcmNo'\] = "(\d+)";
\t+node\d\['eleId'\] = "(\d+)";
\t+node\d\['offset'\] = "(\d+)";
\t+node\d\['length'\] = "(\d+)";
\t+node\d\['dtd'\] = "(.*?)";
\t+node\d\['tocNo'\] =  "(\d+)";'''

def corp_info(corp):
    '''
    기업 개황을 dict로 반환합니다 (corp=기업명, 기업코드, 종목코드 모두 가능)
    '''
    r = requests.get(f'https://dart.fss.or.kr/corp/searchExistAll.ax?textCrpNm={corp}', verify=False)
    corp_code = None if r.text == 'null' else r.text
    if not corp_code:
        print('기업을 찾을 수 없습니다 (기업명, 기업코드, 종목코드 모두 사용 가능 합니다)')
        return None

    r = requests.get(f'https://dart.fss.or.kr/dsae001/select.ax?selectKey={corp_code}', verify=False)
    r.encoding = 'utf-8'
    dfs = pd.read_html(r.text, index_col=0)
    corp_info = dfs[0].to_dict()[1]
    corp_info['corp_code'] = corp_code if corp_code else corp_key 
    return corp_info


def search_corp(corp):
    '''
    기업 검색, 이름을 포함하고 있는 기업을 검색 (corp=회사명 혹은 종목코드)
    '''
    url = 'https://dart.fss.or.kr/dsae001/search.ax'
    data = { 'textCrpNm': corp, 'maxResults': '10000' }
    headers = {'Referer':'http://dart.fss.or.kr/dsap001/guide.do', 'User-Agent': USER_AGENT }

    r = requests.post(url, verify=False, data=data, headers=headers)
    if '일치하는 회사명이 없습니다.' in r.text:
        print('일치하는 회사명이 없습니다.')
        return None
    soup = BeautifulSoup(r.text)
    table = soup.find('table')
    trs = table.tbody.find_all('tr')
    row_list = []
    for tr in trs:
        tds = tr.find_all('td')
        name = tds[0].a.text.strip()
        corp_code = tds[0].a['href'].replace("javascript:select('", "").replace("');", "")
        stock_code = tds[1].text
        row_list.append([name, corp_code, stock_code])
    return pd.DataFrame(row_list, columns=['code', 'corp_code', 'stock_code'])

def list_date_ex(date=None):
    '''
    지정한 날짜의 보고서의 목록 전체를 데이터프레임으로 반환 합니다(시간 포함)
    * date: 조회일 (기본값: 당일)
    '''
    date = pd.to_datetime(date) if date else datetime.today() 
    date_str = date.strftime('%Y.%m.%d')

    columns = ['rcept_dt', 'corp_cls', 'corp_code', 'corp_name', 'rcept_no', 'report_nm', 'flr_nm', 'rm']
   
    df_list = []
    for page in range(1, 100):
        time.sleep(0.1)

        url = f'https://dart.fss.or.kr/dsac001/search.ax?selectDate={date_str}&pageGrouping=A&currentPage={page}'
        r = requests.get(url)

        if '검색된 자료가 없습니다' in r.text:
            if page == 1:
                return pd.DataFrame(columns=columns)
            break

        data_list = []
        soup = BeautifulSoup(r.text, features="lxml")
        trs = soup.table.find_all('tr')
        for tr in trs[1:]:
            tds = tr.find_all('td')

            hhmm = tds[0].text.strip()
            corp_class = tds[1].span.span['title'].replace('시장', '') if tds[1].span else ''
            corp_code = tds[1].span.a['href'].split('\'')[1]
            name = tds[1].a.text.strip()
            rcp_no = tds[2].a['href'].split('=')[1]
            title = ' '.join(tds[2].a.text.split())
            fr_name = tds[3].text
            rcp_date = tds[4].text.replace('.', '-')
            remark = tds[5].text
            dt = date.strftime('%Y-%m-%d') + ' ' + hhmm
            data_list.append([dt, corp_class, corp_code, name, rcp_no, title, fr_name, remark])

        df = pd.DataFrame(data_list, columns=columns)
        df['rcept_dt'] = pd.to_datetime(df['rcept_dt'])
        df_list.append(df)
    return pd.concat(df_list)


def attach_files(rcp_no): 
    '''
    접수번호(rcp_no)에 속한 첨부파일 목록정보를 데이터프레임으로 반환합니다.
    * rcp_no: 접수번호를 지정합니다. rcp_no 대신 첨부문서의 URL(http로 시작)을 사용할 수 도 있습니다.
    '''
    if rcp_no.startswith('http'):
        download_url = rcp_no
    else:
        url = f"http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
        r = requests.get(url, verify=False, headers={'User-Agent': USER_AGENT})

        dcm_no = None
        matches = re.findall(f"viewDoc\('{rcp_no}', '(.*?)'", r.text)
        if matches:
            dcm_no = matches[0]

        if not dcm_no:
            raise Exception(f'{url} 다운로드 페이지를 포함하고 있지 않습니다')

        download_url = f'http://dart.fss.or.kr/pdf/download/main.do?rcp_no={rcp_no}&dcm_no={dcm_no}'

    r = requests.get(download_url, verify=False, headers={'User-Agent': USER_AGENT})
    soup = BeautifulSoup(r.text, 'lxml')
    table = soup.find('table')
    trs = table.find_all('tr')

    attach_files_dict = {}
    for tr in trs[1:]:
        tds = tr.find_all('td')
        fname = tds[0].text
        flink = 'http://dart.fss.or.kr' + tds[1].a['href']
        attach_files_dict[fname] = flink

    return attach_files_dict


def attach_docs(rcp_no, match=None):
    '''
    첨부문서의 목록정보(title, url)을 데이터프레임으로 반환합니다. match를 지정하면 지정한 문자열과 가장 유사한 순서로 소트하여 데이터프레임을 반환 합니다.
    * rcp_no: 접수번호
    * match: 문서 제목과 가장 유사한 순서로 소트
    '''
    r = requests.get(f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}', verify=False, headers={'User-Agent': USER_AGENT})
    soup = BeautifulSoup(r.text, 'lxml')

    row_list = []
    att = soup.find(id='att')
    if not att:
        raise Exception(f'rcp_no={rcp_no} 첨부문서를 포함하고 있지 않습니다')

    for opt in att.find_all('option'):
        if opt['value'] == 'null':
            continue
        title = ' '.join(opt.text.split())
        url = f'http://dart.fss.or.kr/dsaf001/main.do?{opt["value"]}'
        row_list.append([title, url])
        
    df = pd.DataFrame(row_list, columns=['title', 'url'])
    if match:
        df['similarity'] = df.title.apply(lambda x: difflib.SequenceMatcher(None, x, match).ratio())
        df = df.sort_values('similarity', ascending=False)
    return df[['title', 'url']].copy()


def sub_docs(rcp_no, match=None):
    '''
    지정한 URL문서에 속해있는 하위 문서 목록정보(title, url)을 데이터프레임으로 반환합니다
    * rcp_no: 접수번호를 지정합니다. rcp_no 대신 첨부문서의 URL(http로 시작)을 사용할 수 도 있습니다.
    * match: 매칭할 문자열 (문자열을 지정하면 문서 제목과 가장 유사한 순서로 소트 합니다)
    '''
    if rcp_no.isdecimal():
        r = requests.get(f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}', headers={'User-Agent': USER_AGENT})
    elif rcp_no.startswith('http'):
        r = requests.get(rcp_no, headers={'User-Agent': USER_AGENT})
    else:
        raise ValueError('invalid `rcp_no`(or url)')
        
    ## 하위 문서 URL 추출
    multi_page_re = (
        "\s+node[12]\['text'\][ =]+\"(.*?)\"\;" 
        "\s+node[12]\['id'\][ =]+\"(\d+)\";"
        "\s+node[12]\['rcpNo'\][ =]+\"(\d+)\";"
        "\s+node[12]\['dcmNo'\][ =]+\"(\d+)\";"
        "\s+node[12]\['eleId'\][ =]+\"(\d+)\";"
        "\s+node[12]\['offset'\][ =]+\"(\d+)\";"
        "\s+node[12]\['length'\][ =]+\"(\d+)\";"
        "\s+node[12]\['dtd'\][ =]+\"(.*?)\";"
        "\s+node[12]\['tocNo'\][ =]+\"(\d+)\";"
    )
    matches = re.findall(multi_page_re, r.text)
    if len(matches) > 0:
        row_list = []
        for m in matches:
            doc_id = m[1]
            doc_title = m[0]
            params = f'rcpNo={m[2]}&dcmNo={m[3]}&eleId={m[4]}&offset={m[5]}&length={m[6]}&dtd={m[7]}'
            doc_url = f'http://dart.fss.or.kr/report/viewer.do?{params}'
            row_list.append([doc_title, doc_url])
        df = pd.DataFrame(row_list, columns=['title', 'url'])
        if match:
            df['similarity'] = df['title'].apply(lambda x: difflib.SequenceMatcher(None, x, match).ratio())
            df = df.sort_values('similarity', ascending=False)
        return df[['title', 'url']]
    else:
        single_page_re = "\t\tviewDoc\('(\d+)', '(\d+)', '(\d+)', '(\d+)', '(\d+)', '(\S+)',''\)\;"
        matches = re.findall(single_page_re, r.text)
        if len(matches) > 0:
            doc_title = BeautifulSoup(r.text, features="lxml").title.text.strip()
            m = matches[0]
            params = f'rcpNo={m[0]}&dcmNo={m[1]}&eleId={m[2]}&offset={m[3]}&length={m[4]}&dtd={m[5]}'
            doc_url = f'http://dart.fss.or.kr/report/viewer.do?{params}'
            return pd.DataFrame([[doc_title, doc_url]], columns=['title', 'url'])
        else:
            raise Exception(f'{url} 하위 페이지를 포함하고 있지 않습니다')
        
    return pd.DataFrame(None, columns=['title', 'url'])


# (c) 2018,2022 FinaceData.KR