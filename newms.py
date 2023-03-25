from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import time
import random
from tqdm import tqdm

from collections import Counter
import sys
import numpy as np
import pandas as pd
from mplfinance.original_flavor import candlestick2_ohlc
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from pykrx import stock as pystock
import FinanceDataReader as fdr
import talib
import pytz
KST = pytz.timezone('Asia/Seoul')
import sqlite3
import glob
from dateutil.relativedelta import relativedelta
import re
import os
import pickle
import requests
from bs4 import BeautifulSoup
import xmltodict
import telegram
# import ms_web
import itertools
import time
import chardet
from text_mining_utils import extract_table
from io import BytesIO

from telegram.ext import Updater
from telegram.ext import MessageHandler
from telegram.ext import Filters
from telegram.ext import CommandHandler

# import find_stock

import paramiko
from scp import SCPClient

import threading
import queue

from sqlalchemy import create_engine
import json


# import matplotlib.pyplot as plt
# from io import BytesIO

class Db():
    '''
    /datas/db_info.json 
    sqlalchemy ver = 1.4.27 
    '''
    def __init__(self,db_info_file_path, db_name = None):
        self.db_info_file_path = db_info_file_path
        with open(self.db_info_file_path,'r') as f:
            self.db_info = json.load(f)
        self._set_db_info(db_info_file_path, db_name)
        
    def _set_db_info(self,db_info_file_path, db_name = None):
        if db_name == None:
            self.engine_uri1 = f'mysql+pymysql://{self.db_info["user"]}:{self.db_info["password"]}@{self.db_info["host"]}:{self.db_info["port"]}?charset=utf8'
            self.db_engine = create_engine(self.engine_uri1)
        else:
            self.db_info['db']=db_name
            self.engine_uri1 = f'mysql+pymysql://{self.db_info["user"]}:{self.db_info["password"]}@{self.db_info["host"]}:{self.db_info["port"]}?charset=utf8'
            self.engine_uri2 = f'mysql+pymysql://{self.db_info["user"]}:{self.db_info["password"]}@{self.db_info["host"]}:{self.db_info["port"]}/{self.db_info["db"]}?charset=utf8'
            self.db_engine = create_engine(self.engine_uri1)
            self.engine = create_engine(self.engine_uri2)
            
    
    def set_db_name(self,db_name):
        try:
            self.db_info['db'] = db_name
            self.db_engine = create_engine(f'mysql+pymysql://{self.db_info["user"]}:{self.db_info["password"]}@{self.db_info["host"]}:{self.db_info["port"]}?charset=utf8', encoding='utf-8')
            self.engine = create_engine(f'mysql+pymysql://{self.db_info["user"]}:{self.db_info["password"]}@{self.db_info["host"]}:{self.db_info["port"]}/{self.db_info["db"]}?charset=utf8', encoding='utf-8')
        except Exception as e:
            print(e)
            return pd.DataFrame()
    
    
    def show_databases(self):
        try:
            with self.db_engine.connect() as con:
                df = pd.read_sql('show databases',con)
            return df
        except Exception as e:
            print(e)
            return pd.DataFrame()

    def show_tables(self):
        try:
            with self.engine.connect() as con:
                df = pd.read_sql('show tables',con)
            return df
        except Exception as e:
            print(e)
            return pd.DataFrame()
    
    def show_columns(self,table_name):
        try:
            with self.engine.connect() as con:
                df = pd.read_sql(f'show columns from {table_name}',con)
            return df
        except Exception as e:
            print(e)
            return pd.DataFrame()
        
    def get_db(self, sql, index_col=None,**kwargs):
        try:
            with self.engine.connect() as con:
                df = pd.read_sql(sql,con,index_col=index_col,**kwargs)
                return df
        except Exception as e:
            print(e)
            return pd.DataFrame()
       
    def put_db(self,df,table_name, index=False, if_exists="append", **kwargs):
        ## 보내기. 
        ### df에 index내용은 없게 하자.
        
        print(kwargs)
        try:        
            with self.engine.connect() as con:
                df.to_sql(f'{table_name}',con = con, index=index,if_exists=if_exists, **kwargs)
        except Exception as e:
            print(e)
            return pd.DataFrame()

    def back_up_db(self, db_name,backup_file_name):
        # f"mysql -u {root} -p {password} {db_name} < {backup_file_name}.sql"
        pass
    
    def delete_db(self, table, sql):
        pass
    

class Dart:
    
    def get_dart_df(table, code = None,db_file_name = './dart/all_dart.db'):
        # table : '전환청구권행사', '전환사채권발행결정', '공급계약', '무상증자결정', '소각결정', '주식취득결정'
        if code ==None:
            sql = f'select * from "{table}"'
        else:
            sql = f'select * from "{table}" where code ="{code}"'
        
        conn = sqlite3.connect(db_file_name)
        df = pd.read_sql(sql,conn)
        return df

    


class Mymsg:
    '''
    chat_id:
        sean78_bot 
        sean_78_chnnel
        공시알림채널
        CB알림채널
        종목추천채널
        Sean_group
    '''
    def __init__(self,bot_token = None,chat_id = None,chat_name = "sean78_bot"):
        if bot_token == None:
            with open('/home/sean/sean/token/telegram_token.txt',"r") as f:
                self.token = f.readline().strip()
        else:
            self.token = bot_token
            
        self.bot = telegram.Bot(token=self.token)
        
        mydict = {'sean78_bot':'842897939',
                  'sean_78_chnnel':'-1001429647215',
                  '공시알림채널':'-1001186454317',
                  'CB알림채널':'-1001436692375',
                  '종목추천채널':'-1001248675767',
                  'sean_group':'-1001879355449'
                  }
        try:
            if chat_id == None:
                self.chat_id = mydict.get(chat_name)
            else:
                self.chat_id = mydict.get(chat_name)
        except:
            self.chat_id = mydict.get('sean78_bot')
            
        
    def send_message(self,chat_text,parse_mode = None, disable_web_page_preview=None):
        
        self.bot.sendMessage(self.chat_id, chat_text, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview) 
    
    # def send_photo_bio(self,bio, caption = None):
    #     # try:
    #     if caption ==None:
    #         caption = ''
    #         print('a')
    #     else:
    #         caption = str(caption)
    #         print('b')
    #     self.bot.send_photo(self.chat_id, photo = bio,caption=caption)
    #     print('c')
    #     # except:
    #     #     print('d')
    #     #     pass
    
    def send_photo_1(self,**kwargs):
        self.bot.send_photo(self.chat_id, **kwargs)
        
        
    def send_photo(self, file_name, caption = None):
        '''
        file_name 에 경로str 또는 Byteio객체 
        '''
        try:
            if caption ==None:
                caption = ''
            else:
                caption = str(caption)
            
            if type(file_name) == BytesIO:
                # print('ByteIo객체임')
                self.bot.send_photo(self.chat_id,photo=file_name,caption=caption)
            else:
                self.bot.send_photo(self.chat_id, open(file_name, 'rb'),caption=caption)
        except:
            pass
        
    def send_file(self, file_name):
        try:
            self.bot.send_document(self.chat_id, open(file_name, 'rb'))  
        except:
            pass
   
        
    def df_to_msgtext(self,df,extract = [],title = ""):
        '''
        index : 종목 columns 은 종목의 속성  인 df
        index별 내용 전달 텍스트 생성 list 형
        extract = ['',''] 변환할 col만.
        :return: list  테스트 필요
        '''

        ## 데이터프레임 전체내용 메세지 보낼때.[메세지 리스트 반환.]
        ## 필요한  column 만 추출한  df생성후 인자로 넣어줌.

        msg_list = []
        if len(extract)==0:
            col_list = list(df.columns)
        else:
            col_list = [item for item in extract if item in df.columns]
            if not len(col_list):
                return []

        
        for ix, row in df.iterrows():
            if title != "":
                msg_text = f"=== {title} ===\n"
            else:
                msg_text = ""
                
            for col in col_list:

                value = row[col]    
                if str(type(value)) == "<class 'int'>":   ## 숫자면 천단위 콤마,  넣고. str타입변경
                    value = format(value, ',')
                else:
                    value = value
                temp_text = f"{col}  : {value} \n"
                msg_text = msg_text + temp_text

            msg_list.append(msg_text)
        return msg_list

    def dic_to_msgtext(self,dic):
        '''
        모든 타입은 그냥  str으로 변환하면 된다. 수정하자.!
        :return:
        '''
        ## 딕셔너리 형태를 메세지 보낼때 . text반환.
        text = ""
        for key, value in dic.items():
            if str(type(value)) == "<class 'int'>":
                value_int = format(value, ',')
            else:
                value_int = value
            #     key_text = '{0:<1}'.format(key)
            text = text + "{} : {}\n".format(key, value_int)
        return text


class Sean_func:
    
    def my_scp(file_path, folder_path, host, port, user, pw):
        
        # send_file_path = "/home/sean/sean/data/sss.xlsx"
        # save_folder_name = r"C:\Users\twmll\sean\data"
        '''
        port 22 ubuntu
        port 2222 gram_window_ubuntu
        port 222 gram_window
        ## 대기종목 df로 저장한후 파일 윈도우로 보내놓기에 사용.
        
        
        my_scp(send_file_path,save_folder_name,'122.34.201.82','22','sean',"")
        '''
        
        def _createSSHClient(host, port, user, password):
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, port, user, password)
            return client

        ssh = _createSSHClient(host, port, user, pw)
        scp = SCPClient(ssh.get_transport())
        scp.get(file_path, folder_path)
        try:
            ssh.close_ssh_client() # 세션종료
        except:
            print('close err')
    
    
    def get_current_price_from_db(code):
        '''
        db에 있는 최근 주가값 가져오기.
        '''
        if code[0]=="A":
            code = code[1:]
            
        sql = f"select * from '{code}' ORDER BY Date DESC limit 1 "
        try:
            con = sqlite3.connect("/home/sean/sean/data/ohlcv_date.db")
            with con:
                current_price  = pd.read_sql(sql,con).iloc[0]['Close']
        except:
            current_price = 0 
        return current_price
    
    def get_휴장일():
        '''
        return : DataFrame
        date_columns:"calnd_dd_dy" 
        매달 1일 실행. 
        ##### 일단 크롤링이 안됨. 그냥 일단 데이터 받아놓고 사용하자.!   수정필요!!!1
        '''
        try:
            this_year = datetime.today().year
            
            url = "http://open.krx.co.kr/contents/COM/GenerateOTP.jspx?bld=MKD%2F01%2F0110%2F01100305%2Fmkd01100305_01&name=form&_=1678631887496"
            params = {
            "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            }
            resp = requests.get(url,headers = params)
            r_code = resp.text

            url = "http://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx"
            params = {
                "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36X-Requested-With: XMLHttpRequest"
            }

            data = {"search_bas_yy": this_year,
                    "gridTp": "KRX",
                    "pagePath": "/contents/MKD/01/0110/01100305/MKD01100305.jsp",
                    "code": r_code,
                }

            rep = requests.post(url, headers = params, data=data)

            js = rep.json()['block1']
            
            # save data
            if len(js):
                result = pd.DataFrame(js)
                try:
                    file_name = '/home/sean/sean/data/not_business_day.csv'
                    result.to_csv(file_name,index=False)
                    print(f'{file_name} 저장성공')
                except Exception:
                    print(f'{file_name} 저장실패')
                result['calnd_dd_dy'] = pd.to_datetime(result['calnd_dd_dy'])
            
        except Exception as e:
            result = pd.DataFrame()
            print(e)
        return result
    
    def is_휴장일(date = None):
        '''
        휴장일 체크 
        '''
        result = False
        file_name = '/home/sean/sean/data/not_business_day.csv'
        try:
            data = pd.read_csv(file_name)
            data['calnd_dd_dy'] = pd.to_datetime(data['calnd_dd_dy'])
        except:
            return False
        if date != None:
            today = pd.to_datetime(date)
        else:
            today = pd.to_datetime(datetime.today().date())
        
        if today in list(data["calnd_dd_dy"]):
            print('휴장일입니다.')
            result = True
        if (today.day_of_week == 5) | (today.day_of_week == 6) :
            print('주말입니다.')
            result = True
        return result
    
    def split_data(split_able_data , division_n):
        '''
        data를 n 개만큼 나눠주는 함수. 
        indexing 가능한 list , df 등에 사용가능. 
        
        '''
        ls = []
        
        if len(split_able_data) < division_n:
            ls.append(split_able_data)
        else:
            boundary_int = [int(len(split_able_data) * i/division_n) for i in range(division_n+1)]
        #         print('boundara_int',boundary_int)
            for i in range(division_n):
                split_data = split_able_data[boundary_int[i]:boundary_int[i+1]]
                ls.append(split_data)
        return ls


    def find_호가(price,market= '코스피', round_up= True):
        '''
        가격, market, 올림 내림 지정하면  호가로 정리해서 반환.
        '''
        if market in  ['코스피','유가증권시장','kospi','KOSPI',"1"]:
            market = 'kospi'
        elif market in  ['코스닥','kosdaq','KOSDAQ','2']:
            market = 'kosdaq'
        elif market in ['etf','ETF','elw','ELW']:
            market = 'etf' 
        table = {'min_price':[0,1000,5000,10000,50000,100000,500000],
                'max_price':[1000,5000,10000,50000,100000,500000,99999999],
                'kospi':[1,5,10,50,100,500,1000],
                'kosdaq':[1,5,10,50,100,100,100],
                'etf':[5,5,5,5,5,5,5]}
        
        result_df = pd.DataFrame(table)
        cond = (result_df['min_price']  <= price) & (result_df['max_price']  > price)
        unit = result_df.loc[cond,market].values[0]
        
        몫,나머지 = divmod(price,unit)
        
        price = 몫*unit if 나머지 == 0  else (몫+1)*unit  if round_up else 몫*unit
        # if round_up:
        #     if 나머지 ==0:
        #         result = 몫 * unit
        #     else:
        #         result = (몫 +1)* unit
        # else:
        #     result = 몫 * unit
        cond = (result_df['min_price']  <= price) & (result_df['max_price']  > price)
        unit = result_df.loc[cond,market].values[0]
        몫,나머지 = divmod(price,unit)
        price = 몫*unit if 나머지 == 0  else (몫+1)*unit  if round_up else 몫*unit
        
        # if round_up:
        #     if 나머지 ==0:
        #         result1 = 몫 * unit
        #     else:
        #         result1 = (몫 +1)* unit
        # else:
        #     result1 = 몫 * unit
        
        return price
    
    def get_time_KST():
        KST = pytz.timezone('Asia/Seoul')
        return datetime.now(KST)
    
    def find_current_os():
        '''
        return : macos, windows, linux
        '''
        
        import platform
        system_name = platform.system()
        
        if system_name =='Darwin':
            return "macos"
        elif system_name =='Windows':
            return 'windows'
        else:
            return 'linux'
        
    def find_difference_two_df(a_df, b_df,**cols):
        '''
        a_df : 이전 df  숫자만. 
        b_df : 최근 df
        **cols : return되는 df 에 columns 추가할시 dict형태로 임의 임력. ex) 구분 = '분기', 날짜 = '20200101'
        prepare_idx :  비교할 인덱스 입력, 
        '''
        if len(cols):
            keys = list(cols.keys())
            values = list(cols.values())
        else:
            keys = []
            values=[]
        



        if list(a_df.index) != list(b_df.index) or list(a_df.columns) != list(b_df.columns):
            print('데이터가 같지 않아 비교할수 있는데이터만 비교합니다. ')
            
        ## 공통인덱스, 컬럼 
        common_index = list(set(a_df.index) & set(b_df.index))
        common_columns = list(set(a_df.columns) & set(b_df.columns))
        common_index.sort()
        common_columns.sort()
        
        a_df = a_df.loc[common_index,common_columns]
        b_df = b_df.loc[common_index,common_columns]

        
        a_df.replace(np.nan,0,inplace=True)
        b_df.replace(np.nan,0,inplace=True)

        c_df = b_df != a_df 

        ## 변화값행렬만 남기기 위해 필요없는 행렬 제거
        c_df = c_df.replace(False,np.nan) 
        c_df.dropna(axis=1, how= 'all',inplace=True)
        c_df.dropna(axis=0, how= 'all',inplace=True)

        ## 변화값 저장. 
        result_ls = []
        for idx in c_df.index:
            for col in c_df.columns:
                value = c_df.loc[idx,col]
                if value == True :
                    before_value = a_df.loc[idx,col]
                    after_value = b_df.loc[idx,col]
                    try:
                        변화량 = after_value - before_value
                    except:
                        변화량 = 0
                    
                    result_ls.append(values +[idx,col,before_value,after_value,변화량])
                    

        ## 날짜시간 추가. 필요. 
        
        result_df = pd.DataFrame(result_ls,columns = keys + ['row','col','이전값','최근값','변화량'])
        
        return result_df

    def code_to_code_name(code):
        try:
            conn= sqlite3.connect('./data/code_df.db')
            query = 'select * from "code_df"'
            code_df  = pd.read_sql(query , con = conn,index_col=['cd'])
            acode = "A"+code
            code_name = code_df.loc[acode,'nm']
        except:
            print('데이터가져오기 실패(db)')
            return ""
        finally:
            conn.close()
        return code_name

    def krx_listing():
    
        krx = fdr.StockListing('KRX') # krx전체종목 가져오기
        change_columns = {'Symbol':'code' , 'Name':'code_name'} # columns 이름변경dic
        krx.rename(columns = change_columns,inplace = True) # columns명 변경
        krx = krx[krx['code_name'].str.contains('스팩') == False] # 스팩 제외
        krx = krx[krx['Market'].str.contains('KONEX') == False] #KONEX제외
        krx['code'] = krx['code'].apply(lambda x : "A"+x)   # A+code
        krx= krx[krx['code'].str.len()==7]  ## 종목코드 길이로 부동 산 등 제거
        krx = krx[krx['code'].str.match(pat=r'\w+\d{6}')]   ## 코드형태가 문자+숫자6개의 패턴만 추출
        krx=krx.set_index('code')                               # code 인덱스로
        # krx.to_csv('')
        return krx

    def last_xlfile_to_df(path, part_of_filename):
        '''
        폴더(path)내 특정이름이 포함된파일 중 가장최근 엑셀파일을 dataframe으로 반환
        '''
        if path[-1] != "/":
            path = path + "/"
        path = path
        # last_file_name = '파일이름.확장자
        temp_file_name = "*" + part_of_filename.split('.')[0] + "*." + part_of_filename.split('.')[1]
        temp_file_name = path + temp_file_name

        file_name_list = glob.glob(temp_file_name)
        last_file_name = max(file_name_list, key=os.path.getctime)
        # code_df = pd.read_excel(last_file_name).set_index('code')
        code_df = pd.read_excel(last_file_name,index_col=0)

        return code_df

    def 실적기준구하기_예전(YorQ='y'): ## 삭제예정
        
        '''
        현재날짜에 따라 실적기준년도를 네이버제무재표에 따른  columns생성
        :return:
        'Y'
        현재월 (기본 6월) 기준으로 현재년, 내년, 내후년 연도 반환.
        y는 반기 기준으로 현재 성장율 튜플로 반환 (1,2)현재성장율 (2,3)미래성장율
        'Q'
        q는 현재월 기준으로  yoy , qoq 기준 반환 (1,3)yoy (2,3)qoq
        '''
        # """
        # y는 반기 기준으로 현재 성장율 튜플로 반환 (1,2)현재성장율 (2,3)미래성장율
        # q는 현재월 기준으로  yoy , qoq 기준 반환 (1,3)yoy (2,3)qoq
        # """
        ## 실적비교할 연도정보추출(가운데값이 현재기준값)
        ## 1.5_2.0_형식으로 현재기준을 가운데로 하는조건
        ## 튜플형식으로 반환
        if YorQ in ['Y', 'y', '연도', '년']:
            YorQ = 'y'
        elif YorQ in ['Q', 'q', '분기']:
            YorQ = 'q'
        else:
            print('YorQ 값을 잘못입력하였습니다.')
        ym = datetime.today().strftime('%Y_%m')
        # ym = "2020_07"  ## 테스트용
        _year = ym.split('_')[0]
        _month = ym.split('_')[1]

        if YorQ == 'y':
            if int(_month) < 6:  ## 기준월
                c_year = int(_year)  ##상반기
            else:
                c_year = int(_year) + 1  ##하반기
            need_years = (str(c_year - 1), str(c_year), str(c_year + 1))
            return need_years
            ### 기본개념
            ## 분기별
            ## 첫달에 전분기 실적발표 매도,매수 자제
            ## 둘째달에 매수
            ## 셋째달에 발굴, 매수
        if YorQ == "q":
            if int(_month) in [1, 2, 3]:
                need_Q = _year + "/06"  ##분기지정
            elif int(_month) in [4, 5, 6]:
                need_Q = _year + "/09"  ##분기지정
            elif int(_month) in [7, 8, 9]:
                need_Q = _year + "/12"  ##분기지정
            elif int(_month) in [10, 11, 12]:
                need_Q = str(int(_year) + 1) + "/03"  ##분기지정

            a_1 = str(int(need_Q.split("/")[0]) - 1) + "/" + str(need_Q.split("/")[1]).zfill(2)  ## "3".zfill(2) == "03"
            if need_Q.split("/")[1] == "03":
                a_2 = str(int(need_Q.split("/")[0]) - 1) + "/12"
            else:
                a_2 = str(int(need_Q.split("/")[0])) + "/" + str(int(need_Q.split("/")[1]) - 3).zfill(2)
            a_3 = need_Q

            # if need_Q.split("/")[1] == "12":
            #     b_3 = str(int(need_Q.split("/")[0]) + 1) + "/03"
            #     b_1 = str(int(b_3.split("/")[0]) - 1) + "/03"
            # else:
            #     b_3 = str(int(need_Q.split("/")[0])) + "/" + str(int(need_Q.split("/")[1]) + 3).zfill(2)
            #     b_1 = str(int(b_3.split("/")[0]) - 1) + "/" + str(int(b_3.split("/")[1])).zfill(2)
            # b_2 = need_Q

            ### "Y"는('2019', '2020', '2021') , "Q" 는 ('2019/06', '2020/03', '2020/06', '2019/09', '2020/06', '2020/09') 리턴
            ### a그룹은 당해분기, b그룹은 다음분기
            ### b그룹은 잠시 제외. 재무표에 없는것들이 많음. yoy는  a_3/a_1    qoq는  a_3/a_2
            return a_1, a_2, a_3

    def 실적기준구하기(YorQ='y'):
        '''
        -1 은 과거. 0번째는 현재, 1번째는 다음 
        선행주가월수 수정해서사용가능. 
        return :  ('2020','2020')  or ('2020/06', '2021/03', '2021/06')
        '''
        if YorQ in ['Y', 'y', '연도', '년']:
            YorQ = 'y'
        elif YorQ in ['Q', 'q', '분기']:
            YorQ = 'q'
        else:
            print('YorQ 값을 잘못입력하였습니다.')

        def make_Q(day):
            if day.month in [1, 2, 3]:
                ret = str(day.year) + "/03"
            elif day.month in [4, 5, 6]:
                ret = str(day.year) + "/06"
            elif day.month in [7, 8, 9]:
                ret = str(day.year) + "/09"
            elif day.month in [10, 11, 12]:
                ret = str(day.year) + "/12"
            else:
                ret= None
            return ret

        today = datetime.today()
        

        if YorQ == 'y':
            분류기준월 = 6
            if today.month < 분류기준월:
                cur_year = today.year
            else:
                cur_year = today.year +1
            return [str(cur_year-2),str(cur_year-1)],[str(cur_year-1),str(cur_year)],[str(cur_year),str(cur_year+1)]

        elif YorQ == 'q':
            선행개월 =2
            delta_1month = relativedelta(months=1)
            day = today + delta_1month*선행개월 
            이전 = [make_Q(day-delta_1month*15),make_Q(day-delta_1month*6),make_Q(day-delta_1month*3)]

            이번 = [make_Q(day-delta_1month*12),make_Q(day-delta_1month*3),make_Q(day)]
            다음 = [make_Q(day-delta_1month*9),make_Q(day),make_Q(day+delta_1month*3)]
            return 이전,이번,다음

    def get_table_list_from_db(db_file_path):
        '''
        db의 테이블리스트 가져오기.
        '''

        query_table = "SELECT name FROM sqlite_master WHERE type='table'"
        conn = sqlite3.connect(db_file_path)
        with conn:
            tables_df = pd.read_sql(query_table , con = conn)
        ls = list(tables_df['name'])
        return ls

    def event_to_db(pathfile,table_name,list_or_dict):
        '''
        pathfile 은 현재부터 폴더 하나만 지정가능. ex) './data/foo.db'
        dict or list to database 
        '''
        path = re.findall('.+/',pathfile)
        if path:
            if not os.path.isdir(path[0]):
                os.mkdir(path[0])
        
        if type(list_or_dict) == dict:
            df = pd.DataFrame([list_or_dict])
        elif type(list_or_dict) == list:
            df = pd.DataFrame(list_or_dict).T
        else:
            return False
        
        con = sqlite3.connect(pathfile)
        with con :
            df.to_sql(table_name,con,if_exists='append',index=False)
        
        return True

    def event_from_db(pathfile,table_name,limit = None):
        '''
        event_to_db 와 짝꿍
        '''
        if limit == None:
            sql = f"SELECT * FROM {table_name}"
        elif limit != None:
            limit = int(limit)
            sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        else:
            sql = f"SELECT * FROM {table_name}"

        con = sqlite3.connect(pathfile)
        with con :
            df = pd.read_sql(sql, con, index_col=None)
        return df

    def to_pickle(data,filename,path='./pkl/'):
        '''
        기본 현재폴더에 pkl폴더 만들어서 저장.
        '''
        if not os.path.exists("pkl"):
                os.mkdir("pkl")
        filepath = path+filename
        with open(filepath, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOLL)
        
    def read_pickle(filename, path = './pkl/'):
        '''
        기본 현재폴더에 pkl폴더에서 값 가져옴. 
        '''
        if path[-1] != '/':
            path = path +"/"
        try:
            filepath = path + filename
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            return data
        except:
            print(filepath)
            print(f'{filename} 파일이 존재하지 않는것 같습니다.')
            return None

    def get_current_time(n=0):
        '''
        오늘로부터 n일 전후 리턴. 
        return : now [type:datetime] , date [type:str], time [type: str]
        '''

        KST = pytz.timezone('Asia/Seoul')
        now = datetime.now(KST) - timedelta(days=n)
        str_date = now.strftime('%Y-%m-%d')
        str_time= now.strftime('%H:%M:%S')
        return now,str_date,str_time

    def candle_status(o,h,l,c):
        dic={}
        if o<c:
            candle_status = '양봉'
        elif o==c:
            candle_status = '도지'
        else:
            candle_status = '음봉'
        
        all_len = h-l
        head_len = h - max(o,c)
        body_len = max(o,c)-min(o,c)
        tail_len = min(o,c)-l
        
        all_rate = round((h/l)*100,1)
        head_rate = round(head_len/all_len*100,0)
        body_rate = round(body_len/all_len*100,0)
        tail_rate = round(tail_len/all_len*100,0)

        ## 비율이기 때문에 크기에 대한 수치가 필요함. ....
        dic['상태']=candle_status
        dic['전체등락율']=all_rate
        dic['위꼬리비율']=head_rate
        dic['몸통비율']=body_rate
        dic['아래꼬리비율']=tail_rate
        
        return dic

    def remove_file(folder_path,contain_str):
        '''
        folder_path = "./test" # 폴더경로.
        contain_str : 파일명에 포함되는 단어. 예) ohlcv_db [충돌] .db
        '''
        removed_ls = []
        if folder_path[-1]!="/":
            folder_path = folder_path+"/"
        file_name_ls = os.listdir(folder_path)
        예상파일 = [fn for fn in file_name_ls if contain_str in fn]
        print(f'지워질 파일은 총 {len(예상파일)} 개 있니다.')
        for fn in file_name_ls:
            if contain_str in fn:
                path_name = folder_path+fn
                
                loop_boolean = True
                while loop_boolean:
                    q = input(f"{fn}파일을 정말로 지우시겠습니까.? (y/n), 중지하려면 q 입력하세요.")
                    if q =="y":
                        removed_ls.append(path_name)
                        os.remove(path_name)
                        print(f'{fn}파일을 지웠씁니다.')
                        loop_boolean= False
                    elif q=='n':
                        print(f'{fn}을 지우지 않았습니다.')
                        loop_boolean = False
                        continue
                        
                    elif q=='q':
                        print('작업을 중지합니다.')
                        print('지워진 파일: ', removed_ls)
                        return None
                    else:
                        print('잘못입력하였습니다. 다시 입력하여 주세요.')
                        loop_boolean = True
                        
        print('지워진 파일: ', removed_ls)

    def nomalize(s,min_value = 0 , max_value = 1):
        '''
        Series를 입력받아. nomalize정규화하여 Series형태로 반환. 
        input : Series, min_value, max_value
        return: Series
        '''
        i_min = s.min() 
        i_max = s.max()
        i_diff = i_max - i_min
        first_nomalize = (s-i_min)/i_diff
        out_diff = max_value - min_value
        result = (first_nomalize* out_diff ) + min_value
        return result

    ## 전체종목 현재가 얻어오기 
    def get_all_current_price(str_today = None):
        if str_today == None:
            date = Sean_func.get_current_time()[0]
            str_today = Sean_func.get_current_time()[1].replace("-","")
        else:
            date = pd.to_datetime(str_today)
            str_today = date.strftime("%Y%m%d")
        
        df_all = pystock.get_market_ohlcv_by_ticker(str_today,market='ALL')

        df_all  = df_all.loc[df_all['시가']!=0]
        df_all['Date'] = date
        # df_all.index  = "A" + df_all.index
        df_all.rename(columns = {'시가' : 'Open',
                        '고가' : 'High',
                        '저가' : 'Low',
                        '종가' : 'Close',
                        '거래량' : 'Volume',
                        '거래대금' : 'Amount' ,
                        '등락률' : 'Change' }, inplace = True)
        df_all.index.name = 'Code'
        df_all  = df_all.reset_index()   ## 
        ##
        df_all['code'] = df_all['Code']  
        df_all = df_all.set_index('code',drop=True)
        
        return df_all
    
    def get_rest_day(year=None):
        
        if year == None:
            year = datetime.now().year
        
        url = "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
        key_for_holiday = "i1lSt8CVsresHBofzKaGQL8Om9hAnHKBXFW5vogDhk006TcHLo3W2o5wb+euF+3swWa+4sHn7zo8sJ2+kCaFbg=="
        
        result_dic = {}
        
        for mon in range(1,13):
            params = {
                'ServiceKey':key_for_holiday,
                "solYear":str(year),
                "solMonth": str(mon).zfill(2)
            }
            resp = requests.get(url = url, params=params)
            xml = resp.content
            dic = xmltodict.parse(xml)
        
            
            try:
                data = dic['response']['body']['items']['item']
            except:
                continue
            if type(data)!=list:
                data = [data]
            if len(data):
                for item in data:
                    name, strdate = item['dateName'],item['locdate']
                    result_dic[strdate]=name
        
        holi_list = [item.date() for item in pd.to_datetime(pd.Series(result_dic.keys()))]

        print('이번달 휴일 리스트:', holi_list )
        return holi_list

    def list_to_str_ls(ls,max_len = 100,sept = ' '):
        '''
        list 를 문자열로 변형, 최대글짜 지정해서 분리해서 리스트로 반환. 메세지 보낼때 사용.
        max_len 을 실제보다 적게 지정해야함. 최소 5이하로. 
        '''
        
        result = sept.join(ls)
        data = []
        while len(result) > max_len:
            index  = result.find(sept,max_len )
            split_str = result[:index]
            data.append(split_str.strip(","))
            result = result[index:].strip(",")
            if len(result) <= max_len:
                data.append(result)
                break
        return data
    
    def is_holiday(date = None):
        '''
        주말인경우 True, 공휴일인경우 True, else False 반환.
        '''
        if date == None:
            tz = pytz.timezone('Asia/Seoul')
            now = datetime.now(tz=tz)
            date = now.date()
            print(tz)
        
            
        date = pd.to_datetime(date).date()
        print(date,type(date))

        url = "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
        key_for_holiday = "i1lSt8CVsresHBofzKaGQL8Om9hAnHKBXFW5vogDhk006TcHLo3W2o5wb+euF+3swWa+4sHn7zo8sJ2+kCaFbg=="
        
        if date.weekday() >4:
            print('주말입니다.')
            return True
        
        year= date.year
        mon = date.month
        
        try:
            result_dic = {}
            params = {
                'ServiceKey':key_for_holiday,
                "solYear":str(year),
                "solMonth": str(mon).zfill(2)
                    }
            resp = requests.get(url = url, params=params)
            xml = resp.content
            dic = xmltodict.parse(xml)
            
            try:
                data = dic['response']['body']['items']['item']
            except:
                data =[]
            if type(data)!=list:
                data = [data]
            if len(data):
                for item in data:
                    name, strdate = item['dateName'],item['locdate']
                    result_dic[strdate]=name
            
            holi_list = [day.date() for day in list(pd.to_datetime(pd.Series(result_dic.keys())))]
            
            if date in holi_list:
                print('공휴일입니다.')
                return True
            else:
                return False
        except:
            print('공휴일API 문제발생.')
            return False

    def plot_series(*series,**params):
        '''
        type이 같은 걸 넣어줘야 같이 그림. 
        '''
        for idx ,s in enumerate(series):
            if idx == 0: 
                ax1 =s.plot(figsize=(14,8),legend=True,**params)
            else:
                ax1 =s.plot(figsize=(14,8),legend=True)
        return ax1

    ## df 저장하기 .
    def df_to_png(df,path_filename):
        '''
        df를 png로 저장함. path_filename  : full path ex) : '.test1.png'
        '''

        try:
            import dataframe_image
            df.style.export_png(path_filename)

        except:
            print('dataframe_image 오류')

    def send_krx_chart():
        '''
        종합지수 그래프 메세지 보내기 
        '''
        
        import topdown as td
        kospi = Stock('1001','업종')
        kosdaq = Stock('2001','업종')
        f = td.topdown_plot(kospi.df,kosdaq.df,'코스피','코스닥')

        # 파일로 저장
        path = './topdown_temp/'
        file_name = 'topdown_temp.png'
        if os.path.isdir(path):
            f.savefig(path+file_name)
        else:
            os.mkdir(path)
            f.savefig(path+file_name)
        f.clf()

        Message.send_photo(path+file_name)

        return None


    def anal_recommended_data(all_cls_file_name, n = 5):
        '''
        mode3이 완료된이후 작업하기.
        과거 n일전 추천주 수익율 및 추천사윰 매수사유 기법데이터 저장해두기.
        '''
        ## all_cls_file_name 에서 파일과 같은 폴더 추출.
        data_path = f"{os.path.dirname(all_cls_file_name)}/"
        
        with open(all_cls_file_name , 'rb') as f:
            all_cls = pickle.load(f)
        # 삼성전자 = [cls for cls in all_cls if cls.code == '005930'][0]
        temp_cls = random.choice(all_cls)
        start_day = temp_cls.df.index[-(n+1)]
        last_day = temp_cls.df.index[-1]

        con = sqlite3.connect(f'{data_path}추천종목백업.db')
        sql = f'select * from "recommended" where 추천날짜 == "{str(start_day)}"'

        # 과거 추천종목 데이터
        with con:
            data = pd.read_sql(sql,con)
        data['추천날짜'] = pd.to_datetime(data['추천날짜'])

        data = data[data['추천날짜']==start_day]
        data = data.drop_duplicates(['추천날짜','code'])
        
        if len(data):
            check_list = []
            for i, row in data.iterrows():
                dic = {}
                dic['code'] = row['code']
                dic['code_name'] = row['code_name']
                dic['당시point'] = row['당시point']
                dic['추천일등락율'] = row['추천일등락율']
                dic['추천일'] = start_day
                dic['현재날짜'] = last_day
                dic['소요일수'] = n
                dic['추천기법'] = row['추천기법']
                dic['매수사유'] = row['매수사유']

                try:
                    cls = [cls for cls in all_cls if cls.code == row['code']][0]
                    temp_result = cls.cal_수익율(n)
                    dic.update(temp_result)
                    check_list.append(dic)
                except:
                    pass
        else:
            check_list = []
        print(len(check_list))
        if len(check_list):
            
            check_df = pd.DataFrame(check_list)
            
            try:
                check_df['매수사유갯수'] = check_df['매수사유'].apply(lambda x : len(x.split("|")))
            except:
                print('매수개수실패')
                pass
            
            ## Save pickle  1,3,5 일 모두 저장함. 그래서 파일이름에 n을 넣은것임.
            with open(f"{data_path}recommended_df{n}.pickle","wb") as f:
                pickle.dump(check_df, f,protocol=pickle.HIGHEST_PROTOCOL)
                
        else:
            check_df = pd.DataFrame()
        
    
        ## 만약에 check_df 가 빈데이이면 과거 추적종목리스트가 없는것임.
        return check_df
    

class Daum:
    
    def get_ohlcv( code, option, limit=480):
        '''
        :code stock code
        :param limit: Period
        :param option:'월봉','주봉','일봉','60분봉','30분봉','15분봉','5분봉',
        :return: df
        '''
        if code[0]!="A":
            code = "A"+code

        option_dic = {
           '월봉': 'months',
           '주봉':'weeks',
           '일봉': 'days',
           '60분봉': '60/minutes',
           '30분봉':'30/minutes',
           '15분봉': '15/minutes',
           '5분봉': '5/minutes'
          }

        str_option = option_dic[option]
        url = f"http://finance.daum.net/api/charts/{code}/{str_option}"
        params = {
            "limit": f"{limit}",
            "adjusted": "true"
                }
        headers = {
            "referer": "https://finance.daum.net/chart/",
            "user-agent": "Mozilla/5.0"
                }
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()
        data = data['data']
        df = pd.DataFrame(data)
        chage_col = {'candleTime': 'Date',
                     'tradePrice': 'Close',
                     'openingPrice': 'Open',
                     'highPrice': 'High',
                     'lowPrice': 'Low',
                     'candleAccTradePrice': 'TradePrice',
                     'candleAccTradeVolume': 'Volume',
                     }
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'TradePrice']
        df['candleTime'] = pd.to_datetime(df['candleTime'])
        
        # df = df.rename(chage_col, axis='columns')
        df.rename(columns = chage_col,inplace= True)
        df = df[columns].set_index('Date')

        return df

    def send_head_news():
        chat_id = "-1001248675767" ## 종목추천 채널
        url = "https://finance.daum.net/content/news/news_top"

        headers = {
            "user-agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"
            ,"referer": "https://finance.daum.net/news"
        }

        resp = requests.get(url , headers = headers)
        result = resp.json()

        ls=[]
        text =""
        for item in result:
            word = f"[{item['title']} -{item['cpKorName']} {item['updatedAt']}]({item['newsUrl']})\n"
            ls.append(word)
            text += word
        # try:
        #     Message.send_message(chat_id = chat_id, chat_text=text,disable_web_page_preview=True)
        # except:
        #     pass
        
        return ls

class Naver:
    
    def get_naver_finance(code, YorQ):
        '''
        네이버금융 재무제표를 df형식으로 반환
        :param YorQ: 연도별: "Y"  , 분기별 :"Q"
        :return: DataFrame
        '''
        if YorQ in ["Y", "y", "연","년","연도별"]:
            YorQ = "Y"
        elif YorQ in ["Q", "q", "분기", "분기별"]:
            YorQ = "Q"
        else:
            print('매개변수오류, q y 분기 연 ')
            return None

        url_tmp = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        r_tmp = requests.get(url_tmp)

        pattern_enc = re.compile("encparam: '(.+)'", re.IGNORECASE)
        pattern_id = re.compile("id: '(.+?)'", re.IGNORECASE)

        target_text = r_tmp.text
        encparam = pattern_enc.search(target_text).groups()[0]
        id_ = pattern_id.search(target_text).groups()[0]

        payload = {}
        payload['cmp_cd'] = code
        payload['fin_typ'] = 0
        payload['freq_typ'] = YorQ
        payload['encparam'] = encparam
        payload['id'] = id_

        head = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:67.0) Gecko/20100101 Firefox/67.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Referer': "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?",
            'X-Requested-With': 'XMLHttpRequest'
        }

        finacial_url = "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?"
        consen_url = "https://navercomp.wisereport.co.kr/v2/company/cF1002.aspx"
        r = requests.get(finacial_url, params=payload, headers=head)
        r1 = requests.get(consen_url, params=payload, headers=head)
        
        df = pd.read_html(r.text)[1]
        consen_df = pd.read_html(r1.text)[0]
        
        if YorQ == "Y":
            new_col = [x[1].split()[0].split("/")[0].strip() for x in list(df.columns)]  ## column명 처리
        elif YorQ == "Q":
            new_col = [x[1].split()[0].split("(")[0].strip() for x in list(df.columns)]  ## column명 처리

        df.columns = new_col  ## column 명 지정
        df = df.reset_index(drop=True)  ##index  reset
        df = df.set_index('주요재무정보')  ##새로운인덱스 지정

        if 'Unnamed:' in df.columns:
            df.drop(['Unnamed:'], axis=1, inplace=True)  ## 데이터크롤링후 상장전데이터가 있으면 제거.

        return df

    # 크롤링하여 DB로 저장
    def naver_finance_update_db(data_path):
        if data_path[-1] !="/":
            data_path += "/"
        
        KST = pytz.timezone('Asia/Seoul')
        today = datetime.now(KST)
        str_today = today.strftime("%Y%m%d %H:%M")


        tickers = Fnguide.get_ticker_by_fnguide(data_path)

        ## data폴더가 없으면 만들기.
        if not os.path.isdir(data_path):
            os.mkdir(data_path)


        conn = sqlite3.connect(f'{data_path}재무제표.db')
        change_ls = []
        for idx,row in tickers[2:].iterrows():
        #for idx,row in tickers[445:].iterrows():
            code = row['cd'][1:]
            code_name = row['nm']
            print('=='*15)
            print( idx, code_name )
            
            for gb in ['연도별', '분기별']:

                # web crawl
                try:
                    df = Naver.get_naver_finance(code,gb)
                    
                except:
                    print(code_name,'웹크롤링 실패!')
                    continue

                ## 잘못된컬럼 수정 연도표시를 잘못한 종목이 존재함. 예 2021년이 두개 존재. 
                ls = list(df.columns)
                for i in ls:
                    if ls.count(i)!=1:
                        a = ls.index(i)
                        ls[a]=ls[a]+'_1'
                new_col = ls
                df.columns = new_col

                ## 기존데이터 불러와서 비교하고 변화값 따로 엑셀로 저장.###################################################
                tableName = 't_' + gb + "_" + code
                query = 'select * from ' + tableName
                try:
                    with conn:
                        pre_df = pd.read_sql(query , con = conn,index_col='주요재무정보')
                except:
                    print(f'{code_name}db가져오기 실패')
                    pre_df = pd.DataFrame()
                    ## db에 저장하고 continue
                    try:
                        with conn:
                            df.to_sql(tableName,conn,if_exists = 'replace')
                            print('db 새로 저장 성공!')
                    except:
                        print('db 새로 저장 실패')
                    
                    continue
                    
                
                # change_df = Sean_func.find_difference_two_df(pre_df,df,종목코드 = code , 종목명 = code_name, 감지날짜 = str_today,구분 = gb)
                
                ## 변경사항이 있으면 변경내용을 change_ls에 추가하고  새로운 df 를 db로 저장.
                try:
                    change_df = Sean_func.find_difference_two_df(pre_df,df,종목코드 = code , 종목명 = code_name, 감지날짜 = str_today,구분 = gb)
                    if len(change_df):
                        change_ls.append(change_df)
                        with conn:
                            df.to_sql(tableName,conn,if_exists = 'replace')
                        print(f'{code_name} {gb} 데이터갱신성공',end = "")
                    else:
                        print(f'{code_name}종목의 변경사항없음..')
                except Exception as e:
                    change_df = pd.DataFrame()
                    print(f'{e} : {code_name} {code} 변경사항 저장 오류.....')

        conn.close()

        ## 검색자료가 있으면 ...
        if len(change_ls):
            result_df = pd.concat(change_ls)
            result_df.reset_index(drop=True,inplace=True)
            result_df['변동구분'] = result_df.apply(lambda x: '신규' if x['이전값']==0 and x['최근값']!=0 else '삭제' if x['이전값']!=0 and x['최근값']==0 else '변동'  , axis=1   )
            result_df['변동율'] = result_df.apply(lambda x :   0 if x['변동구분']=='삭제' else  x['최근값'] if x['변동구분']=='신규'  else (x['최근값']/x['이전값'])-1  ,axis=1  )
            
            ## 변경사항 저장. 
            try:
                con = sqlite3.connect(f'{data_path}재무제표_변경.db')
                with con:
                    result_df.to_sql('change_list',con,if_exists='append',index=False)
                # con.close()
            except:
                print('추정실적 변경사항 데이터저장 오류') 

                
            
            ###### 알림 부분 ################
            try:
                col = ['종목명','row','이전값','최근값']  # 보내질 Col지정. 
                변동률 = 0.1
                항목구분 =['매출액','영업이익','당기순이익','EPS(원)'] ## 변동기준항목지정.

                변동구분_ls = ['변동','신규','삭제','하향'] ## 
                for 변동구분 in 변동구분_ls:
                    if 변동구분 == "변동":
                        expr = f"변동구분 == '{변동구분}' & 변동율 >={변동률} & row in {항목구분} & 구분=='연도별' & 최근값>0"
                        cond_df = result_df.query(expr)
                    elif 변동구분 == "하향":
                        expr = f"변동구분 == '{변동구분}' & 변동율 <=-{변동률} & row in {항목구분} & 구분=='연도별' & 최근값>0 & 변화량<0 "
                        cond_df = result_df.query(expr)
                    elif 변동구분 == "신규":
                        expr = f"변동구분 == '{변동구분}' & row in {항목구분} & 구분=='연도별' & 최근값>0"
                        cond_df = result_df.query(expr)
                    elif 변동구분 == "삭제":
                        expr = f"변동구분 == '{변동구분}' & row in {항목구분} & 구분=='연도별'"
                        cond_df = result_df.query(expr)
            
                    if len(cond_df):
                        ## 메세지로 보내기 
                        try:
                            for year in sorted(list(cond_df.col.unique())):
                                year_send_df = cond_df.query("col ==@year")   # 년도별로 보냄. 
                                txt = year_send_df.to_string(header=False, index =False, columns = col ,show_dimensions=True)
                                txt = year+"_"+변동구분 +"\n"+ txt
                                time.sleep(2)
                        except:
                            pass
            except:
                pass
         
        else:
            print('전체데이터  변경사항없음.')


    def get_news_by_ticker(code,cnt= 10):
        '''
        code: stock_code, cnt : 개수, 뉴스데이터 모바일사이트에서 크롤릴 
        return : DataFrame
        '''
        if not str(code).isdigit() or (len(str(code))!=6):
            print('코드번호를 입력하세요.')
            return None
        url = f"https://m.stock.naver.com/api/news/stock/{str(code)}"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',}
        params = {
            "pageSize":cnt,
            "page":1,
            "searchMethod":"title_entity_id.basic",
            }
        resp = requests.get(url,params=params,headers= headers)
        js = resp.json()
        result_ls = []
        for j in js:
            temp_dic = {}
            temp_dic['dt'] = pd.to_datetime(j['items'][0]['datetime']).strftime("%Y-%m-%d")
            temp_dic['title'] = j['items'][0]['title']
            temp_dic['officename'] = j['items'][0]['officeName']
            result_ls.append(temp_dic)
        result = pd.DataFrame(result_ls)
        return result

    def _get_group_list(group = 'theme'):
        '''
        group : theme, upjong
        return : list [(theme(upjong)_name, url),(theme_name, url),...]
        '''

        url = f"https://finance.naver.com/sise/sise_group.naver?type={group}"
        
        r = requests.get(url)
        print('응답코드 : ',r.status_code)
        print('url :', r.url) 
        try: 
            soup = BeautifulSoup(r.text, 'html5lib')
        except:
            soup = BeautifulSoup(r.text, 'html.parser')
        
        selector = "#contentarea_left > table >  tr > td>  a" ### 변경 주의.
        tags = soup.select(selector)
        if not len(tags):
            selector = "#contentarea_left > table > tbody >  tr > td>  a"
            tags = soup.select(selector)

        if not len(tags):
            print('selector 오류.')
            return []

        basic_url = 'https://finance.naver.com'
        ls = []
        for tag in tags:
            detail_url = basic_url + tag['href']
            ls.append((tag.text,detail_url))
        return ls

    def _get_theme_codelist_from_theme(name , url_theme):
        '''
        input : theme(upjong), url 
        return : list (dict) theme(upjong)name , code, code_name
        '''
        params = {}
        r = requests.get(url_theme)
        try:
            soup = BeautifulSoup(r.text,'html5lib')
        except:
            soup = BeautifulSoup(r.text,'html.parser')
        
        
        selector = "#contentarea > div:nth-child(5) > table > tbody > tr"
        table_tag = soup.select(selector)

        ls = []
        for tag in table_tag:
            dic = {}
            try:
                code_name = tag.select('td.name > div > a')[0].text
                code = tag.select('td.name > div > a')[0]['href'].split("=")[-1]
                try:
                    theme_text = tag.select("p.info_txt")[0].text
                except:
                    pass
                dic['name']= name
                dic['code']= code
                dic['code_name']= code_name
                try:
                    dic['theme_text']= theme_text
                except:
                    pass
                ls.append(dic)
            except:
                pass
        return ls    

    def save_theme_upjong_list(data_path):
        '''
        업종, 테마 리스트 받아와서 df형태로 pickle저장 ! 
        '''
        if data_path[-1] !="/":
            data_path += "/"
            
        theme_ls ,upjong_ls  = Naver._get_group_list() , Naver._get_group_list('upjong')
        
        temp_ls = []
        
        for ls in [theme_ls,upjong_ls]:
            all_ls = []
            
            for name , url in  ls:
                print(name,url,"naver info to pickle(theme, upjong")
                ls = Naver._get_theme_codelist_from_theme(name , url)
                all_ls = all_ls + ls

            df = pd.DataFrame(all_ls)
            print(f'{len(all_ls)}개 정보저장.')
            temp_ls.append(df)
        
        
        theme_df , upjong_df = temp_ls
    
    
        ## make folder 
        try:
            if not os.path.exists(data_path):
                os.mkdir(data_path)
                print(f'maked {data_path} directory')

            ## Save pickle
            with open(f"{data_path}naver_theme_df.pickle","wb") as f:
                pickle.dump(theme_df, f,protocol=pickle.HIGHEST_PROTOCOL)

            with open(f"{data_path}naver_upjong_df.pickle","wb") as f:
                pickle.dump(upjong_df, f,protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            print(e , '저장실패.')
            
            pass
        return temp_ls

    def __get_item_info(code):
        '''
        basic info 가져오기
        '''
        code_url = f"https://finance.naver.com/item/main.naver?code={code}"
        r = requests.get(code_url)
        try:
            soup = BeautifulSoup(r.text, 'html5lib')
        except:
            soup = BeautifulSoup(r.text, 'html.parser')
        dfs = pd.read_html(str(soup))
        info = []
        for df in dfs:
            if df.shape[1] == 2:
                info.append(df)
        
        col = [col[1] for col in dfs[3].columns]
        temp_df = dfs[3].copy()
        temp_df.columns = col
        temp_df = temp_df.set_index('주요재무정보').filter(regex = r"\d{4}.\d{2}$").iloc[:,-1:].reset_index()
        temp_df.columns = 0,1

        info.append(temp_df)
        
        result = pd.concat(info).set_index(0).T
        return result

    def _get_all_basic_info(data_path):
        '''
        전체기본정보 pickle로 저장하기.new_versionb
        '''
        
        print('ver 2022-12-12')
        if data_path[-1]!="/":
            data_path += "/"
        
        codes_df= Fnguide.get_ticker_by_fnguide(data_path)
        codes_df = codes_df[2:]
        all_count = len(codes_df)
        
        concat_list = []
        for i,row in codes_df[:].iterrows():
            
            code =row['cd'][1:]
            code_name = row['nm']
            try:
                item_info  = Naver.__get_item_info(code)
                item_info['code'] = code
                item_info['code_name'] = code_name
            except:
                print('오류나서 pass')
                continue
            print(i,code_name , round(i/all_count * 100,1),'작업중..')
            concat_list.append(item_info)
        
        ## make folder 
        if not os.path.exists(data_path):
            os.mkdir(data_path)
            print(f'maked {data_path} directory')
                
        try:
            ## concat_list 일단 저장하기 .
            temp_file_name2 = f"{data_path}basic_info_temp_list.pkl","wb"
            with open(temp_file_name2, 'wb') as f:
                pickle.dump(concat_list, f, protocol=pickle.HIGHEST_PROTOCOL)
            print('concat_list 임시저장 완료!')
            ## 나중엔 없애야함.  ## 파일도 지워야함. 
        except:
            pass
        
        
        result_df = pd.concat(concat_list)
        # reset_index
        result_df = result_df.reset_index(drop=True)

        ##전처리 오류발생 대비 일단 저장. 
        ## Save pickle
        with open(f"{data_path}naver_basic_info_df.pickle","wb") as f:
            pickle.dump(result_df, f,protocol=pickle.HIGHEST_PROTOCOL)
            print('전처리 이전 저장완료.')
        
        # 시가총액 전처리.
        result_df['시가총액(억)'] = result_df['시가총액'].str.replace('[^0-9]','',regex=True).astype('float')

        ## 시가총액순위, 코스닥코스피  전처리.
        result_df['코스피/코스닥'] = result_df['시가총액순위'].str.split(" ",expand=True)[0]
        result_df['시총순위'] = result_df['시가총액순위'].str.split(" ",expand=True)[1].str.replace("[^0-9]","",regex  = True).astype('float')


        ## 액면가 처리
        result_df['액면가']= result_df['액면가l매매단위'].str.split(expand=True)[0].str.replace("[^0-9]","",regex = True).astype('float')

        ## 52주 최고최저 전처리
        result_df['52주최고'] = result_df['52주최고l최저'].str.split("l",expand = True)[0].str.replace('[^0-9]',"",regex=True).astype('float')
        result_df['52주최저'] = result_df['52주최고l최저'].str.split("l",expand = True)[1].str.replace('[^0-9]',"",regex=True).astype('float')

        result_df[['매출액','영업이익','당기순이익']] = result_df[['매출액','영업이익','당기순이익']].replace("-",np.nan)
        result_df[['매출액','영업이익','당기순이익']] = result_df[['매출액','영업이익','당기순이익']].astype('float')

        result_df[['영업이익률','순이익률','ROE(지배주주)','부채비율','당좌비율','유보율']] = result_df[['영업이익률','순이익률','ROE(지배주주)','부채비율','당좌비율','유보율']].replace('-',np.nan)
        result_df[['영업이익률','순이익률','ROE(지배주주)','부채비율','당좌비율','유보율']]= result_df[['영업이익률','순이익률','ROE(지배주주)','부채비율','당좌비율','유보율']].astype('float')

        result_df[['EPS(원)','PER(배)','BPS(원)','PBR(배)','주당배당금(원)','시가배당률(%)','배당성향(%)']] = result_df[['EPS(원)','PER(배)','BPS(원)','PBR(배)','주당배당금(원)','시가배당률(%)','배당성향(%)']].replace("-",np.nan)
        result_df[['EPS(원)','PER(배)','BPS(원)','PBR(배)','주당배당금(원)','시가배당률(%)','배당성향(%)']] = result_df[['EPS(원)','PER(배)','BPS(원)','PBR(배)','주당배당금(원)','시가배당률(%)','배당성향(%)']].astype('float')

        col = ['code', 'code_name', '시가총액(억)', '코스피/코스닥', '시총순위', '액면가',
            '52주최고', '52주최저','매출액','영업이익','당기순이익','영업이익률','순이익률','ROE(지배주주)',
            '부채비율','당좌비율','유보율','EPS(원)','PER(배)','BPS(원)','PBR(배)','주당배당금(원)','시가배당률(%)','배당성향(%)']

        result_df = result_df[col]
        
        ## Save pickle
        temp_file_name = f"{data_path}naver_basic_info_df.pickle"
        with open(temp_file_name,"wb") as f:
            pickle.dump(result_df, f,protocol=pickle.HIGHEST_PROTOCOL)
            print('전처리 이후 저장완료!')

            try:
                os.remove(temp_file_name2)
                print(f'{temp_file_name2} 임시파일 삭제')
            except:
                pass

            
    def get_basic_info(data_path):
        '''
        - 기본정보  데이터로 저장하기. 
        개선할사항: ROE(%) 추가!
        전종목 순회하여 엑셀로 저장해줌. 중간 에러시 pickle파일로 저장되니 오류시 수정후 재실행
        ./data/basic_info.xlsx 로 저장!.
        return : df
        '''
        if data_path[-1] != "/":
            data_path += data_path
            
        # 테마정보 dic형태로 load
        with open('f{data_path}theme.pickle', 'rb') as f:
            theme_dic = pickle.load(f)

        # ## 코드로 테마가져오기. 
        # code = '077360'
        # theme = [key for key, value in theme_dic.items() if code in value]  ## list 반환


        ## 임시백업파일 이름 지정 
        pick_file_path = f'{data_path}basic_info_data_temp.pickle'
        ## 기존재무제표 가져옴. 
        # conn = sqlite3.connect("C:/Users/twmll/Google 드라이브/choco_py/sean_stock/data/재무제표정리.db")
        conn = sqlite3.connect(f"{data_path}재무제표정리.db")
        query = 'select * from ' + 'naver_finance'
        with conn:
            재무df = pd.read_sql(query , con = conn,index_col='주요재무정보')

        code_df = Fnguide.get_ticker_by_fnguide(data_path)

        # 혹시 백업파일이 있다면 가져와서 리스트에 담는다 
        try:
            with open(pick_file_path, 'rb') as f:
                all_ls = pickle.load(f)
        except:
            all_ls =[]
            
        start = len(all_ls)   ## df슬라이싱에 변수넣는다. 
        print(start,'번쨰부터 재시작')

        for idx, row in code_df[start:].iterrows():
            
            if idx % 10 == 0:   ## 저장 개수 단위. 짧을수록 안전. 
                                
                with open(pick_file_path, 'wb') as f:
                    pickle.dump(all_ls, f, protocol=pickle.HIGHEST_PROTOCOL)
            #####################################################################################    
            temp_dic={}
                        
            acode = row['cd']
            code_name = row['nm']
            print(idx,acode,code_name)
            
            theme = [key for key, value in theme_dic.items() if acode[1:] in value]  ## list 반환
            print('테마 : ',",".join(theme), '저장명령 필요. ')
            
            url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode={acode}"
            resp = requests.get(url)
            soup = BeautifulSoup(resp.text,'html5lib')
            
            selector = "#corp_group2 > dl > dd"
            tags= soup.select(selector)
            tags = [float(tag.text.replace("-","0").replace(",","").replace("%","0")) for tag in tags  ]
            PER , PER_12M , _ , PBR , 배당수익률 = tags
            #     PER,PER_12M,PBR,배당수익률
            
            selector = "#compBody > div.section.ul_corpinfo > div.corp_group1 > p > span"
            tags= soup.select(selector)
            tags = [tag.text for tag in tags]
            
            try:
                구분 = tags[1].split(" ")[0]
            except:
                구분 = ""
            try:
                업종 = tags[1].split(" ")[1]
            except:
                업종 = ""
            try:    
                FICS = tags[3].split(" ")[2]
            except:
                FICS = ""
        #     구분,업종,FICS
            
            try:
                액면가 = extract_table(soup,'+종가/ 전일대비 +액면가','액면가')
                액면가 = int(액면가.replace(",",""))
            except:
                액면가 = 0
        #     액면가


            발행주식수 = extract_table(soup,'+종가/ 전일대비 +발행주식수','발행주식수',ncol = -2)
            try:
                보통발행주식수= int(발행주식수.split('/')[0].replace(",",""))
                우선발행주식수= int(발행주식수.split('/')[1].replace(",",""))
            except:
                보통발행주식수,우선발행주식수 = (0,0)
        #     보통발행주식수,우선발행주식수


            시가총액_미상장포함 = extract_table(soup,'+종가/ 전일대비 +시가총액 ','+시가총액 ',ncol = -2,verbose=False)
            시가총액 = extract_table(url,'+종가/ 전일대비 +시가총액 ' ,'+시가총액 -상장예정',ncol = -2,verbose=False)
            시가총액_미상장포함 = 시가총액_미상장포함.replace(",","")
            시가총액= 시가총액.replace(",","")
            try:
                시가총액_미상장포함=int(시가총액_미상장포함)
            except:
                시가총액_미상장포함=0
            
            try:
                시가총액=int(시가총액)
            except:
                시가총액 =0

        #     시가총액_미상장포함,시가총액
            
            
            유동주식수와비율 = extract_table(soup,'+종가/ 전일대비 유동주식수 ','+유동주식수 ',ncol = -1,verbose=False)
            try:
                유동주식수 = int(유동주식수와비율.split("/")[0].replace(",",""))
                유동비율 = float(유동주식수와비율.split("/")[1].replace(",",""))
            except:
                유동주식수,유동비율 = (0,0)
        #     유동주식수,유동비율    

            
            외국인보유비중 = extract_table(soup,'+종가/ 전일대비 +외국인 보유비중 ','+외국인 보유비중 ',ncol = -1,verbose=False)
            try:
                외국인보유비중=float(외국인보유비중.replace("-",""))    
            except:
                외국인보유비중 = 0
        #     외국인보유비중

            
            
            
            ## 부채비율도 가져오기 위해 유보율로 마지막 col_name 구함. 
            # temp_col = 재무df[재무df['code']==f'{acode}'].loc['자본유보율'][:7].dropna().index[-1]    ## 재무표상 가장최근값 가져옴. ( 예측치가 잇어서 보완필요.)
            temp_col = Sean_func.실적기준구하기()[0][0]   ### 과거년도의 첫번쨰연도 가져옴.   ( 그냥 확정값만가져옴. )
            try:
                자본유보율  = 재무df[재무df['code']==f'{acode}'].loc['자본유보율',temp_col]
            except:
                자본유보율= None
                
            try:    
                부채비율  = 재무df[재무df['code']==f'{acode}'].loc['부채비율',temp_col]
            except:
                부채비율= None
            try:
                ROE = 재무df[재무df['code_name']==f'{code_name}'].loc['ROE(%)',temp_col]
            except:
                ROE= None
        #   자본유보율,부채비율
            
            
            temp_dic['acode']=acode
            temp_dic['code_name']=code_name
            temp_dic['구분']=구분
            temp_dic['업종']=업종
            temp_dic['FICS']=FICS
            temp_dic['테마']=",".join(theme)
            
            temp_dic['시가총액']=시가총액
            temp_dic['시가총액_미상장포함']=시가총액_미상장포함
            
            temp_dic['액면가']=액면가
            temp_dic['자본유보율']=자본유보율
            temp_dic['부채비율']=부채비율
            temp_dic['외국인보유비중']=외국인보유비중
            temp_dic['유동주식수']=유동주식수
            temp_dic['유동비율']=유동비율
            temp_dic['보통발행주식수']=보통발행주식수
            temp_dic['우선발행주식수']=우선발행주식수
            temp_dic['PER']=PER
            temp_dic['PER_12M']=PER_12M
            temp_dic['PBR']=PBR
            temp_dic['ROE']=ROE
            temp_dic['배당수익률']=배당수익률
        ###################################################################################    
            all_ls.append(temp_dic)
            
        df = pd.DataFrame(all_ls)

        df.to_excel(f'{data_path}basic_info.xlsx')

        try:
            conn = sqlite3.connect(f'{data_path}basic_info.db')
            with conn:
                df.to_sql('info',conn,if_exists='replace',index=False)

        except:
            print('db저장오류')

        ## 작업모두 완료시 피클백업파일 삭제.
        if os.path.isfile(pick_file_path):
            os.remove(pick_file_path)
        
        return df

    
class Fnguide:
    
    def __init__(self,code):
        self.code = code
        # self.__스크래핑 = self.graph_재무제표()
        # self.포괄손익계산서 = self.__스크래핑[0]
        # self.재무상태표 = self.__스크래핑[1]
        # self.현금흐름표 = self.__스크래핑[2]
        self.재무제표_Y = self.table_재무제표() 
        # self.재무제표_Q = self.table_재무제표(self.code,YorQ="Q")
    def table_재무제표(self,YorQ="Y"):
        if YorQ in ["분기,q","분기별"]:
            YorQ = "Q" 
        elif YorQ in ["연도","연도별","년","년도별","y","year"]:
            YorQ = "Y"
        
        url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{self.code}"
        resp = requests.get(url)
        try:
            soup = BeautifulSoup(resp.text,'html5lib')
        except:
            soup = BeautifulSoup(resp.text,'html.parser')

        ##  index부분 가져오기
        sel = f"#divSonik{YorQ} > table > tbody > tr > th"
        result = soup.select(sel)
        idx = []
        for item in result:
            if item.string==None:
                child = item.select_one('div')
                if child.string == None:
                    grand_child = child.select_one('span')
                    idx.append(grand_child.string)
                else:
                    idx.append(child.string.strip())
            else:
                idx.append(item.string.strip())
        
        ## column 부분 가져오기 (연도)    
        columns = []
        sel = f"#divSonik{YorQ} > table > thead > tr > th"
        result =soup.select(sel)
        for item in result[:]:
            if YorQ == "Y":
                columns.append(item.string.strip()[:4])
            else:
                columns.append(item.string.strip())

        ## 데이터 가져오기 
        data = []
        for i in range(len(columns)-1):
            year_data =[]
            sel= f"#divSonik{YorQ} > table > tbody > tr > td:nth-child({i+2}) "    ## 바꿔가며 저장. 
            result = soup.select(sel)
            
            for item in result:
                if item.string.strip() == "":
                    year_data.append(0)
                else:
                    # year_data.append(float(item.string.strip().replace(",",""))) "문자있는경우 오류"
                    year_data.append(item.string.strip().replace(",",""))
            data.append(year_data)
    
        data_df = pd.DataFrame(data).T
        idx_df = pd.DataFrame(idx)
        df = pd.concat([idx_df,data_df],axis=1)
        df.columns = columns
        try:
            df.set_index(df.columns[0],inplace=True)
        except:
            pass
        df = df.filter(regex=r'\d+') ## 전년동기 필드 제거.
        df = df.astype('int') ## 형변형.
        return df


    def table_재무제표_to_db(data_path = './data/'):

        if data_path[-1] !="/":
            data_path += "/"
        KST = pytz.timezone('Asia/Seoul')
        today = datetime.now(KST)
        str_today = today.strftime("%Y%m%d %H:%M")
        tickers = Fnguide.get_ticker_by_fnguide(data_path)

        ## data폴더가 없으면 만들기.
        if not os.path.isdir(data_path):
            os.mkdir(data_path)


        for idx,row in tickers[2:].iterrows():
        # for idx,row in tickers[172:].iterrows(): ## 테스트 용.

            code = row['cd'][1:]
            code_name = row['nm']
            print('=='*15)
            print( idx, code_name )
            
            for i in range(5):
                try:
                    print('데이터가져오기 시도.')
                    fn = Fnguide(code)
                    df = fn.재무제표_Y
                except Exception as e:
                    print('오류발생 5초간 대기. ',i)
                    time.sleep(5)
                    df = pd.DataFrame()
                    continue
                    
                else:
                    print(i,'데이터 스크래핑 성공')
                    break
                
            if len(df)==0:
                print(f'{code_name} 데이터 가져오기 실패. 일단 스킵.')
                continue
            
            conn = sqlite3.connect(f'{data_path}fn재무제표.db')
            
            tableName = 't_' + code
            query = 'select * from ' + tableName
            
            try:
                with conn:
                    df.to_sql(tableName,conn,if_exists = 'replace')
                    print('db 새로 저장 성공!')
            except Exception as e:
                print('db 새로 저장 실패',e)
                print(idx,'정지')
                # break  # 오류나면 정지
                continue  ## 오류나도 계속 다음 진행
            finally:
                conn.close()

        

    def get_table_재무제표_from_db(code,data_path = './data'):

        tableName = 't_' + code
        query = 'select * from ' + tableName
        if data_path[-1] !="/":
                data_path += "/"
        conn = sqlite3.connect(f'{data_path}fn재무제표.db')
        try:
            with conn:
                pre_df = pd.read_sql(query , con = conn,index_col='IFRS')
                
                pre_df = pre_df.astype('int')  ## 형 변형.
                
                # print('기존데이터 가져오기 성공')
        except Exception as e:
            print(e)
            pre_df = pd.DataFrame()
            ## db에 저장하고 continue
        finally:
            conn.close()
        
        return pre_df


    def graph_재무제표(self):
        '''
        포관손익계산서, 재무상태표,현금흐름표 순으로 튜플로 반환.
        '''
        url = f"https://comp.fnguide.com/SVO2/json/chart/03/chart_A{self.code}_D.json?_=1607609713536"
        resp = requests.get(url)
        # encoding 찾아 변경하기
        ec = chardet.detect( resp.text.encode()  )['encoding']
        resp.encoding = ec
        
        result= resp.json()
        
        포괄손익계산서 = {}
        for v in result['01_Y']:
            포괄손익계산서[v['GS_YM'][:4]] = [
                float(v['VAL1'].replace(",","").replace("-","")),
                float(v['VAL2'].replace(",","").replace("-","")),
                float(v['VAL3'].replace(",","").replace("-","")),
                float(v['VAL4'].replace(",","").replace("-","")),
            ]
        포괄손익계산서_col= [] 
        for v in result['01_Y_H']:
            포괄손익계산서_col.append(v['NAME'])
        포괄손익계산서 = pd.DataFrame(포괄손익계산서).T
        포괄손익계산서.columns = 포괄손익계산서_col

        재무상태표 = {}
        for v in result['03_Y']:
            재무상태표[v['GS_YM'][:4]] = [
                float(v['VAL1'].replace(",","")),
                float(v['VAL2'].replace(",","")),
                float(v['VAL3'].replace(",","")),
                float(v['VAL4'].replace(",","")),
            ]
        재무상태표_col= [] 
        for v in result['03_Y_H']:
            재무상태표_col.append(v['NAME'])
        재무상태표 = pd.DataFrame(재무상태표).T
        재무상태표.columns = 재무상태표_col

        현금흐름표 = {}
        for v in result['06_Y']:
            현금흐름표[v['GS_YM'][:4]] = [
                float(v['VAL1'].replace(",","")),
                float(v['VAL2'].replace(",","")),
                float(v['VAL3'].replace(",","")),
             ]
        
        현금흐름표_col= [] 
        for v in result['06_Y_H']:
            현금흐름표_col.append(v['NAME'])
        현금흐름표 = pd.DataFrame(현금흐름표).T
        현금흐름표.columns = 현금흐름표_col

        return 포괄손익계산서,재무상태표,현금흐름표

    def get_ticker_by_fnguide( data_path='/home/sean/sean/data/' , option = "web" ):
        '''
        Fnguide 에서 ticker정보 크롤링 함수.
        df로 반환
        option : "web"(default) contain save db , "db"
        
        '''
        if data_path[-1]!="/":
            data_path += "/"
            
        # data폴더가 없으면 만들기. 
        if not os.path.exists(data_path):
            os.mkdir(data_path)
            print(f'maked {data_path} directory')
        
        file_name = f'{data_path}code_df.db'
        table_name = 'code_df'
        
        if option =="web":
            # df = pd.DataFrame()
            all_ls = []
            
             ## 레버리지, 인버스 추가. 
            dic = {"cd":["A122630","A252670"] , "nm" : ["KODEX 레버리지","KODEX 200선물인버스2X"],"gb":["ETF","ETF"]}
            # df = pd.DataFrame(dic).append(df).reset_index(drop=True)
            all_ls.append(pd.DataFrame(dic))
            
            for mkt_gb in [2,3]:
                url = f"http://comp.fnguide.com/SVO2/common/lookup_data.asp?mkt_gb={mkt_gb}&comp_gb=1"
                resp = requests.get(url)
                data = resp.json()
                all_ls.append(pd.DataFrame(data))
                # df = df.append(pd.DataFrame(data))
                # df = df[df['nm'].str.contains('스팩') == False] # 스팩 제외
            
           
            df = pd.concat(all_ls)
            df = df.reset_index(drop=True)
            df = df[df['nm'].str.contains('스팩') == False] # 스팩 제외
            

            ## 관리종목 거래정지종목 제외하기 
            try:
                url = "https://finance.naver.com/sise/trading_halt.naver" # 거래정지
                거래정지 = pd.read_html(url,encoding='cp949')[0].dropna()
                url = "https://finance.naver.com/sise/management.naver" #관리종목
                관리종목 = pd.read_html(url,encoding='cp949')[0].dropna()
                stop_ls = list(관리종목['종목명']) + list(거래정지['종목명']) # 관리종목 정지종목 리스트 
                print(f'총 {len(stop_ls)}개의 거래정지 관리종목을 제외')
                df =  df[~df['nm'].isin(stop_ls)]           ## 제외하기.
            except:
                pass
            
            df = df.reset_index(drop=True)
            

            ## 코드정보 db로 저장.     
            con = sqlite3.connect(file_name)
            with con:
                df.to_sql(table_name,con,if_exists='replace',index=False)
            
            return df
        
        else :
            try:
                con = sqlite3.connect(file_name)
                query = f"select * from '{table_name}'"
                df = pd.read_sql(query,con)
                
                return df
            except:
                print('데이터베이스 파일오류')
                return pd.DataFrame()
            
    def report_info_by_fnguide(date=None,stext=None):
        '''
        Fnguide 요약 리포트 가져오기 . 
        date : 날짜지정
        stext : 검색어 지정(ticker)
        df으로 반환
        '''

        if date == None:
            date = datetime.now(KST).date().strftime("%Y%m%d")
            # date = datetime.today().strftime("%Y%m%d")
        if stext == None:
            stext = ""
        
    
        url = "http://comp.fnguide.com/SVO2/ASP/SVD_Report_Summary_Data.asp"
    
        params = {"fr_dt":date,
                "to_dt":date,
                "stext":stext,
                "check":'all',
                }

        r  = requests.get(url,params= params)
        html = r.text
        soup = BeautifulSoup(r.text,'html.parser')

        tags_gpbox = soup.findAll('span', {'class': 'gpbox'})
        tags_title = soup.find_all('span',{'class':'txt2'})

        tags = soup.findAll('dl', {'class': 'um_tdinsm'})

        data = []

        for i,tag in enumerate(tags):
            data_dic = {}
            name = tag.find('a').text
            data_dic['code_name'] = name.split()[0]
            data_dic['acode'] = name.split()[1]
            temp_text = tags_title[i].text.strip()

            for dd in tag.find_all('dd'):
                temp_text = temp_text + '\n' + dd.text.strip()
            data_dic['description'] = temp_text.strip()
            data_dic['comment'] = tags_gpbox[::3][i].text.strip()  ## 투자의견
            data_dic['target_price'] = tags_gpbox[1::3][i].text.strip()  ## 목표가
            data_dic['reporter'] = tags_gpbox[2::3][i].text.strip()  ## 투자의견
            data.append(data_dic)

        report_df = pd.DataFrame(data)
        report_df['reporting_date']=date
        return report_df


class Thinkpool:
    
    def _get_code_list_by_issue_num(num,issue_name,title,content,issue_date):
        '''
        theme별 code_list 가져오기.
        '''
        ## 이슈번호로 이슈 포함 종목 가져오기. 
        data_list = []
        pno = 1
        while True:
            sub_params = {"issn":f'{num}',
                        'pno': {pno}}
            sub_url = "https://api.thinkpool.com/analysis/issue/ralatedItemSummaryList"
            r_code = requests.get(sub_url,params = sub_params)
            time.sleep(1)
            
            pno +=1
            js = r_code.json()
            try:
                if js['totalCount'] // 10 ==0:
                    cnt_page = js['totalCount'] // 10
                else:
                    cnt_page = js['totalCount'] // 10 + 1
            except:
                print(js)
                
                break 
        
            for dic in js['list']:
                temp_dic ={}

            
                temp_dic['이슈'] = issue_name
                temp_dic['iss_date'] = issue_date
                
                temp_dic['title'] = title
                temp_dic['contents']= content
                
                temp_dic['code'] = dic.get('code')
                temp_dic['code_name'] = dic.get('codeName')
                temp_dic['기업개요'] = dic.get('overview')
                try:
                    temp_dic['다른이슈'] = ",".join([a['is_str'] for a in dic.get('otherIssueList')])
                except:
                    temp_dic['다른이슈'] = ""
                data_list.append(temp_dic)
                print(".",end = " ")

            if cnt_page < pno:
                break
        return pd.DataFrame(data_list)
    
    def _get_issue_list(n=1):
        '''
        최근 닐짜 이슈 데이터 가져오기. 
        n : 최근날짜개수.
        전체 : 1000
        
        '''
        # year = datetime.today().year
        year = datetime.now(KST).date().year
        
        i = 0 
        
        url = "https://www.thinkpool.com/analysis/issue/recentIssue"
        params = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36",
            "Referer" : "https://www.google.com/"
                }
        resp = requests.get(url,params=params)
        try:
            soup = BeautifulSoup(resp.text, 'html5lib')        
        except:
            soup = BeautifulSoup(resp.text, 'html.parser')
        time.sleep(1)

        base_url = "http://www.thinkpool.com"
        tables = soup.select("#content > div > div > div > table > tbody > tr")
        
        data_list = []
        
        for table in tables[:]:
            dic = {}
        #     print(table)
            try:
                date,weekday = table.select("td.fst")[0].text.strip().split()
                i+=1 # 날짜있을때매다 숫자증가.
                if i > n: #
                    break
            except:
                pass

            dic['날짜']= date
            dic['요일']= weekday

            이슈이름 = table.select("td.text-center")[0].text.strip()
            dic['이슈']= 이슈이름

            sub_url = base_url + table.select('a.obj')[0]['href'].strip()
            dic['issue_link']= sub_url


            이슈번호 = table.select('a.obj')[0]['href'].split('/')[-1].strip()
            dic['issn'] = 이슈번호

        #     이슈내용= table.select('a.obj')[0].text
        #     print(이슈내용.strip())


            ## 헤드라인정보.
            headline_url = f"https://api.thinkpool.com/analysis/issue/headline?issn={이슈번호}"
            r_headline = requests.get(headline_url)

            head_line = r_headline.json()
            time.sleep(1)

            title= head_line['hl_str']
            content  = re.findall(r"(.+)\<.+",head_line['hl_cont'])[0]
            link = re.findall("href=\"(.+)\""  ,head_line['hl_cont'] )[0]
            dic['title'] = title
            dic['content'] = content
            dic['content_link'] = link


            data_list.append(dic)
            
            
        # 합치기.
        result = pd.DataFrame(data_list)
        # 날짜 전처리.
        month = result['날짜'].str.split('/',expand=True)[0]
        day = result['날짜'].str.split('/',expand=True)[1]
        result['날짜'] = str(year) + month + day
        return result

    def _update_issue_list(data_path):
        '''
        기존파일 issue_list 업데이트. 하고 최근파일 return! 
        '''
        if data_path[-1] != "/":
            data_path += "/"
            
        
        ## 파일 존재여부 확인 . 
        file_name = f'{data_path}issue_list.pickle'
        try:
            with open(file_name, 'rb') as f:
                pre_issue_list_df = pickle.load(f)
        except:
            print('기존파일 없음 !')
            pre_issue_list_df = pd.DataFrame()
            # return None

        # 오늘날짜와 기존파일 마지막날짜 차이 구해서 새로운데이터 받는 n 변수생성하기
        today =  datetime.now(KST).date()
        if len(pre_issue_list_df):
            pre_str_date = pre_issue_list_df['날짜'].max()
            pre_date  = datetime.strptime(pre_str_date,"%Y%m%d").date()
            n = today - pre_date
            n = n.days +1
        else:
            n = 30
            
        print(f'{n}일 작업 시작...')
        

        ### 기존이슈리스트 정리할필요 있음. $@#ㅃ#$꺄ㅃ#)$쌰ㅃ 땪ㅃ{ㅑㄷㄱ렘ㄷ쟈겜ㄴㄷ게;ㄹㅎ던;해런
        ############################################################
        ##################################################################
        ###### ######   해보기  code 리스트는 잘 들어감. 이슈 파일은 업데이트 안됨. . ##########################################


        # 새로운 데이터 다운로드
        new_issue_list_df = Thinkpool._get_issue_list(n)

        ##  다운로드된거 중 기존 이슈리스트에 없는것 찾기.
        try:
            기존날짜리스트 = list(pre_issue_list_df['날짜'].unique())
            기존날짜이슈 = list(pre_issue_list_df['이슈'].unique())
            최근이슈날짜 = max(기존날짜리스트)
            
        except:
            기존날짜리스트,기존날짜이슈 = [],[]
        cond_date = new_issue_list_df['날짜'].isin(기존날짜리스트)
        cond_issue = new_issue_list_df['이슈'].isin(기존날짜이슈)
        all_cond = cond_date & cond_issue
        new_issue_list_df_add = new_issue_list_df.loc[~all_cond]
        
        print('추가이슈 데이터:')
        try:
            print(new_issue_list_df_add[['날짜','이슈']])
        except:
            pass
        
        # 합치기. 전처리(sorting)
        concat_df = pd.concat([new_issue_list_df_add,pre_issue_list_df ])
        concat_df = concat_df.sort_values('날짜',ascending = False).reset_index(drop=True)
        concat_df = concat_df.drop_duplicates(['날짜','이슈'],keep='first')
        
        ## make folder 
        if not os.path.exists(data_path):
            os.mkdir(data_path)
            print(f'maked {data_path} directory')

        # 다시 pickle저장. 
        with open(file_name, 'wb') as f:
            pickle.dump(concat_df,f,protocol=pickle.HIGHEST_PROTOCOL)
        print('Update 완료')
        
        
        return new_issue_list_df_add
        
    def update_issue_code(data_path):
        # 기존파일 불러와서 
        # # 업데이트도 하고. 새로운 이슈파일 받아서. 
        if data_path[-1] != "/":
            data_path += "/"
        
        
        file_name = f'{data_path}issue_code_list.pickle'
        new_issue_list = Thinkpool._update_issue_list(data_path)
        
        try:
            with open(file_name, 'rb') as f:
                pre_issue_code_list = pickle.load(f)
            
            ## 추가할 이슈정보 . 기존에 없는데이터만 추출
            기존날짜리스트 = list(pre_issue_code_list['iss_date'].unique())
            기존날짜이슈 = list(pre_issue_code_list['이슈'].unique())
            최근이슈날짜 = max(기존날짜리스트)
            cond_date = new_issue_list['날짜'].isin(기존날짜리스트)
            cond_issue = new_issue_list['이슈'].isin(기존날짜이슈)
            all_cond = cond_date | cond_issue   ### | 로 하냐 & 로 하냐. ?? 
            new_issue_list_df_add = new_issue_list.loc[~all_cond]
        
        except:
            print('기존파일 없어서 모두 새로 모두 작업.')
            pre_issue_code_list = pd.DataFrame()
            with open(f'{data_path}issue_list.pickle' , 'rb') as f: 
                new_issue_list_df_add = pickle.load(f)


        
        # 새로운이슈 존재하면 종목정보 크롤링하기.
        if len(new_issue_list_df_add):  #새로운데이터 존재하면
            all_code_list_df = []
            for _, row in new_issue_list_df_add.iterrows():
                iss_name  = row['이슈']
                issn = row['issn']
                issue_date = row['날짜']
                content_link = row['content_link']
                title = row['title']
                content = row['content']

                print(issue_date, issn,iss_name,title)
                code_list_df = Thinkpool._get_code_list_by_issue_num(issn,iss_name,title, content,issue_date)
                all_code_list_df.append(code_list_df)
                time.sleep(1)
                print(".")    
            new_code_list = pd.concat(all_code_list_df)

            # 크롤링한거 모두 합침.
            all_code_list = pd.concat([pre_issue_code_list,new_code_list])

            # 날짜로 소팅.
            all_code_list = all_code_list.sort_values(['iss_date'], ascending= False)
            # 중복제거 drop_dupplicted 날짜 , 이슈, issn, 종목(종목코드)
            all_code_list = all_code_list.drop_duplicates(['iss_date','이슈','code'])
            
            ## make folder 
            if not os.path.exists(data_path):
                os.mkdir(data_path)
                print(f'maked {data_path} directory')

            # 다시 피클에 저장하기.
            with open(file_name, 'wb') as f:
                pickle.dump(all_code_list,f,protocol=pickle.HIGHEST_PROTOCOL)
        
        else:
            print(f'{최근이슈날짜} 일 이후 새로운 데이터가 없습니다.')

        return new_issue_list_df_add
        

class Investor:
    
    def get_investor(date= None):
        '''
        특정날짜 전체 수급정보.
        '''
        if date == None:
            KST = pytz.timezone('Asia/Seoul')
            date = datetime.now(KST).date()
            str_date = pd.Timestamp(date).strftime("%Y%m%d")
        else:
            str_date = pd.Timestamp(date).strftime("%Y%m%d")
        
        text = '개인/외국인/기관합계/금융투자/투신/연기금/보험/사모/은행/기타금융/기타법인/기타외국인'
        investor_ls = text.split("/")

        datas = []
        for investor in investor_ls:
            try:
                temp = pystock.get_market_net_purchases_of_equities(str_date, str_date, "ALL", investor)
            except Exception as e:
                print(f'에러발생:{e}')
                return pd.DataFrame()
            print( investor , end = " ")
        
            temp['날짜'] = pd.Timestamp(str_date).date()
            temp['투자자'] = investor
            if len(temp)==0:
                print('데이터없음')
                break
            temp  = temp.reset_index().set_index(['티커','종목명','날짜','투자자'])
            datas.append(temp)

            time.sleep(0.5)
        
        df = pd.concat(datas)
        return df


    def investor_to_db(data_path , start_date = None):
        '''
        start_date ~ today
        start_date == None : db확인해서 이후날짜부터 ~ today 까지 작업.
        start_date != None : start_date ~ today 까지 작업.
        '''
        KST = pytz.timezone('Asia/Seoul')
        today = datetime.now(KST).date()
        
        if data_path[-1] != "/":
            data_path += "/"
            
        
        file_name = f'{data_path}new_investor.db'
        
        ## make folder 
        if not os.path.exists(data_path):
            os.mkdir(data_path)
            print(f'maked {data_path} directory')
        
        
        if start_date == None:
            sql = "select max(날짜) from 'investor' "
            con = sqlite3.connect(file_name)
            try:
                with con:
                    max_date = pd.read_sql(sql, con,)
                start_date = pd.Timestamp(max_date.iloc[0,0])
            except:
                start_date = datetime.today() - timedelta(days=90)
            
            start_str_date = start_date.strftime('%Y%m%d')
            end_str_date = today.strftime('%Y%m%d')
        
        else:
            start_date = pd.Timestamp(start_date)
            start_str_date = start_date.strftime('%Y%m%d')
            end_str_date = today.strftime('%Y%m%d')
        
        date_range = pd.date_range(start =start_str_date , end =end_str_date,freq="B")
        print(date_range , '작업중..')
        datas = []
        for date in date_range:
            print(date,'데이터 다운로드시작.')
            temp = Investor.get_investor(date)
            if len(temp):
                datas.append(temp)
        print(f'datas 개수 {len(datas)}')
        
        if len(datas):        
            result = pd.concat(datas).reset_index().fillna(0)
            
            now = datetime.now(KST)
            cond_time = 14 < now.hour <= 16
            is_holiday = Sean_func.is_holiday()
            if len(result)==0 & cond_time & (not is_holiday):
                # 데이터가 없고 휴일이 아니고 시간조건이 맞으면 5분후 재실행. 
                time.sleep(5* 60)
                Investor.investor_to_db()
                print('데이터가 없어 대기후 재실행 예정!')
        else:
            print('datas정보가 없어 프로그램 종료')
            return
        
        # 임시 pickle 저장.
        temp_pickle_file_name = "temp_investor.pkl"
        try:
            if len(result):
                with open(f'{data_path}{temp_pickle_file_name}','wb') as f:
                    pickle.dump(result, f,protocol=pickle.HIGHEST_PROTOCOL)
                    print(f'{data_path}{temp_pickle_file_name} 로 저장완료')
        except:
            print('pkl저장 실패')
        
        try:
            if len(result):
                # 기존데이터 있으면 지우기. 
                print('data saving....')
                con = sqlite3.connect(file_name)
                del_date_ls = [date.strftime("%Y-%m-%d") for date in date_range]
                for str_date in del_date_ls:
                    try:
                        print(str_date , 'db파일 지움')
                        sql = f"delete  from 'investor' where 날짜 = '{str_date}'"
                        with con:
                            cursor = con.cursor()
                            cursor.execute(sql)
                            con.commit() ### 지울땐 커밋을 꼭  해야한다.!
                    except:
                        pass
                    finally:
                        con.close()
                        
                ## old data delete idea!
                
                # old_date = today-timedelta(days = 6*30)  ## 6개월
                # old_date = old_date.strftime("%Y-%m-%d")
                # file_name = f'{data_path}new_investor.db'
                # con = sqlite3.connect(file_name)
                # sql = f"delete  from 'investor' where 날짜 < '{old_date}'"
                # with con:
                    # cursor = con.cursor()
                    # cursor.execute(sql)
                    # con.commit()
                
                
                # 데이터 추가하기. 
                con = sqlite3.connect(file_name)
                with con:
                    result.to_sql('investor',con, if_exists="append", index=False)
                    print('새로운데이터 저장성공.')
                    
                
                ## 저장완료되었다면. 임시저장했던 pickle파일 제거. 
                # try:
                #     os.remove(f"{data_path}{temp_pickle_file_name}")
                # except:
                #     print(f'{temp_pickle_file_name} 파일 (임시파일) 지우기 실패! ')
                
                
        except Exception as e:
            print('db저장은 실패함.')
            print(e)
    
    def get_순매수순위(investor = '외인기관합계', start_date=None, end_date=None):
        '''
        start_date 지정날짜부터 현재까지 데이터 반환 시총대비순매수비율로 sorting.
        investor : 
        외인기관합계 / 금융투자 / 보험 / 투신 / 사모 / 은행 / 기타금융 / 연기금 / 기관합계 / 기타법인 / 개인 / 외국인 / 기타외국인 / 전체
        start_date : "20210901"
        '''
        # 오전 12시부터 만약 오후 4시 사이면 오늘이 어제 
        KST = pytz.timezone('Asia/Seoul')
        today = datetime.now(KST)
        if today.hour < 16:
            today = today - timedelta(days=1)


        
        today_str = today.strftime("%Y%m%d")

        if (start_date == None) & (end_date == None):
            start_date = today.strftime("%Y%m%d")
            end_date  = start_date
            # print('1')
        ## end_date 만 있는경우. 
        elif (start_date == None) & (end_date != None):
            if type(end_date) == str:
                end_date = pd.to_datetime(end_date).date()
            end_date = end_date.strftime("%Y%m%d")
            if end_date > today_str:
                end_date = today_str
            start_date = end_date
            # print('2')
        ## start_date만 있는경우
        elif (start_date != None) & (end_date == None):
            if type(start_date) == str:
                start_date = pd.to_datetime(start_date).date()
            start_date = start_date.strftime("%Y%m%d")
            end_date = today.strftime("%Y%m%d")
            # print("3")
        ## 둘다 있는 경우.    
        else:
            if type(start_date) == str:
                start_date = pd.to_datetime(start_date).date()
            if type(end_date) == str:
                end_date = pd.to_datetime(end_date).date()
            
            start_date = start_date.strftime("%Y%m%d") 
            end_date = end_date.strftime("%Y%m%d")  
            if end_date > today_str:
                end_date = today_str
            if start_date > today_str:
                start_date = today_str

            # print("4")
        # print('start_str',start_date)
        # print('today_str',end_date)

        if end_date > today_str:
            end_date = today_str
            # print('end_date 수정함. ')

        시가총액_data = pystock.get_market_cap_by_ticker(end_date)
        전체등락율 = pystock.get_market_ohlcv_by_ticker(end_date,market='ALL')
        
        if investor =='외인기관합계':
            df_외인 = pystock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, "ALL", '외국인')
            df_기관 = pystock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, "ALL", '기관합계')
            # df = df_외인 + df_기관
            종목명 = pd.concat([ df_외인.종목명.reset_index(),df_기관.종목명.reset_index() ]).drop_duplicates().set_index('티커')
            # df['종목명']=종목명
            종목명['외인_순매수거래량'] = df_외인['순매수거래량']
            종목명['외인_순매수거래대금'] = df_외인['순매수거래대금']
            종목명['기관_순매수거래량'] = df_기관['순매수거래량']
            종목명['기관_순매수거래대금'] = df_기관['순매수거래대금']
            종목명 = 종목명.fillna(0)
            종목명['순매수거래량'] = 종목명['외인_순매수거래량'] +종목명['기관_순매수거래량']
            종목명['순매수거래대금'] = 종목명['외인_순매수거래대금'] +종목명['기관_순매수거래대금']
            df = 종목명[['종목명','순매수거래량','순매수거래대금']].copy()
            
            # print('외인기관합계 실행')
        else:
            
            df = pystock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, "All", investor)
            # print('개별 실행')
        ## 합쳐야할 게 있다면 따로 뽑아 더한 후  데이터 정리해야함 .  
        df = df.filter(regex = "종목명|순매수",axis=1)
        
        # 계산
        df['시가총액']= 시가총액_data['시가총액']
        df['현재등락율']=(전체등락율['등락률']).round()
        df['순매수금/시가총액'] = (df['순매수거래대금']/df['시가총액'] *100).round(2)
        df = df.sort_values(by="순매수금/시가총액",ascending=False)
        
        # 정리
        df['순매수거래량'] =df['순매수거래량'].astype(int) 
        df['시가총액'] = (df['시가총액']/100000000).round()
        df['순매수거래대금'] = (df['순매수거래대금']/100000000).round()
        df.rename(columns = {'시가총액':'시가총액_억', '순매수거래대금':'순매수거래대금_억'},inplace = True)

        
        df.columns.name = f"{start_date}~"
        print('날짜체크' ,start_date, end_date)
        return df

    
    
    def arrange_investor_lastdays(data_path, last_date = None):
        '''
        # 시총대비 수급 정리
        '''
        if data_path[-1] != "/":
            data_path += "/"
            
        ### 0.8% 이상 상위 100개 추출하기. !!
        rate = 0.8 # 순매수금/ 시가총액
        top_cnt = 100 # 상위 100개
        
        if last_date == None:
            KST = pytz.timezone('Asia/Seoul')
            last_date  = datetime.now(KST).date() ## 오늘 날짜형식으로 변환
        else:
            last_date = pd.to_datetime(last_date)

        
        ls = []
        for i in [15,7,4,0]:
            try:
                start_date = last_date - timedelta(days=i)
                data = Investor.get_순매수순위(start_date=start_date , end_date= last_date)
                df_invest_tops = data[data['순매수금/시가총액']>=rate].copy() 
                df_invest_tops['rank'] = df_invest_tops['순매수금/시가총액'].rank(ascending=False)
                df_invest_tops = df_invest_tops[df_invest_tops['rank'] <= top_cnt] # 상위 100개.
                df_invest_tops.index.name = f"{start_date}~{last_date}"
                df_invest_tops.to_excel(f'{data_path}최근{i}일간수급.xlsx')
                df_invest_tops.to_pickle(f'{data_path}invest_tops_for_{i}day.pickle')
                ls.append(df_invest_tops)
            except Exception as e:
                print(f'n 일간수급 오류 {e}')
            
            time.sleep(1)
       

        return ls

class Polling:
    def __init__(self,data_path = './data/'):
        
        if data_path[-1] != "/":
            data_path += "/"
        self.data_path = data_path

        self.chat_id = "-594680691" # totobang ###  필요. !!!!!!!!!
        self.today = datetime.now().date()
        self.min = datetime.now().minute
        
        self.code_df = Fnguide.get_ticker_by_fnguide(self.data_path)
        with open('/home/sean/sean/token/sean1_bot_token.txt' , 'rt') as f:
            self.my_token = f.readlines()[0].strip() 
        # self.my_token = '5134275624:AAGC2U74_VYO-LhIjMyTbdsybKQpzjxM7ns' # Sean1_bot 
        self.bot = telegram.Bot(token = self.my_token)
        
        self.start()

    def check_time(self):
        '''
        날짜가 다르면 init파일 다시 실행하기. 
        '''
        print('check_time 실행.....')
        today = datetime.now().date()
        if today.day != self.today.day:
            # self.updater.stop_polling()
            # self.start()
            self.today = today
            self.code_df = Fnguide.get_ticker_by_fnguide(self.data_path)
            print('code_df 새로받음.')
            print('###############'*5)
            print('###############'*5)
            pass
        else:
            pass



        # 만약 날짜가 다르면 다시 로드하기. 
        


    def send_messange(self,msg):
        self.bot.sendMessage(chat_id = self.chat_id, text=msg)
    
    def send_photo(self, file_name ,caption = None):
        if caption ==None:
            caption = ''
        else:
            caption = str(caption)  
            
        bot = telegram.Bot(token=self.my_token)  # 봇을 생성합니다.
        bot.send_photo(self.chat_id, open(file_name, 'rb'),caption=caption)  # @bill_chat 으로 메세지를 보냅니다.
        return None
    
    
    def text_to_code(self,text):
        # text = '넥스원'
        cond1 = self.code_df['nm']==text
        cond2 = self.code_df['cd']==text
        finded_series = self.code_df.loc[cond1|cond2]
        
        if len(finded_series)==0: ## 원제목으로 검색실패시. 
            text = text.upper()   ## 원제목 대소문자 upper
            cond1 = self.code_df['nm']==text
            cond2 = self.code_df['cd']==text
            finded_series = self.code_df.loc[cond1|cond2]
            if len(finded_series)==1:
                finded_series = finded_series.iloc[0]
                code_name = finded_series['nm']
                code = finded_series['cd'][1:]
                gb = finded_series['gb']
                return code, code_name, gb

            else :
                pass
        if len(finded_series)==1:
            print('2')
            finded_series = finded_series.iloc[0]
            code_name = finded_series['nm']
            code = finded_series['cd'][1:]
            gb = finded_series['gb']
            return code, code_name, gb

        cond1 = self.code_df['nm'].str.contains(text)
        cond2 = self.code_df['cd'].str.contains(text)
        finded_series = self.code_df.loc[cond1|cond2]
        if len(finded_series)==0:
            print('결과없음',finded_series)
            return None,None,None
        else:
            finded_series = finded_series.iloc[0]
            code_name = finded_series['nm']
            code = finded_series['cd'][1:]
            gb = finded_series['gb']
            return code, code_name, gb
       

    # help reply function
    def _help_command(self,update, context) :
        '''
        update.message.text : /help team_name
        '''
        # bot.sendMessage(chat_id = chat_id, text=update.message.text)
        msg = "/help 도움말\n"
        msg += "/c 차트정보\n"
        
        self.bot.sendMessage(chat_id = self.chat_id, text=msg)
        # update.message.reply_text("무엇을 도와드릴까요?")
    
    def _send_chart_command(self,update,context):
        '''
        update.message.text : /c team_name
        '''
        ## 날짜부터 확인하기. !! 
        self.check_time()

        command_msg = update.message.text
        
        # 받은 메세지. 가지고 작업.
        recieve_msg = ' '.join(command_msg.split()[1:])    # 앞 명령어 제외한 텍스트 
        print("받은메세지 : ", recieve_msg)
        # recieve_msg 를 코드로 변경. 코드 > 코드 , 이름 > 코드
        try:
            codes = self.text_to_code(recieve_msg)
            print('ver : 0.2 , 받은파일명 text_to_code 결과: ' , codes)
            if codes[0]==None:
                self.send_messange('No Data')
                return
            ## Stock객체 생성. 
            s = Stock(*codes,option = 'telegram',data_path = self.data_path)
            print('객체생성 완성')
            # 점수부여.
            stocks = Stocks(data_path = self.data_path)
            s = find_stock.set_point(s,stocks)
            ## plot 받아서 임시저장. 
            fig = s.plot()
            fig1= s.plot('30분봉')
            print('차트객체 생성 성공!!!!!!!!')


            fig.savefig(f'{self.data_path}temp_day_chart.png', dpi=250)
            fig1.savefig(f'{self.data_path}temp_30min_chart.png', dpi=250)
            code = codes[0]
            code_name= codes[1]
            print('내부작업완료.')
        except Exception as e:
            print('에러 ',e)
            return
        
        ## send_photo 로 차트 보냄. 
        try:
            self.send_photo(file_name=f'{self.data_path}temp_day_chart.png' ,caption=f'{code} {code_name}')
        except:
            self.send_photo(file_name=f'{self.data_path}temp_day_chart.png' )
            
        self.send_photo(file_name=f'{self.data_path}temp_30min_chart.png')
        
        print('메세지 보내기 완료. ')

    def start(self):

        self.updater = Updater(self.my_token, use_context=True)

        ## message handler
        # message_handler = MessageHandler(Filters.text & (~Filters.command), self._get_message) # 메세지중에서 command 제외
        # updater.dispatcher.add_handler(message_handler)
       
       
        ## add command handler 
        # # 전적가져오기 /r 토트넘
        send_chart_command = CommandHandler('c', self._send_chart_command)
        self.updater.dispatcher.add_handler(send_chart_command)
        
       
        help_handler = CommandHandler('help', self._help_command)
        self.updater.dispatcher.add_handler(help_handler)
        

        ## polling start!!
        # self.updater.start_polling(timeout=3, clean=True)
        self.updater.start_polling()
        self.updater.idle()
        
        
    def stop_polling(self):
        self.updater.stop_polling()
        



class Talib():
    
    def _del_short_v(self,all_data, v_data, bong = 0):
        '''
        index만 사용함.
        all_data = Series, DataFrame , 원본데이터 , 
        v_data = Series, DataFrame , 고저점 데이터. 
        bong = 두 데이터 간격, 3 => 간격2 
        return : Series, DataFrame
        ex ) 간격이 1일인것을 무시하겠다면 bong =1 , 3일보다 같거나 적은것을 무시하겠다면 bong = 3 지정.
        '''
        
        for i in range(len(v_data)-1):
            # if i == 0:
            #     continue  
            start = v_data.index[i]
            end = v_data.index[i+1]
            distance = len(all_data.loc[start:end]) -1 # 실제차이수.
            if i == len(v_data)-1:
                return v_data

            if distance <= bong:
                v_data = v_data.loc[~v_data.index.isin([start,end])]
                # print( f"{start},{end} 2개 데이터 제거")
                return self._del_short_v(all_data, v_data, bong = bong)  ## 재귀함수.
        
        return v_data
        
    def _get_low_high(self,series1,bong = 0):
        '''
        bong : 변곡점간의 무시되는 거리. -> 장기이평에만 사용하기. 
        iter_data : list,Series ,array 
        return : df (high, low, change)
        
        
        series = 위아래 같은값 제거하지 않은 데이터. 
        series2 = 위아래 같은값을 제거한 데이터. 
        
        '''
         
        series = series1.copy()
        
        if type(series)!= pd.core.frame.DataFrame:
            series = pd.Series(series)
        
        ## 전날 같은값이면 일단 제거하고 
        series2 = series.loc[~(series==series.shift(-1))].copy()
        series2 = series2.loc[~(series==series.shift(-1))]
        
        ## 앞뒤같은값이 있다면  없어질때까지 제거.
        series2 = series.copy()
        while sum(series2==series2.shift(-1)):
            series2 = series2.loc[~(series==series.shift(-1))]
        
                
     
        ## 단순 고저점 조건지정
        high_variation_cond = ( series2.shift(-1) < series2 ) & ( series2 > series2.shift(1) )
        low_variation_cond = ( series2.shift(-1) > series2 ) & ( series2 < series2.shift(1) )

        h_index = high_variation_cond.loc[high_variation_cond==True].index
        l_index = low_variation_cond.loc[low_variation_cond==True].index
        all_index = h_index.union(l_index)
        
        ## 고저점 제외한 데이터 nan 값 지정
        # series.loc[~series.index.isin(all_index)]=np.nan
        # result_s = series.dropna()
        result_s = series.loc[series.index.isin(all_index)]
        
        try:
            result_s.name = result_s.name+"_변곡점"
        except:
            result_s.name = '변곡점'
        
        ## 가까운 데이터 제거하기. #************* series 와 series2 중 어떤데이터 를 넣어야하는가....?
        result_s = self._del_short_v(series , result_s, bong= bong)
        
        ######
        result_change = result_s.pct_change()
        try:
            result_change.name = result_change.name+"_변화량"
        except:
            result_change.name = '변화량'
        
        ## 데이터 합침
        result_df = pd.concat([result_s,result_change],axis=1)
        
        # return result_df
        return result_s,result_change
    
    def _two_ma_info(self,short,long):
        '''
        두 이평선의 gc,dc정보만 반환.
        return Series
        '''
        same_cond = short == long
        short1 = short.loc[~same_cond]
        long1 = long.loc[~same_cond]

        diff_s = short1 - long1
        gc_cond = (diff_s.shift(1)<0) & (diff_s >0)
        dc_cond = (diff_s.shift(1)>0) & (diff_s <0)

        diff_s.loc[gc_cond]='gc'
        diff_s.loc[dc_cond]='dc'
        result = diff_s[(dc_cond | gc_cond )]
        result
        return result
        
    def _add_tri_ma(self,df):
        '''
        df : ohlcv df 
        func : talib moving average method
        self.mas : 현재기준 가능한 이평선들.
        '''
        cnt = len(df)
        #     rerolling_dic = {3:2, 5:3, 10:4, 20:6, 40:8, 60:10, 120:10, 240:10, 390:10}
        arr = np.array([3,5,10,20,60,120,240])
        mas = arr[arr<=cnt]

        for ma in mas:
            str_ma = 'ma'+str(ma)
        #         df[str_ma] = talib.MA   ( talib.MA(df['Close'],ma) ,rerolling_dic[ma] )
            # low + high 중간값과 종가 의 평균.으로 하면 어떻게 차이가 나까.?
            df[str_ma] =  talib.TRIMA ( df['Close'] , ma )

        return df
    
    def _add_ma(self,df,option = '일봉'):
        '''
        df : ohlcv df kjdflkja
        func : talib moving average method
        '''
        cnt = len(df)
        if cnt <=3:
            print('data부족으로 _add_ma 실행 실패',option)
            return df
        
        ##분봉인지 확인하기.
        cond1 = ((df.index[-2]  - df.index[-3]).days == 0)  | ((df.index[-2]  - df.index[-3]).seconds == 1800)
        cond2 = ((df.index[-6]  - df.index[-7]).days == 0 ) | ((df.index[-6]  - df.index[-7]).seconds == 1800 )
        if any([cond1 , cond2]):
            option = '30분봉'
        
        
        # rerolling_dic = {3:2, 5:3, 10:4, 20:6, 40:8, 60:10, 120:10, 240:10, 390:10}
        arr = np.array([3,5,10,20,60,120,240])
        if '분봉' in option :
            arr = np.array([10,20,40,60,240])
        elif option == '주봉':
            arr = np.array([3,20,60])
        elif option == '월봉':
            arr = np.array([3,20])
        
        mas = arr[arr<=cnt]
        
        ## 분봉은 종가에 비중을 주자. 또는 다른방법. 필요 . 
        if '분봉' in option :
            ## Price
            x = ((df['High']+df['Low']) / 2 + df['Close']) / 2
        else:
            # average 기준 x : Close, ((High+Low) / 2 + Close) / 2 ,or, ((Open+Close) / 2 + Close) / 2 
            
            # x = (((df['High']+df['Low']) / 2) + df['Close']) / 2
            # print('중간값 적용')
            x = df['Close']
        
        
        for ma in mas:
            str_ma = 'ma'+str(ma)
            df[str_ma] =  talib.MA ( x , ma )
            change_str_ma = str_ma +'변화량'
            df[change_str_ma]= df[str_ma].pct_change()
            # self.low_high_df = self._get_low_high(df[str_ma])
            ########  이거 어떻게 해야 들어가냐..?????????
            
            변곡점 = f'{str_ma}_변곡점'
            변곡점_변화량 = f'{str_ma}_변곡점_변화량'
            
            ## bong 옵션에 따리 이평에 따라 값 변경 필요.!확인필요.
            ignore_bong30_dic = {3:0, 5:0, 10:1, 20:2, 40:4, 60:6, 120:10, 240:15, 390:20}
            ignore_bong_dic = {3:1, 5:3, 10:3, 20:3, 40:4, 60:6, 120:8, 240:15, 390:20}
            # ignore_bong_dic = {3:0, 5:0, 10:3, 20:3, 40:4, 60:6, 120:8, 240:15, 390:20}
            
            if '분봉' in option:
                ignore_bong = ignore_bong30_dic[ma]
                # print(f'{ma} : {ignore_bong} 적용')
            elif option == '일봉':
                ignore_bong = ignore_bong_dic[ma]
            else : 
                ignore_bong = 0
            
            self.ignore_bong = ignore_bong
            
            # print(f'{ma} : {ignore_bong}개 무시 적용')
            df[변곡점] , df[변곡점_변화량] = self._get_low_high(df[str_ma], bong= ignore_bong)
            ## 전저고점 설정
            # print('정제된 변곡점 data')
            # print(df[변곡점].dropna())
            df['temp_전저점'] , _ = talib.MINMAX(df['Low'],ma)
            _ , df['temp_전고점'] = talib.MINMAX(df['High'],ma)

            
            cond = df[변곡점].dropna()
            # print('정제된데이터 조건')
            # print(cond)
            전저고점 = f'{str_ma}_전저고점'
            df.loc[cond.index,전저고점] = df.apply(lambda x : x['temp_전고점'] if x[변곡점_변화량]>0 else x['temp_전저점'], axis=1 )

            df = df.drop(['temp_전저점','temp_전고점'],axis=1)
            
            
        ## Volume
        if max(mas) > 20: # 20개초과일때
            for ma in [20, max(mas)]:
                str_ma = 'vol'+str(ma)+"ma"
                df[str_ma] = talib.MA ( df['Volume'], ma)
        else: # 20개 안되면. 
            # str_ma = 'vol'+str(max(mas))+"ma"
            # df[str_ma] = talib.MA ( df['Volume'], ma)
            for ma in [20, max(mas)]:
                str_ma = 'vol'+str(ma)+"ma"
                df[str_ma] = np.nan

        ## pre vol
        if len(df)>2:
            df['pre_V'] = df['Volume'].shift(1)
            df[ 'pre_pre_V'] = df['Volume'].shift(2)
        else:
            df['pre_V'] = np.nan
            df[ 'pre_pre_V'] = np.nan
        
        ## bb 추가
        try:
            df['upper_bb'] ,_ , df['lower_bb']= talib.BBANDS(df['Close'],max(mas),2,2)
            df['coke_width']= df.apply(lambda x : ((x["upper_bb"] / x["lower_bb"])-1)*100,axis=1 )
        except:
            df['upper_bb'] ,_ , df['lower_bb']= np.nan
            df['coke_width']= np.nan
        
        try:
            if 60 in mas:
                df['upper_bb_60'] ,_ , df['lower_bb_60']= talib.BBANDS(df['Close'],60,2,2)
            else:
                df['upper_bb_60'] ,_ , df['lower_bb_60']= np.nan
        except:
            pass
        
        ## sun 추가
        sun_df = pd.DataFrame()
        try:
            # for i in range(10,max(mas)+1,10):  
            for i in range(10,min(max(mas),200)+1,10):   ## 최고 200까지만 보기로 함. 
                sun_df[i]= talib.MA(df['Close'],i)
                
            df['sun_max'] = sun_df.apply(lambda x : max(x[sun_df.columns]) , axis = 1)
            df['sun_min'] = sun_df.apply(lambda x : min(x[sun_df.columns]) , axis = 1)
            df['sun_width'] = df.apply(lambda x : ((max(x['sun_min'],x['sun_max']) / min(x['sun_min'],x['sun_max']))-1)*100 , axis = 1)
            df['sun_location']= df.apply(lambda x : ( x['Close'] - x['sun_min'] ) / ( x['sun_max'] - x['sun_min'] )*100 if x['sun_max'] - x['sun_min']!=0 else 0 ,axis=1) 
        except:
            df['sun_max'] = np.nan
            df['sun_min'] = np.nan
            df['sun_width'] = np.nan
            df['sun_location'] = np.nan
        
        # rsi
        df['rsi'] = self._get_rsi(df)
        ## psar
        df['psar'] = self._get_psar(df)
        ## trix
        df[['trix','trix_sig']] =  self._get_trix(df)
        
        ## adx 
        df['adx'] = self._get_adx(df)
        
        return df
    
    def _get_매물대(self,df,parts=10, period=240):
        '''
        df : ohlcv , parts : 구분
        return : list 240거래일기준 가장많은 매물대 1, 2위 가격.
        '''
        df= df[-period:].copy()
        df = df[['Close','Open','High','Low','Volume']]
        all_v = df['Volume'].sum()
        df['rate_v']=df['Volume']/all_v
        # df['mprice']= (df['High'] + df['Low']) /2
        df['mprice']= (df['High'] + df['Low']+df['Open']+df['Close']) /4
        max_price = df['High'].max()
        min_price = df['Low'].min()
        bins = np.linspace(min_price,max_price,parts)
        labels = [  str(int((bins[i]+ bins[i+1])/2)) for i in range(len(bins)-1)   ]
        df['bin'] = pd.cut(df['mprice'],bins=bins, labels=labels)
        result = df.groupby('bin').sum()['rate_v'].sort_values(ascending=False)
   
        return [int(index) for index in result.index[0:2]]
    
    def _get_rsi(self,df,period = 11):
        ## rsi ## pi daq period 구분 필요.!
        if self.gb in ['KOSPI',"유가증권시장",'코스피',"1"]:
            period = 10
        elif self.gb in ['KOSDAQ','코스닥',"2"]:
            period = 12
        else:
            period = 11
        df['rsi'] = talib.RSI(df['Close'],period)
        return df['rsi']
   
    def _get_psar(self,df,acceleration=0.3,maximum=3.0):
        df['psar']= talib.SAR(df['High'],df['Low'],acceleration, maximum)
        return df['psar']
    
    def _get_trix(self, df , trix_period = 7, trima_period  = 5 ):
        '''
        return : df
        '''
        df['trix'] = talib.TRIX(df['Close'],trix_period)
        df['trix_sig'] = talib.TRIX(df['Close'],trima_period)
        return df[['trix','trix_sig']]
    
    
    def _get_adx(self, df, period=9):
        df['adx'] = talib.ADX(df['High'],df['Low'],df['Close'],period)
        return df['adx']
    
    def _get_dmi(self,df,period = 14):
        df['pdm'] = talib.PLUS_DM(df['High'],df['Low'],period)
        df['pdi'] = talib.PLUS_DI(df['High'],df['Low'],df['Close'],period)
        return df[['pdm','pdi']]
    
class Anal_tech(Talib):
    '''
    return :tuple , 결과값과 그에 따른 내용 dict 로 반환하기.
    '''
    
    def cal_candle_rate(df,date = None, n =1):
        '''
        date: index, n : if date ==None n번째봉.
        return : dict , value : float
        '''
        
        if date ==None:
            date = df.index[-n]
        date = pd.to_datetime(date)
        try:
            o,h,l,c = df.loc[date,['Open','High','Low','Close']]
            all_range = h - l

            몸통비율 = (max(o,c) - min(o,c)) / all_range  *100 
            아래꼬리비율 = (min(o,c) - l) / all_range *100 
            윗꼬리비율 = (h - max(o,c) ) / all_range  *100 
            총변동율 = ((h / l)- 1 ) * 100 
            몸통변동율 = ((c / o)- 1 ) * 100 

            dic = {"몸통비율":몸통비율,
                "아래꼬리비율":아래꼬리비율,
                "윗꼬리비율":윗꼬리비율,
                "총변동율":총변동율,
                "몸통변동율":몸통변동율,}
        except Exception as e:
            dic = {"몸통비율":0,
            "아래꼬리비율":0,
            "윗꼬리비율":0,
            "총변동율":0,
            "몸통변동율":0,}
            # print(e,o,h,l,c)
        return dic  
  
    def get_ab_v_rate(self):
        if 'info_ab_volume' not in dir(self):
            return 0
        if (self.info_ab_volume['ma20']['기준날짜'] == None) & (self.info_ab_volume['big_v']['기준날짜'] == None) :
            result = 0
        else:
            if self.info_ab_volume['ma20']['기준날짜'] == None:
                if self.info_ab_volume['big_v']['기준날짜'] != None:
                    result = self.info_ab_volume['big_v']['ab_v_rate']
            else:
                if self.info_ab_volume['big_v']['기준날짜'] == None:
                    result = self.info_ab_volume['ma20']['ab_v_rate']
                else:

                    if self.info_ab_volume['ma20']['경과일'] <= self.info_ab_volume['big_v']['경과일']:
                        result = self.info_ab_volume['ma20']['ab_v_rate']
                    else:
                        result = self.info_ab_volume['big_v']['ab_v_rate']

                    if abs(self.info_ab_volume['ma20']['경과일'] - self.info_ab_volume['big_v']['경과일']) <10:
                        result = max (self.info_ab_volume['big_v']['ab_v_rate'] , self.info_ab_volume['ma20']['ab_v_rate'])

        return result    
    
    
    
    
    def _get_ma_info(self,df ,ma):
        '''
        작업중. 특정이평의 정보추출하기.
        '''
        empty_result = {
            '이평':np.nan,
            '현재방향': np.nan,
            '변곡지속일': np.nan,
            '이격도min' : np.nan,
            '이격도max' : np.nan,
            '가격이격도': np.nan,
            '현재저점이격도': np.nan,
            '저점개수': np.nan,
            '저점추세': np.nan,
            '고점개수': np.nan,
            '고점추세': np.nan,
            '저고점': np.nan,
            '고저점': np.nan,
            '이평현재위치': np.nan,  ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
            '가격현재위치': np.nan, ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
            '진동폭':np.nan,
            '저점날짜' :  [] ,  ## 저점날짜들 역순 입력.
            '고점날짜' : []
            }
        #
        
        if ma not in df.columns:
            # print(f'{self.code_name} {ma} ma가 존재하지 않습니다.')
            return empty_result
        
        regex_ = 'Close|'+str(ma)
        temp_df = df.filter(regex=regex_).copy()
        변곡df = temp_df[temp_df[f"{ma}_변곡점"].notnull()]
        
        ## 최대, 최소 가격이평 이격도
        max_dist = round((temp_df['Close'] / temp_df[ma] ).max() *100,1)
        min_dist = round((temp_df['Close'] / temp_df[ma] ).min() * 100,1)
        
        current_ma_value = temp_df[f'{ma}'][-1] ## 현재이평값
        current_price = temp_df['Close'][-1] ## 현재가
        현재가격이격도 = round(current_price / current_ma_value * 100,2)
        현재저점이격도 = round(df['Low'][-1] / current_ma_value * 100,2)
        변화량 = temp_df.iloc[-1][f'{ma}변화량']
       
        ### 변화량이 0이면 이전의 추세를 따른다.!
        i =1
        while 변화량 == 0:
            변화량 = temp_df.iloc[-i][f'{ma}변화량']
            i+=1
            # print(f'변화량 추적{i}  {temp_df.iloc[-i].index}')
        현재방향 = '상승' if 변화량 > 0 else '하락'
        
        if len(변곡df):
            변곡지속일 = len(temp_df.loc[변곡df.index[-1]:])-1
        else:
            변곡지속일 = len(temp_df)
        
        
        # cnt # 저점 연속. 1 -> v, 2 -> w ,3 -> vw
        # date = 저점 날짜리스트.[최근날짜 순서로.]
        
        ## 저점추세. ############################################
        cond_저점 = temp_df[f'{ma}_변곡점_변화량'] < 0 
        if sum(cond_저점) <= 1:
            # print(f'{self.code_name} {ma} 변곡정보부족')
            result_dic = {
            '이평':ma,
            '현재방향': 현재방향,
            '변곡지속일': 변곡지속일,
            '이격도min' : min_dist,
            '이격도max' : max_dist,
            '가격이격도': 현재가격이격도,
            '현재저점이격도': 현재저점이격도,
            '저점개수': 0,
            '저점추세': np.nan,
            '고점개수': np.nan,
            '고점추세': np.nan,
            # '저고점': np.nan,
            # '고저점': np.nan,
            '이평현재위치': np.nan,  ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
            '가격현재위치': np.nan, ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
            '진동폭':np.nan,
            '저점날짜' :  [] ,  ## 저점날짜들 역순 입력.
            '고점날짜' : []
            }
            return result_dic
        #########################################################    
        cnt  = 1 ## 저점개수
        date = [] ## 저점날짜.
        date.append(temp_df[cond_저점][f'{ma}_변곡점'].index[-(cnt)])
        
        ## 전저점비교할때 앞의저점은 전 최저점을 기준으로 함. idea , **** 중요.
        cnt_low = len(temp_df.loc[cond_저점]) 
        
        while cnt_low - cnt:  ## 0.99 - > 1% 까지는 허용.
            # if temp_df[cond_저점][f'{ma}_변곡점'].iloc[-(cnt)]  >= temp_df[cond_저점][f'{ma}_변곡점'].iloc[-(cnt+1)]:
            최종v저점 = temp_df[cond_저점][f'{ma}_변곡점'].iloc[-(cnt)]
            
            ## 
            if ma not in ['ma3','ma5']:
                최종이전저점의전저점 = temp_df[cond_저점][f'{ma}_변곡점'].iloc[-(cnt+1)]  ## 20일선 이상은 이평선저점으로 판단.
                if 최종v저점  >= 최종이전저점의전저점 * 0.98:   # 2% 허용. 
                    # date.append(temp_df[cond_저점][f'{ma}_변곡점'].index[-(cnt+1)]) ## 최종v저점의 날짜 추가.
                    date.append(temp_df[cond_저점][f'{ma}_변곡점'].index[-(cnt+1)]) ## 최종v저점의 날짜 추가.
                    cnt+=1
                    
                else:
                    break
            
            else:    
                최종이전저점의전저점 = temp_df[cond_저점][f'{ma}_전저고점'].iloc[-(cnt+1)]
                if 최종v저점  >= 최종이전저점의전저점:
                    # date.append(temp_df[cond_저점][f'{ma}_변곡점'].index[-(cnt+1)]) ## 최종v저점의 날짜 추가.
                    date.append(temp_df[cond_저점][f'{ma}_변곡점'].index[-(cnt+1)]) ## 최종v저점의 날짜 추가.
                    
                    cnt+=1
                else:
                    break
        ## 저점추세. 
        if len(date)>=2:
            if 변곡df.loc[date[0],f'{ma}_변곡점'] > 변곡df.loc[date[-1],f'{ma}_변곡점']:
                저점추세 = '우상'
            else:
                저점추세 = '우하'
        else:
            저점추세 = np.nan
        저점개수 = cnt
        
        #############################################################################
        
        ## 고점추세. ############################################
        cond_고점 = temp_df[f'{ma}_변곡점_변화량'] > 0 

        if sum(cond_고점) <= 1:
            # print(f'{self.code_name} {ma} 변곡정보부족')
            result_dic = {
            '이평':ma,
            '현재방향': 현재방향,
            '변곡지속일': 변곡지속일,
            '이격도min' : min_dist,
            '이격도max' : max_dist,
            '가격이격도':현재가격이격도,
            '현재저점이격도': 현재저점이격도,
            '저점개수': 저점개수,
            '저점추세': 저점추세,
            '고점개수': np.nan,
            '고점추세': np.nan,
            # '저고폭': np.nan,
            # '고저폭': np.nan,
            '이평현재위치': np.nan,  ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
            '가격현재위치': np.nan, ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
            '진동폭':np.nan,
            '저점날짜' :  sorted(date),  ## 저점날짜들 역순 입력.
            '고점날짜'  : []
            }
            # return result_dic


        cnt  = 1 ##
        high_date = []
        try:
            high_date.append(temp_df[cond_고점][f'{ma}_변곡점'].index[-(cnt)])
            cnt_high = len(temp_df.loc[cond_고점]) 
            
            while cnt_high - cnt:
                if temp_df[cond_고점][f'{ma}_변곡점'].iloc[-(cnt)] >= temp_df[cond_고점][f'{ma}_변곡점'].iloc[-(cnt+1)]:
                    high_date.append(temp_df[cond_고점][f'{ma}_변곡점'].index[-(cnt+1)])
                    cnt+=1
                else:
                    break
        except:
            print('장기오류 해결된.')
            pass
        
        ## 고점추세. 
        if len(high_date)>=2:
            if 변곡df.loc[high_date[0],f'{ma}_변곡점'] > 변곡df.loc[high_date[-1],f'{ma}_변곡점']:
                고점추세 = '우상'
            else:
                고점추세 = '우하'
        else:
            고점추세 = np.nan
        고점개수 = cnt
        ##################################################
        
        high = max(변곡df[f'{ma}_변곡점'][-2:])  # 마지막변곡점 값 . 현재상태에 따라 의미가 달라짐. 고저일경우 내린 값. 저고 일경우 오룸값.
        low = min(변곡df[f'{ma}_변곡점'][-2:])
        current_wave_width = round(((high/low)-1)*100,1)  ## 고저점 저고점 진동 폭.(최근진동)
        
        
        # if temp_df[f"{ma}_변곡점_변화량"].dropna()[-1] < 0 : #상승상태. 저고폭(전저점,전고점), 고저폭(전고점,저점)
        #     저고폭 = temp_df[f"{ma}_변곡점_변화량"].dropna()[-2].round(2)
        #     고저폭 = temp_df[f"{ma}_변곡점_변화량"].dropna()[-1].round(2)
        # elif temp_df[f"{ma}_변곡점_변화량"].dropna()[-1] > 0 : # 하락상태. 저고폭(전저점,전고점), 고저폭(전고점,현재ma값)
        #     저고폭 = temp_df[f"{ma}_변곡점_변화량"].dropna()[-1].round(2)
        #     고저폭 = (temp_df[f'{ma}'][-1] / temp_df[f'{ma}_변곡점'].dropna()[-1]) -1
        #     고저폭 = 고저폭.round(2)
        
        
        ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상.
        try:
            # 이평선.
            current_ma_rate = round((current_ma_value-low)/(high-low),2)
            # 주가위치
            current_price_rate = round((current_price-low)/(high-low),2)
        except:
            current_ma_rate = -100
            current_price_rate = -100
        
        result_dic = {
            '이평':ma,
            '현재방향':현재방향,
            '변곡지속일':변곡지속일,
            '이격도min' : min_dist,
            '이격도max' : max_dist,
            '가격이격도' : 현재가격이격도, 
            '현재저점이격도': 현재저점이격도,
            '저점개수':저점개수,
            '저점추세':저점추세,
            '고점개수':고점개수,
            '고점추세':고점추세,
            # '저고폭':저고폭,
            # '고저폭':고저폭,
            '이평현재위치':current_ma_rate,  ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상. -100이면 오류
            '가격현재위치':current_price_rate, ## 0보다 작으면 하 , 0~1 이면 중, 1 이상이면 상. -100이면 오류
            '진동폭':current_wave_width, # 최근파동 진동폭. 
            '저점날짜' : sorted(date),   ## 저점날짜들 역순 입력.
            '고점날짜' : sorted(high_date)
            
        }        
        
        
        # if sum(cond_저점) >= 1 & sum(cond_고점) >=1:
        if len(변곡df[f'{ma}_변곡점']) >= 3:
            # 현재방향 = '상승' if 변화량 > 0 else '하락'
            if (현재방향 == '상승') & (sum(cond_저점) == 1) : ## v중..
                고저폭 =  변곡df[f'{ma}_변곡점'][-1] / 변곡df[f'{ma}_변곡점'][-2]  
                저고폭 =  temp_df[f'{ma}'][-1] / 변곡df[f'{ma}_변곡점'][-1]  # 저고 현재값 적용.
                a_wave_cnt = len(temp_df.loc[변곡df[f'{ma}_변곡점'].index[-1] : ])  # a_wave_cnt = 상승기간(봉)
                b_wave_cnt = len(temp_df.loc[변곡df[f'{ma}_변곡점'].index[-2] : 변곡df[f'{ma}_변곡점'].index[-1]]) # b_wave_cnt = 하락기간(봉)
                
            elif (현재방향 == '상승') & (sum(cond_저점) > 1) : #@ w중.
                저고폭 = 변곡df[f'{ma}_변곡점'][-2] / 변곡df[f'{ma}_변곡점'][-3] # 저고폭.
                고저폭 = 변곡df[f'{ma}_변곡점'][-1] / 변곡df[f'{ma}_변곡점'][-2] 
                a_wave_cnt = len(temp_df.loc[변곡df[f'{ma}_변곡점'].index[-3] : 변곡df[f'{ma}_변곡점'].index[-2]])
                b_wave_cnt = len(temp_df.loc[변곡df[f'{ma}_변곡점'].index[-2] : 변곡df[f'{ma}_변곡점'].index[-1]])
            elif (현재방향  == '하락'):
                저고폭 =  변곡df[f'{ma}_변곡점'][-1] / 변곡df[f'{ma}_변곡점'][-2]  # 저고
                고저폭 = temp_df[f'{ma}'][-1] / 변곡df[f'{ma}_변곡점'][-1] # 현재값 적용
                a_wave_cnt = len(temp_df.loc[변곡df[f'{ma}_변곡점'].index[-2] : 변곡df[f'{ma}_변곡점'].index[-1]])
                b_wave_cnt = len(temp_df.loc[변곡df[f'{ma}_변곡점'].index[-1] : ])
            else:
                print('조건미달..')
                저고폭 = np.nan
                고저폭 = np.nan
                a_wave_cnt =np.nan
                b_wave_cnt = np.nan
        else:
            print('cond저점수 미달.')
            저고폭 = np.nan
            고저폭 = np.nan
            
        result_dic['저고폭']=round((저고폭-1)*100,1)
        result_dic['고저폭']=round((고저폭-1)*100,1)
        if (a_wave_cnt!=np.nan) & (a_wave_cnt > b_wave_cnt):
            result_dic['ab기간파동'] = True
        else:
            result_dic['ab기간파동'] = False
            
        return result_dic
    
    def _get_two_ma_info(self,ma_df,short_ma='ma3', long_ma='ma10'):
        '''
        short_ma : ma3 ma5...ma240''
        return df
        '''

        if short_ma not in ma_df.columns or long_ma not in ma_df.columns:
            # print('ma가 존재하지 않습니다.')
            return None
        
        
        df = ma_df[[short_ma,long_ma]].copy()
        
        ## short_status 구하기. 
        cond_short_ma_status = df[short_ma] > df[short_ma].shift(1) 
        cond_long_ma_status = df[long_ma] > df[long_ma].shift(1)
        # df['short_ma_status'] = df[short_ma]-df[short_ma].shift(1) > 0
        # df['long_ma_status'] = df[long_ma] - df[long_ma].shift(1) < 0 
        df.loc[cond_short_ma_status,'short_ma_status'] = 'up'
        df.loc[~cond_short_ma_status,'short_ma_status'] = 'down'

        df.loc[cond_long_ma_status,'long_ma_status'] = 'up'
        df.loc[~cond_long_ma_status,'long_ma_status'] = 'down'

        df['cross']=  df[short_ma] - df[long_ma]
        cond_gc = (df['cross'] > 0) & (df['cross'].shift(1) < 0)
        df.loc[cond_gc,'cross_status']='gc'
        cond_dc = (df['cross'] < 0) & (df['cross'].shift(1) > 0)
        df.loc[cond_dc,'cross_status']='dc'

        result = df[ df['cross_status'].notnull()]
        # result['disparity'] = result.iloc[:,0] / result.iloc[:,1] *100 # 이격도 추가.
        return result
     
    # def _w_status(self,df1,ma):
    #     '''
    #     3중바닥여부. 파동의 깊이. 대기 또는 저저점 붕괴상태 .등등.
    #     return : dic
    #     '''
    #     df = df1.copy()
    #     if ma not in df.columns:
    #         # print('df입력오류 또는 ma 입력 오류')
    #         return None
    #     try:
    #         dic = self._get_ma_info(df,ma)
    #         # {'이평': 'ma3',
    #         # '현재방향': '상승',
    #         # '변곡지속일': 10,
    #         # '저점개수': 3,
    #         # '저점추세': '우상',
    #         # '고점개수': 1,
    #         # '고점추세': None,
    #         # '이평현재위치': 6.02,
    #         # '가격현재위치': 6.65,
    #         # '저점날짜': [Timestamp('2022-02-23 00:00:00')]}
    #         cond1 = dic.get('현재방향')=='상승'
    #         cond2 = 0 < dic.get('이평현재위치') 
    #         # cond3 = dic.get('변곡지속일') <5 
    #         cond3 = True
    #         if all([cond1,cond2,cond3]):
    #             return {'result':True,**dic}
    #         else:
    #             return {'result':False,**dic}
    #     except:
    #         return {'result':False,**dic}
    


    def _coke_status(self,df1,기준변화율 = 0.002, candle = 'day'):
        '''
        return: dict
        candle  : day(일, 주 월봉). min(분봉)
        '''
         ## 만약 데이터 간격이 30분이면 . 
        cond1 = ((df1.index[-2]  - df1.index[-3]).days == 0)  | ((df1.index[-2]  - df1.index[-3]).seconds == 1800)
        cond2 = ((df1.index[-6]  - df1.index[-7]).days == 0 ) | ((df1.index[-6]  - df1.index[-7]).seconds == 1800 )
        if any([cond1 , cond2]):
            기준변화율 = 0.0015  ## 수정필요
            candle = 'min'
            
        try:
            # 기준변화율 = 0.0025
            기준기간일 = 10
            if candle == 'day':
                try:
                    start_day = self.info_ma3['start_day'] 
                except:
                    start_day = df1.index[-1]
            elif candle == 'min':
                
                try:
                    start_day = self.info_min30_ma40['저점날짜'][-1]
                except:
                    start_day = df1.index[-1]
                
                
                
            df= df1[['upper_bb','lower_bb','coke_width']].loc[:start_day].copy()
            df2 = df1[['upper_bb','lower_bb','coke_width']].loc[start_day:].copy() ## start_day 이후 데이터.
            
            # 조건설정.
            cond_upper = abs(df['upper_bb'].pct_change(2)) < 기준변화율
            cond_lower = abs(df['lower_bb'].pct_change(2)) < 기준변화율

            # cond_all = cond_upper & cond_lower ## 동시만족
            cond_all = cond_upper   ## upper만 일자

            cond_all_u = cond_upper != cond_upper.shift(1)
            df['g_u'] = cond_all_u.cumsum() # 그룹할 숫자 정리.

            cond_all_l = cond_lower != cond_lower.shift(1)
            df['g_l'] = cond_all_l.cumsum() # 그룹할 숫자 정리.

            grouped_u = df.loc[cond_upper].groupby('g_u') ## 조건만족하는 데이터만 그룹화.
            grouped_l = df.loc[cond_lower].groupby('g_l') ## 조건만족하는 데이터만 그룹화.


            # # 기준기간일 무관하게 조건만족하는 dict
            # result_dic = dict(list(grouped))

            # # 기준기간일 이상데이터만. 
            # result_dic = dict([g  for g in list(grouped) if len(g[-1]) >= 기준기간일]) ## 20개이상 데이터만

            ## plot용으로 변형. 
            plot_dic_u  = dict([(str(k),v[['upper_bb']]) for k,v in dict(list(grouped_u)).items()])## plot용으로 변형. 
            plot_dic_l  = dict([(str(k),v[['lower_bb']]) for k,v in dict(list(grouped_l)).items()])

            ## 

            ## 마지막 정보가져오기
            u_last_coke_data = list(grouped_u)[-1][-1]
            l_last_coke_data = list(grouped_l)[-1][-1]


            # 마지막과 의 끝과 현재가 같으면 현재진행형. 
            if u_last_coke_data.index[-1] == df.index[-1]:
                # u_current_status = '진행중'
                u_종료이후캔들수 = 0
                upper_수평지속일 = len(u_last_coke_data)
            else:
                # u_current_status = '종료'
                u_종료이후캔들수 = len(df.loc[u_last_coke_data.index[-1]:])
                upper_수평지속일 = len(u_last_coke_data)


            if l_last_coke_data.index[-1] == df.index[-1]:
                # l_current_status = '진행중'
                l_종료이후캔들수 = 0
                lower_수평지속일 = len(l_last_coke_data)
            else:
                # l_current_status = '종료'
                l_종료이후캔들수 = len(df.loc[l_last_coke_data.index[-1]:])
                lower_수평지속일 = len(l_last_coke_data)
                            

            
            # width_change_rate = abs(df['coke_width'].pct_change(3))
            # coke_width_pct_cond = width_change_rate < 0.015
            width_change_rate = abs(df2['coke_width'].pct_change(3)) ## start_day 이후데이터존재여부.
            coke_width_pct_cond = width_change_rate < 0.015
            ## result True 고 수평지속일 n일이상이거나. 수평지속일 n일이상이고 upper_current_status True면 만족함.
            ## code_width_pct :start_day 당시의 변화율.만족여부.sum(coke_width_pct_cond)
            width_pct_cond = sum(coke_width_pct_cond) >= 10 ## 너비변화율 조건
            
            result  = ((upper_수평지속일 >= 20) | (lower_수평지속일 >= 20)) & width_pct_cond ## upper lower 모두 True 면. 
            
            dic = {"result":result,
                    "upper_current_value": df.iloc[-1]['upper_bb'] , 
                    "upper_current_status": cond_upper[-1], 
                    "upper_start_date":u_last_coke_data.index[0],
                    "upper_last_date":u_last_coke_data.index[-1],
                    "upper_수평지속일": upper_수평지속일, 
                    "upper_수평종료후캔들수" : u_종료이후캔들수,

                    "lower_current_value": df.iloc[-1]['lower_bb'],
                    "lower_current_status":cond_lower[-1] , 
                    "lower_수평지속일": lower_수평지속일,
                    "lower_start_date":l_last_coke_data.index[0],
                    "lower_last_date":l_last_coke_data.index[-1],
                    "lewer_수평종료후캔들수" : l_종료이후캔들수,
                    
                    "coke_width": df['coke_width'][-1] ,
                    "coke_width_pct_start_day" : coke_width_pct_cond[0],
                    "coke_width_pct_cnt" : sum(coke_width_pct_cond),
                    "기준변화율" : 기준변화율,
                    "start_day_ma3" : start_day
                    }
            return dic
        except:
            # print('코크정보가져오기 오류종목 : ',self.code_name)
            result = False
            dic1 = {"result":result,
                    "upper_current_value": 0 , 
                    "upper_current_status": "", 
                    "upper_start_date":"",
                    "upper_last_date":"",
                    "upper_수평지속일": -1, 
                    "upper_수평종료후캔들수" : -1,

                    "lower_current_value": 0,
                    "lower_current_status":"" , 
                    "lower_수평지속일": "",
                    "lower_start_date":"",
                    "lower_last_date":"",
                    "lewer_수평종료후캔들수" : -1,
                    
                    "coke_width": 1000 ,
                    "기준변화율" : 기준변화율,
                    }
            return dic1

    def _sun_status(self,df1, 너비기준 = 30):
        '''
        return : dict
        '''
        
        ## 만약 데이터 간격이 30분이면 . 
        cond1 = ((df1.index[-2]  - df1.index[-3]).days == 0)  | ((df1.index[-2]  - df1.index[-3]).seconds == 1800)
        cond2 = ((df1.index[-6]  - df1.index[-7]).days == 0 ) | ((df1.index[-6]  - df1.index[-7]).seconds == 1800 )
        if any([cond1 , cond2]):
            너비기준 = 10  ## 수정필요
            
        
        try:
            df= df1[['sun_max', 'sun_min','sun_width']].copy()

            width_avg = df['sun_width'].rolling(20).mean()
            새출20평균너비 = width_avg[-1]
            새출현재너비 = df['sun_width'][-1]
            현재상단값 = df['sun_max'][-1]
            현재하단값 = df['sun_min'][-1]
            
            result = True if 새출20평균너비 <= 너비기준 else False
            
            dic = {"result":result,
                "새출현재너비":  새출현재너비 , 
                "새출20일평균너비":새출20평균너비,
                "현재상단값":현재상단값,
                "현재하단값":현재하단값,
            }
        except:
            dic = {"result":False,
                "새출현재너비":  -100 , 
                "새출20일평균너비":-100,
                "현재상단값":-100,
                "현재하단값":-100,
            }
        return dic

    def _ac_status(self):
        '''
        ac정보가져오기.
        # pct_change 는 1이 100% 두배인것. 1보다 크면 두배이상인것임. 
        그냥 ac 인 df만 반환. 수치와 반환.
        return ; df
        '''
        df = self.df.copy()
        try:
            df = df[['Open','High','Low','Close','Volume','vol20ma','ma3','ma3변화량','ma3_변곡점']].copy()
            if len(df)==0:
                return pd.DataFrame() ,pd.DataFrame()
        except:
            return pd.DataFrame(),pd.DataFrame()
        # pct_change 는 1이 100% 두배인것. 1보다 크면 두배이상인것임. 
        df['ma20_rate'] = round( df['Volume'] / df['vol20ma'].shift(1) - 1 , 2 )
        df['pre_day_rate'] = round(df['Volume'].pct_change(1),2)  # 전일대비
        df['pre_2day_rate'] = round(df['Volume'].pct_change(2),2)  # 전전일대비 

        df['max'] = df.apply(lambda row : max(row['ma20_rate'],row['pre_day_rate'],row['pre_2day_rate']),axis=1)
        df['min'] = df.apply(lambda row : min(row['ma20_rate'],row['pre_day_rate'],row['pre_2day_rate']),axis=1)
        df['mid'] = df.apply(lambda row : np.median([row['ma20_rate'],row['pre_day_rate'],row['pre_2day_rate']]),axis=1)

        ## a값 구하기.
        low_indexs = df.loc[(df['ma3변화량'] < 0 ) & df['ma3_변곡점'].notnull()].index
        arr_low_indexs = np.array(low_indexs)
        temp = [ arr_low_indexs [ arr_low_indexs < day] for day in df.index ]  ## ac구간마다 전저점의 날짜 찾음.
        temp1 = [arr[-1] if len(arr) >0 else False for arr in temp ] ## ac구간마다 날짜의 마지막 즉 최근값 , 즉 ac바로 전저점 날짜를 구함.
        temp2 = [df.loc[date,'ma3'] if date else np.nan for date in temp1]  ## ac구간마다 전저점 (ma3) 값을 구함. 즉. abc 일경우 a값.
        df['abc_a'] = temp2
        df['abc_b'] = (df['abc_a'] + df['Close']) / 2 
        df['abc_c'] = df['Close']

        cond_1 = df['vol20ma']*1.1 < df['Volume'] # 평균거래량보단 10프로이상 많은것만. 
        cond_plus = (df['Close'].shift(1) < df['Close'] ) &  (df['Open'] < df['Close'] ) # 전일대비 혹은 양봉.
        
        # 상승음봉 정의 
        cond_상승음봉 = (df['Low'].shift(1) < df['Close']) & (df['Close'].shift(1) < df['Open'])   
        cond_ac = (df['ma20_rate'] >= 1.2) | (df['pre_day_rate'] >= 1.2) | (df['pre_2day_rate'] >= 1.2)  
        cond_bic_ac = (df['min'] >= 7) | (df['max'] >= 9)  ## -> max추가한 이유는 대량거래연속터질때 그거래량도 포함하기 위함. 
        # result = df.loc[cond_1 & cond_ac & (cond_plus | cond_상승음봉)]   
        result = df.loc[cond_ac & (cond_plus | cond_상승음봉)]   
        result_bic_ac = df.loc[cond_1 & cond_bic_ac & (cond_plus | cond_상승음봉)]  
        
        return result , result_bic_ac
     
    def _ab_volume_status(self,df1):
        '''
        ab거래량체크
        '''
        df  = df1.copy()
        
        df = df[(df['Volume'] !=0)]  ## 거래정지있으면 데이터 제외.
        
        ### 데이터가 적을때. 
        # # ab거래량 찾기.aa
        # # 1. 20일 마지막  변곡점  가져오기,
        low_point_days = self._get_ma_info(df,'ma20').get('저점날짜')
        if low_point_days:
            last_low_point_day = low_point_days[-1]
            ## 양옆 20일 기준 
            # a_vol_avg = df.loc[last_low_point_day,'vol20ma']  ## 저점날짜 포함.
            a_vol_avg = df.loc[:last_low_point_day,'Volume'][-20:-1].mean()  ## 저점날짜 제외
            b_vol_avg = df.loc[last_low_point_day:,'Volume'][1:20].mean()  ## 저점날짜 제외.
            
            if a_vol_avg >0:
                ab_v_rate = round(b_vol_avg / a_vol_avg,1 )
            else:
                ab_v_rate = None
            
            
            dic_ma20 = {"기준날짜":last_low_point_day,
                        "경과일":len(df.loc[last_low_point_day:]),
                        "ab_v_rate": ab_v_rate
                        } 
        else:
            dic_ma20 = {"기준날짜":None,
                        "경과일":None,
                        "ab_v_rate": None
                        } 


        ## 2. 마지막 대량거래 지점 가져오기. 

        # ac_df = self._ac_status()[0]
        ac_df = self.info_ac[0]
        
        try:
            big_df= ac_df[ac_df['pre_day_rate'] > 8] # 7배이상
        except:
            big_df  = pd.DataFrame()
        if len(big_df):
            last_big_vol_info  =  big_df.tail(1)
            last_big_vol_info = last_big_vol_info[['ma20_rate','pre_day_rate','pre_2day_rate','abc_a','abc_b','abc_c']]
            last_big_vol_date = last_big_vol_info.index[-1]
            
            # a_vol_avg = df.loc[:last_big_vol_date,'Volume'][-20:-1].mean()  ## ac날짜 제외
            # b_vol_avg = df.loc[last_big_vol_date:,'Volume'][1:20].mean()  ## ac날짜 제외.
            a_vol_avg = df.loc[:last_big_vol_date,'Volume'][-20:-1].mean()  ## ac날짜 제외
            b_vol_avg = df.loc[last_big_vol_date:,'Volume'][1:20].mean()  ## ac날짜 제외.
            if a_vol_avg > 0:
                ab_v_rate = round(b_vol_avg / a_vol_avg,1 )
            else:
                # print(f'{self.code_name}  대량거래날짜 범위가 있는지 없는지 체크')
                # print('아래 big_df 내용.')
                ab_v_rate = None

            dic_from_df = last_big_vol_info.to_dict('records')[0]
            dic_big_v  = {"기준날짜":last_big_vol_date,
                        "경과일":len(df.loc[last_big_vol_date:]),
                        "ab_v_rate": ab_v_rate,
                        **dic_from_df 
                        } 
                
        else:
            dic_big_v  = {"기준날짜":None,
                        "경과일":None,
                        "ab_v_rate": None,
                         
                        } 
        dic = {}
        dic['ma20'] =dic_ma20
        dic['big_v'] = dic_big_v
     
        return dic
            
    def is_w_by_day(self,df,ma="ma3"):
        '''
        get_ma_info 보완. 
        duration = 지속일 기준
        result_type = w, w독사, v눌림(short만 사용.) w대기
        '''
        # w, w독사, v눌림 (거래량눌림목)
        result_type_ls = []


        ma_info = self._get_ma_info(df,ma)
        info_ac = self._ac_status()
        
        상승조건 = ma_info.get('현재방향')=='상승'
        상승지속조건 = ma_info.get('변곡지속일')>=1
        저점조건 = ma_info.get('저점개수')>=2
        #저점이 두개고 그 둘 사이에 고점이 있어야함.
        try:
            변곡점자리조건 = ma_info.get('저점날짜')[-2] < ma_info.get('고점날짜')[-1] < ma_info.get('저점날짜')[-1]
        except:
            변곡점자리조건 = False
        추세조건 = ma_info.get('저점추세') =='우상'

        # w조건
        result = all([상승조건 , 상승지속조건, 저점조건, 추세조건,변곡점자리조건])
        if result:
            result_type_ls.append('w완성')

        doksa_value_dic = {'ma3':'Close',
                           'ma5' : 'Close',
                           'ma20' : 'ma3',
                           'ma60' : 'ma3',
                           'ma120' : 'ma3',
                           'ma240' : 'ma3',
                           }
        try:
            현재주가조건 = df.iloc[-1][ma] <= df.iloc[-1][doksa_value_dic[ma]] # 이평보다 주가가 큰상태. 
            # print('주가조건')
        except:
            # print('except1')
            현재주가조건 = False
        try:
            하락조건 = ma_info.get('현재방향')=='하락'
            # print('하락')
        except:
            # print('except2')
            하락조건 = False
        
        저점존재조건 = len(ma_info.get('저점날짜')) > 0 # 1개이상 저점.
        고점존재조건 = len(ma_info.get('고점날짜')) > 0 # 1개이상 저점.
        try:
            대기자리조건 = ma_info.get('저점날짜')[-1] < ma_info.get('고점날짜')[-1]
        except:
            대기자리조건 = False
        if 저점존재조건 & 고점존재조건 & 대기자리조건:
            # print('저점존재조건')
            전저점값 = df.loc[ma_info.get('저점날짜')[-1],ma]
            전고점값 = df.loc[ma_info.get('고점날짜')[-1],ma]
            # print(f"전저점값:{전저점값}, 전고점값:{전고점값}")
            
            w대기_조건 = (전저점값 <= self.df.iloc[-1][ma] < 전고점값)
            # print(w대기_조건)
        else:
            w대기_조건 = False
        독사조건 = all([현재주가조건,하락조건,저점존재조건,w대기_조건])
        대기조건 = all([하락조건,저점존재조건,w대기_조건])
        
        if 독사조건:
            result = True
            result_type_ls.append("w독사")
        if 대기조건:
            result = False
            result_type_ls.append("w대기")


        ## W 일때 알파베타, 또는 착시 확인하기.
        alphabeta_dic = {}
        abc_v_dic ={} # 착시거래 포함.
        coke_v_dic = {} # 코크상단과 gc이후 v존 찾기.( 코크이전 너비정보 포함) 
        sun_v_dic = {} # 
        start_day = ""
        end_day = ""
        # if result or 독사조건: # 확인해야할 날짜 지정. 
        if (ma_info.get('저점날짜') is not None) & (len(ma_info.get('저점날짜')) !=0) :
            if 'w완성' in result_type_ls:
                start_day = ma_info.get('저점날짜')[-2]
                end_day = ma_info.get('저점날짜')[-1]
            elif 독사조건: # 하락이면서 전저점 안깬상황에 가격 gc는 일어난 상황.
                start_day = ma_info.get('저점날짜')[-1]
                end_day = df.index[-1]
                # print("독사조건 날짜지정:",start_day,end_day)
            else:
                start_day = ma_info.get('저점날짜')[-1]
                end_day = df.index[-1]

            temp_df = df.copy()
            temp_df['pre_vol20ma'] = temp_df['vol20ma'].shift(1)
            temp_df = temp_df.loc[start_day:end_day] ## temp_df w 저점사이 데이터. 
            
            
            
            
            
            ## info_ac에서 착시 판단하기.. 
            info_ac_df = self.info_ac[1]
            ac_for_착시 = info_ac_df.loc[start_day:]
            if len(ac_for_착시):
                착시거래일리스트 = list(ac_for_착시.index)
                best_v_date = info_ac_df.loc[start_day:,'Volume'].idxmax()
                착시거래량비율 = info_ac_df.loc[best_v_date,'min']
                
                v이후최고거래량 = info_ac_df.loc[best_v_date,'Volume']
                v이전평균거래량 = self.df['vol20ma'].shift(1).loc[best_v_date]
                기준거래량 = max((v이후최고거래량 / 10), v이전평균거래량)

                abc_v_dic['착시'] = True
                abc_v_dic['착시거래량비율']  = 착시거래량비율
                abc_v_dic['착시거래일list']=착시거래일리스트
                abc_v_dic['abc_a']=info_ac_df.loc[best_v_date,'abc_a']
                abc_v_dic['abc_b']=info_ac_df.loc[best_v_date,'abc_b']
                abc_v_dic['abc_c']=info_ac_df.loc[best_v_date,'abc_c']


                ## 기준거래량과 현재 비교해서 result_type_ls.append('v눌림')하기. 
                vv_cond1 = self.df.iloc[-1]['Volume'] <= 기준거래량
                vv_cond2 = info_ac_df.loc[best_v_date,'abc_c'] < self.df.iloc[-1]['Close']
                vv_cond3 = sum(self.df.loc[best_v_date: , ma] < info_ac_df.loc[best_v_date,'abc_a'])==0 # 체크기간동안 abc_c보다 작은건 하나도 없어야함.
                if vv_cond1 & vv_cond2 & vv_cond3 :
                    result_type_ls.append('v눌림')
            else:
                abc_v_dic['착시'] = False
                abc_v_dic['착시거래량비율']  = 0
                abc_v_dic['착시거래일list']=[]
                abc_v_dic['abc_a']=-1
                abc_v_dic['abc_b']=-1
                abc_v_dic['abc_c']=-1


            ## 그지점에 bb상단과. gc가 있는지 확인.  ma  와 . upper_bb
            bb_gc_cond = (temp_df.loc[start_day,ma] < temp_df.loc[start_day,"upper_bb"]) & (temp_df.loc[end_day,ma] > temp_df.loc[end_day,"upper_bb"])
            start_coke_width_value = temp_df.loc[start_day,"coke_width"]
            ## w 일때 bb상단뚫고 v 존인지 확인하고. 너비까지 확인하기.
            
            if bb_gc_cond:
                coke_v_dic['bb_gc'] = True
                # print('bb_True',ma)
            else:
                coke_v_dic['bb_gc'] = False
            
            coke_v_dic['bb_gc_width_value'] = start_coke_width_value
            

            sun_gc_cond = (temp_df.loc[start_day,ma] < temp_df.loc[start_day,"sun_max"]) & (temp_df.loc[end_day,ma] > temp_df.loc[end_day,"sun_max"])
            start_sun_width_value = temp_df.loc[start_day,"sun_width"]
            
            if sun_gc_cond:
                sun_v_dic['sun_gc'] = True
                # print('sun_True',ma)
            else:
                sun_v_dic['sun_gc'] = False
            
            sun_v_dic['sun_gc_width_value'] = start_sun_width_value
            



            #### 알파베타 확인하기. #########
            # 현재 이평 int 
            current_ma_int = int(ma.replace("ma",""))  #  == ma
            
            # 현재 데이터에 존재하는 ma 체크.
            exist_mas = self.df.filter(regex=r'ma\d+$').columns
            exist_mas = np.array([int(ma.replace("ma","")) for ma in exist_mas])
            # 현재이평보다 큰 이평 3개만 추출해서 alphabeta 체크. 
            check_mas = exist_mas [ exist_mas > current_ma_int][:3]


            check_mas = [ma] + ["ma"+str(check_ma) for check_ma in check_mas]   ## gcv보기위해 자기자신도 넣음.
            # print('check_mas: ',check_mas)

            
            # ma가 2개이상이면 조합 추출하기. 
            if len(check_mas)>2:
                gc_checking_items = list(itertools.combinations((check_mas),2))  ##return  [ (ma60,ma120) , (ma120, ma240) ,... ]
                ## short ma 일때만 . bb와 sun 추가하기.
                # if ma in ['ma3','ma5','ma10']:
                #     gc_checking_items = gc_checking_items +[ (ma,'upper_bb'),(ma,'sun_max')]
            else:
                gc_checking_items = []
            if gc_checking_items:
                alphabeta_dic['alphabeta_gc_ls']=[]
                alphabeta_dic['alphabeta_type'] = set()
                
                # print(gc_checking_items)
                
                for short, long in gc_checking_items:
                    # gc cond
                    
                    
                    gc_cond = (temp_df.loc[start_day,short] < temp_df.loc[start_day,long]) & (temp_df.loc[end_day,short] > temp_df.loc[end_day,long])
                    if gc_cond :
                        alphabeta_dic['alphabeta_gc_ls'].append((short,long))
                        alphabeta_dic['alphabeta_type'].add('A')
                        
                    #     
                    
                    # 역배열. 이격도  , 추세 cond
                    역배열_cond = temp_df.loc[end_day, short] < temp_df.loc[end_day, long] # 역배열.
                    이격도_cond = temp_df.loc[end_day, long] / temp_df.loc[end_day, short] * 100 < 101  ## 이격도
                    diff = temp_df[long] - temp_df[short]
                    추세_cond = diff[-2] > diff[-1]
                    gc_cond2 = 역배열_cond & 이격도_cond & 추세_cond
                    if gc_cond2 :
                        alphabeta_dic['alphabeta_gc_ls'].append((short,long))
                        alphabeta_dic['alphabeta_type'].add('B')
                        
                alphabeta_dic['alphabeta_type'] = list(alphabeta_dic['alphabeta_type'])
                if len(alphabeta_dic['alphabeta_gc_ls']):
                    alphabeta_dic['alphabeta'] = True
                else:
                    alphabeta_dic['alphabeta'] = False
            else:
                # alphabeta : True or False
                # alphabeta_type = 'A' # gc. , 'B' # gc대기. 'AB' 혼합. or None
                # alphabeta_gc_ls = [(ma60, ma120),(ma120, ma240)] or [] # 이렇게 추가하기. 
                alphabeta_dic['alphabeta']= False
                alphabeta_dic['alphabeta_type'] = []
                alphabeta_dic['alphabeta_gc_ls'] = []
        else:
            alphabeta_dic['alphabeta']= False
            alphabeta_dic['alphabeta_type'] = []
            alphabeta_dic['alphabeta_gc_ls'] = []
            abc_v_dic['착시'] = False
            abc_v_dic['착시거래량비율'] = np.nan
            coke_v_dic['bb_gc'] = False
            try:
                coke_v_dic['bb_gc_width_value'] = self.df.iloc[-1]["coke_width"]
            except:
                coke_v_dic['bb_gc_width_value']  = np.nan
            sun_v_dic['sun_gc'] = False
            try:
                sun_v_dic['sun_gc_width_value'] = self.df.iloc[-1]["sun_width"]
            except:
                sun_v_dic['sun_gc_width_value'] = np.nan
            
            
        dic = {"result": result,
               'start_day': start_day,
               'end_day': end_day,
               'result_type_ls':  result_type_ls,
               **ma_info, **alphabeta_dic ,**abc_v_dic ,**coke_v_dic, **sun_v_dic}
        return dic


    def is_w_by_min30(self,ma='ma40'):
        if "df_min30" not in dir(self):
            self.df_min30 = self._get_data_daum('30분봉')
        if not len(self.df_min30):
            print('30분봉데이터가 없습니다.')
            return 
        기준변화량 = 0.0005
        독사단기ma = 'ma10'
        ma_info = self._get_ma_info(self.df_min30,ma)

        기울기조건 = (abs(self.df_min30['ma40변화량']) < 기준변화량)  # 40 어정쩡한상태
        기울기조건 = sum(기울기조건.tail(10)) >= 1  ## 최근에 한번이라도 꺽인적있는것.
        현재주가조건 = self.df_min30[ma] < self.df_min30[독사단기ma] # 10이평이 와있는상태. 
        현재주가조건 = 현재주가조건[-1]
        독사조건 = all([기울기조건 , 현재주가조건])
        
        상승조건 = ma_info.get('현재방향')=='상승'
        상승지속조건 = ma_info.get('변곡지속일')>=1
        저점조건 = ma_info.get('저점개수')>=2
        w조건 = all([상승조건,상승지속조건,저점조건])
        
        result = 독사조건 | w조건
        dic = {"result": result,
               'from_df': 'min30',
               '독사':독사조건,
               **ma_info}
        return dic



    def is_buy_rate_cond(self):
        '''
        매집비조건에 만족하면 True
        
        '''
        if "info_buy_rate" in dir(self):
            rates_ls = [dic['매집비'] for dic in self.info_buy_rate['외인금투연_구간별']]
            if rates_ls:
                cond1 = (np.array(rates_ls) > 100).sum() >=3
                cond2 = (np.array(rates_ls) > 130).sum() >= 1
                cond_buy = self.info_buy_rate['외인금투연_구간별'][0]['순매수대금_백만'] >= 100 # 순매수 1억이상
                
                if (cond1 | cond2) & cond_buy:
                    return True
                else:
                    # print('조건불만족')
                    return False
            else:
                # print('값없음')
                return False
        else:
            # print('변수없음')
            return False

    
    def __구간별정보추출(self, i_df,ohlcv_df,col = ['외국인', '금융투자', '투신', '연기금']):
        result_dic = {}
        ma = 'ma3'
        # invest원본데이터에 구간데이터를 넣으면 그 구간에 대한  투자자에 따른. 매수대금, 매도대금, 순매수대금 정보 반환.
        순매수 = i_df.groupby(['날짜','투자자'])['순매수거래대금'].sum().unstack().fillna(0)/1000000
        매수 = i_df.groupby(['날짜','투자자'])['매수거래대금'].sum().unstack().fillna(0)
        매도 = i_df.groupby(['날짜','투자자'])['매도거래대금'].sum().unstack().fillna(0)
        
        start = 순매수.index.min()
        end = 순매수.index.max()
        
        # start_ma3_value = s.df.loc[start,ma] ## ohlcv데이터가 있어야함.
        
        주도기관 = list(순매수.filter(items=col).sum().sort_values(ascending = False).index[:2]) ## 주도세력 찾기
        적용기관 = list((순매수.filter(items=col) != 0).sum().index)
        
        
        매도대금 = 매도.filter(items=col).sum().sum()
        매수대금 = 매수.filter(items=col).sum().sum()
        순매수대금 = 순매수.filter(items=col).sum().sum().round(1)
        순매수대금_억 = (순매수대금 / 100).round(1)
        if 매도대금 != 0 :
            매집비 = round(매수대금 / 매도대금 *100)
            풀매수여부 = ""
        else:
            매집비 = 1000
            풀매수여부 = "풀매수"
        
        start_ma3_value = ohlcv_df.loc[start,ma]
        current_price = ohlcv_df.Close[-1]
        pre_price = ohlcv_df.loc[start,f'{ma}_전저고점']
        저점대비현재가상승률 = round(((current_price/pre_price)-1 )* 100,1)


        result_dic['기간'] = (start,end)
        result_dic['순매수대금'] = 순매수대금
        result_dic['순매수대금_억'] = 순매수대금_억
        
        result_dic['매수대금'] = 매수대금
        result_dic['매도대금'] = 매도대금
        result_dic['적용기관'] = 적용기관
        result_dic['주도기관'] = 주도기관
        result_dic['매집비'] = 매집비
        result_dic['풀매수여부'] = 풀매수여부
        result_dic['start_ma3_value'] = start_ma3_value
        result_dic['저점대비현재가상승률'] = 저점대비현재가상승률
        
        
        return result_dic    
    
    
    def _buy_rate_status(self, ma = 'ma3'):
        dic = {}
        code =  self.code
        # print(code)
        # 저점날짜 = self.info_ma3.get('저점날짜')
        
        # 저점날짜 새로지정하기.
        x_date_ls = []
        try:
            x_date1 = x_date = self.info_ma20['저점날짜'][-2]
            x_date_ls.append(x_date1)
            # print("1")
        except:
            pass
        try:
            x_date2 = self.info_ma20['저점날짜'][-1]
            x_date_ls.append(x_date1)
            # print("2")
        except:
            pass
        try:
            if self.info_ma3_ma20.iloc[-1]['cross_status'] =='gc':

                dc_date = self.info_ma3_ma20.index[-2]
                gc_date = self.info_ma3_ma20.index[-1]
                x = self.df[(self.df['ma3_변곡점_변화량'] <  0)].loc[dc_date : gc_date]
                x_date3 = x['ma3_변곡점'].idxmin()
                x_date_ls.append(x_date3)
                # print("3")
        except:
            pass
        try:
            x_date4 = self.info_ma3['저점날짜'][0]
            x_date_ls.append(x_date4)
            # print("4")
        except:
            pass
        
        last_x_date = min(x_date_ls)
        # 단순저점데이터들
        cond = (self.df['ma3_변곡점_변화량'] <  0)
        저점날짜 = self.df.loc [ last_x_date : ].loc[cond].index
                
        
        
        ####################################
        
        저점날짜= [저점.strftime('%Y-%m-%d') for 저점 in 저점날짜]
        end = self.df.index[-1].strftime('%Y-%m-%d')
        if 저점날짜[-1] == end:
            구분날짜들 = 저점날짜
        else:
            구분날짜들 = 저점날짜 + [end]
            
        # print(구분날짜들)

        if (저점날짜 !=None) & (len(저점날짜) > 1 ) : ## 현재저점이 2개이상만 
            # 저점시작부터 끝까지 전체상황
            start = 저점날짜[0]
            # print(f'start: {start} code: {code}')

            # db에서 가져오기. 
            try:
                db_path = self.data_path + "new_investor.db"
                con = sqlite3.connect(db_path)
                sql = f'SELECT * FROM "investor" where 티커 = "{code}" and 날짜 >= "{start}"'
                # print(sql)
                with con:
                    i_df = pd.read_sql(sql,con)
                    # i_df = i_df.set_index('날짜')
            except:
                print('investor 자료없음.')
                return None

            col = ['외국인','금융투자', '투신', '연기금','사모'] # 세부적.

            # print(i_df)
            
            # 전체 데이터( 의미 있는 데이터만 집계하기.)########################
            cond20pro = i_df['매수거래량'] * 0.2  < abs(i_df['순매수거래량'])
            cond_full_buy = i_df['매수거래량'] == i_df['순매수거래량']
            i_df = i_df[cond20pro | cond_full_buy]
            ############################################################
              
            전체 = self.__구간별정보추출(i_df, self.df, col)


            ## 구간별 확인하기.
            구간별 = []
            for i in range(len(구분날짜들)-1):
                try:
                    start = 구분날짜들[i]
                    end = 구분날짜들[i+1]
                    # print("start_end",start, end)
                    if i == len(구분날짜들)-2: # 마지막이면
                        temp_df = i_df.loc [ (i_df['날짜'] >= start) ]
                    temp_df = i_df.loc [ (i_df['날짜'] >= start) &  (i_df['날짜'] < end)  ]

                    result = self.__구간별정보추출(temp_df, self.df, col)

                    구간별.append(result)
                except:
                    continue
            # print("구간별 개수",len(구간별))
            
            dic['전체'] = [전체]
            dic['구간별'] = 구간별
            
        else:
            # print('매집비 의미없음')
            dic = {}
            
        return dic
    

    def _get_finance_table_from_db(self,gb = '연도별'):
        '''
        code, gb['연도별','분기별']
        '''
        try:
            tableName = 't_' + gb + "_" + self.code
            query = 'select * from ' + tableName
            file_path = self.data_path + "재무제표.db"
            conn = sqlite3.connect(file_path)
            with conn:
                pre_df = pd.read_sql(query , con = conn,index_col='주요재무정보')
            return pre_df.T
        except:
            return pd.DataFrame()

      ## 기본정보, 실적정보 가져오기.


    #####  데이터 가져오기. 
    def _arrange_data_재무(self):
        try:
            self._finance_table_y = self._get_finance_table_from_db('연도별').dropna(how = 'all')
            self._finance_table_q = self._get_finance_table_from_db("분기별").dropna(how = 'all')
            self.현금흐름표 = self._finance_table_y.filter(regex= '현금흐름')
            self.실적_y = self._finance_table_y[['매출액','영업이익','당기순이익']].copy()
            
            self.부채비율 = self._finance_table_q['부채비율'].dropna()[-1]
            self.유보율 = self._finance_table_q['자본유보율'].dropna()[-1]
            self.상장주식수 = self._finance_table_q['발행주식수(보통주)'].dropna()[-1]
            self.실적_q = self._finance_table_q[['매출액','영업이익','당기순이익']].copy()
            
            
            실적_y_성장률 = self.실적_y.pct_change().round(2)
            실적_q_yoy_성장률 = self.실적_q.pct_change(4).round(2)
            실적_q_qoq_성장률 = self.실적_q.pct_change().round(2)
            
            
            ## 연실적 status 정리.
            turnarround_cond1 = (self.실적_y['영업이익'] > 0  ) & (self.실적_y['영업이익'] *  self.실적_y['영업이익'].shift(1) < 0)
            turnarround_cond2 = (self.실적_y['당기순이익'] > 0  ) & (self.실적_y['당기순이익'] *  self.실적_y['당기순이익'].shift(1) < 0)
            minus_cond1 = (self.실적_y['영업이익'] <= 0  )
            minus_cond2 = (self.실적_y['당기순이익'] <= 0  )
            self.실적_y.loc[turnarround_cond1 | turnarround_cond2 ,'status'] = '턴어라운드'
            self.실적_y.loc[minus_cond1 | minus_cond2 ,'status'] = '적자'
            self.실적_y['status'] = np.where(pd.notnull(self.실적_y['status']) == True, self.실적_y['status'], 실적_y_성장률['영업이익'])
            
            
            ## 분기실적 status 정리 영업이익이든 당기순이익이든 적자나 턴어라운드를 먼저표기하고 아닌것들은 영업이익율로 status 채움.
            # 분기정보
            yoy_turnarround_cond1 =  (self.실적_q['영업이익'] > 0  ) & (self.실적_q['영업이익'] *  self.실적_q['영업이익'].shift(4) < 0)
            yoy_turnarround_cond2 =  (self.실적_q['당기순이익'] > 0  ) & (self.실적_q['당기순이익'] *  self.실적_q['당기순이익'].shift(4) < 0)
            yoy_minus_cond1 = (self.실적_q['영업이익'] <= 0  )
            yoy_minus_cond2 = (self.실적_q['당기순이익'] <= 0  )
            self.실적_q.loc[yoy_turnarround_cond1 | yoy_turnarround_cond2 ,'yoy_status'] = '턴어라운드'
            self.실적_q.loc[yoy_minus_cond1 | yoy_minus_cond2 ,'yoy_status'] = '적자'

            qoq_turnarround_cond1 = (self.실적_q['영업이익'] > 0  ) & (self.실적_q['영업이익'] *  self.실적_q['영업이익'].shift(1) < 0)
            qoq_turnarround_cond2 = (self.실적_q['당기순이익'] > 0  ) & (self.실적_q['당기순이익'] *  self.실적_q['당기순이익'].shift(1) < 0)
            qoq_minus_cond1 = (self.실적_q['영업이익'] <= 0  )
            qoq_minus_cond2 = (self.실적_q['당기순이익'] <= 0  )
            self.실적_q.loc[qoq_turnarround_cond1 | qoq_turnarround_cond2 ,'qoq_status'] = '턴어라운드'
            self.실적_q.loc[qoq_minus_cond1 | qoq_minus_cond2 ,'qoq_status'] = '적자'
           
            self.실적_q['yoy_status'] = np.where(pd.notnull(self.실적_q['yoy_status']) == True, self.실적_q['yoy_status'], 실적_q_yoy_성장률['영업이익'])
            self.실적_q['qoq_status'] = np.where(pd.notnull(self.실적_q['qoq_status']) == True, self.실적_q['qoq_status'], 실적_q_qoq_성장률['영업이익'])
            
        except Exception as e:
            print('_arrange_date_재무 오류',e)
            
    def _arrange_basic_info(self,file_name = 'naver_basic_info_df.pickle'):
        '''
        .{data_path}/basic_info.pickle df 에서 가져오기. 
        '''
        file_path = self.data_path + file_name
        with open(file_path, 'rb') as f:
            naver_basic_info_df = pickle.load(f)

        cond_name = naver_basic_info_df['code_name'] ==self.code_name
        cond_code = naver_basic_info_df['code'] == self.code
        cond = cond_name | cond_code
        if sum(cond):
            
            basic_info_dict = naver_basic_info_df.loc[cond].to_dict('records')[0]
            self.시가총액_억 = basic_info_dict.get('시가총액(억)')
            self.시총순위  = basic_info_dict.get('시총순위')
            self.액면가  = basic_info_dict.get('액면가')
            self.최고52주  = basic_info_dict.get('52주최고')
            self.최저52주  = basic_info_dict.get('52주최저')
            self.ROE  = basic_info_dict.get('ROE(지배주주)')
            # self.부채비율  = basic_info_dict.get('부채비율')
            self.당좌비율  = basic_info_dict.get('당좌비율')
            # self.유보율  = basic_info_dict.get('유보율')
            self.EPS  = basic_info_dict.get('EPS(원)')
            self.PER  = basic_info_dict.get('PER(배)')
            self.BPS  = basic_info_dict.get('BPS(원)')
            self.PBR = basic_info_dict.get('PBR(배)')
        else:
            self.시가총액_억 = np.nan
            self.시총순위  = np.nan
            self.액면가  = np.nan
            self.최고52주  = np.nan
            self.최저52주  = np.nan
            self.액면가  = np.nan
            self.ROE  = np.nan
            # self.부채비율  = np.nan
            self.당좌비율  = np.nan
            # self.유보율  = np.nan
            self.EPS  = np.nan
            self.PER  = np.nan
            self.BPS  = np.nan
            self.PBR = np.nan

        try:
            if self.부채비율 <= 30:
                부채비율 = 0
            else:
                부채비율 = self.부채비율
                
            self.현금가 = self.액면가 * (self.유보율 - 부채비율) / 100
        except:
            self.현금가 = 0
            
        if "현금가" in dir(self):
            if self.현금가 != 0:
                self.현재가 = self.df.Close[-1]
                self.현금가대비현재가 = int(((self.현재가 / self.현금가) - 1) * 100)
                
    def _arrange_issue_info(self,file_name = 'issue_code_list.pickle'):
        '''
        file_name : file path + name
        
        issue_code_list , naver_theme_df , naver_upjong_df 필요
        
        '''
        file_path = self.data_path + file_name
        with open(file_path, 'rb') as f:
            issue_code_list = pickle.load(f)
        
        if len(issue_code_list.columns):
            cond = issue_code_list['code']==self.code
        else:
            cond =[]
            print('issue_code 데이터 없음')

        if sum(cond):
            self.iss_name = issue_code_list.loc[cond,"이슈"].values[0]
            self.iss_title = issue_code_list.loc[cond,"title"].values[0]
            self.iss_date_str = issue_code_list.loc[cond,"iss_date"].values[0]
            self.iss_company_info = issue_code_list.loc[cond,"기업개요"].values[0]
            self.iss_other_issue = issue_code_list.loc[cond,"다른이슈"].values[0]
        else:
            self.iss_name = ""
            self.iss_title = ""
            self.iss_date_str = ""
            self.iss_company_info = ""
            self.iss_other_issue = ""
        
        ## 네이버테마정보 추가. 
        file_path = self.data_path + "naver_theme_df.pickle"
        with open(file_path,'rb') as f:
            naver_theme_df = pickle.load(f)

        if len(naver_theme_df.columns):
            cond_code  =naver_theme_df['code']==self.code
        else:
            condi_code =[]
            print('naver_theme정보 데이터 없음')

        if sum(cond_code):
            naver_themes = list(naver_theme_df.loc[cond_code,'name'])
            self.naver_themes = ','.join(naver_themes)
        else:
            self.naver_themes = ""
       
        ## 네이버업종정보 추가. 
        file_path = self.data_path + "naver_upjong_df.pickle"
        with open(file_path,'rb') as f:
            naver_upjong_df = pickle.load(f)

        if len(naver_upjong_df):
            cond_code  =naver_upjong_df['code']==self.code
        else:
            cond_code = []
            print('네이버업종 데이터 없음')
        if sum(cond_code):
            naver_upjong = list(naver_upjong_df.loc[cond_code,'name'])
            self.naver_upjong = ','.join(naver_upjong)
        else:
            self.naver_upjong = ""
    
    def get_ohlcv(self):
        db_file_name = '/home/sean/sean/data/ohlcv_date.db'
        con = sqlite3.connect(db_file_name)
        sql = f'SELECT * FROM "{self.code}"'
        with con:
            data = pd.read_sql(sql, con)
        data['Date'] = pd.to_datetime(data['Date'])
        data  = data.set_index('Date')
        return data
    
    def to_back(self,n=1):
        # n일만큼 뒤로 가기.
        if n > 0:
            n = -n
        if n == -1:
            self.df_min30 = self.df_min30
            self.df = self.df
        elif n ==0:
            pass
        
        else:
            last_day_for_min30 = self.df.index[n+1]
            last_day  =  self.df.index[n]
            self.df_min30 = self.df_min30.loc[:last_day_for_min30]
            self.df = self.df.loc[:last_day]

        self.info_coke = self._coke_status(self.df)
        ## 오류발행 가능성! data 부족시.
        self.info_sun = self._sun_status(self.df)
        self.info_ac = self._ac_status()
        self.info_ab_volume = self._ab_volume_status(self.df)
        
        self.info_ma3 = self.is_w_by_day(self.df,'ma3')
        self.info_ma5 = self.is_w_by_day(self.df,'ma5')
        self.info_ma20 = self.is_w_by_day(self.df,'ma20')
        self.info_ma60 = self.is_w_by_day(self.df,'ma60')
        self.info_ma120 = self.is_w_by_day(self.df,'ma120')
        self.info_ma240 = self.is_w_by_day(self.df,'ma240')
        
        
        self.info_ma3_ma20 = self._get_two_ma_info(self.df,'ma3','ma20')
        self.info_ma5_ma20 = self._get_two_ma_info(self.df,'ma5','ma20')
        
        self.info_ma3_ma60 = self._get_two_ma_info(self.df,'ma3','ma20')
        self.info_ma5_ma60 = self._get_two_ma_info(self.df,'ma5','ma20')
        
        self.info_ma20_ma60 = self._get_two_ma_info(self.df,'ma20','ma60')
        self.info_ma60_ma120 = self._get_two_ma_info(self.df,'ma60','ma120')
        self.info_ma120_ma240 = self._get_two_ma_info(self.df,'ma120','ma240')
        self.info_ma60_ma240 = self._get_two_ma_info(self.df,'ma60','ma240')
        self.info_ma120_ma240 = self._get_two_ma_info(self.df,'ma120','ma240')
        
        ## trix 
        self.info_trix = self._get_two_ma_info(self.df,'trix_sig','trix')
        ## 
        self.info_ma3_sun_max = self._get_two_ma_info(self.df,'ma3','sun_max')
        self.info_ma5_sun_max = self._get_two_ma_info(self.df,'ma5','sun_max')
        self.info_ma3_upper_bb = self._get_two_ma_info(self.df,'ma3','upper_bb')
        self.info_ma5_upper_bb = self._get_two_ma_info(self.df,'ma5','upper_bb')
        
        ## 30분봉작업
        self.get_ohlcv_min30(refresh = False)
        
        

    def _refresh_ohlcv(self,by='db',exchange="KS"):
        '''
        by : 'db', 'fdr'
        '''
        today = datetime.today()
        start = today - timedelta(days = 450)
        if exchange == "KS":
            if by=='db':
                self.df = self.get_ohlcv()
            elif by =='fdr':
                self.df = fdr.DataReader(self.code, start)
                
            else:
                self.df = self.get_ohlcv()
                
            if self.n != 0:
                self.df = self.df[:-self.n]       
        ## 
        self.refresh_day_data_arrange()        
        
    def _refresh_ohlcv_with_all_current_price(self,all_current_price_data):
        '''
        class 내에서 당일 데이터받아 업데이트 
        이전날까지 데이터가 있다는 가정하에.
        '''
        
        # 전체데이터에서 추출해서 ohlcv 업데이트.
        extract_date = pd.to_datetime(all_current_price_data.loc[self.code,"Date"].date())
        extract_data = all_current_price_data.loc[self.code,["Open","High","Low","Close","Volume","Amount","Change"]]
        self.df.loc[extract_date] = extract_data
            
        self.refresh_day_data_arrange()
            
                        
    def refresh_day_data_arrange(self):
        self.df = self._add_ma(self.df)

        backward_df = self.df.copy() ## 거꾸로
        backward_df  = -backward_df
        
        
        self.info_coke = self._coke_status(self.df)
        ## 오류발행 가능성! data 부족시.
        self.info_sun = self._sun_status(self.df)
        self.info_ac = self._ac_status()
        self.info_ab_volume = self._ab_volume_status(self.df)
        
        ## is_w_by_day 에서 info_sun, ac 등등 으 ㄹ사용하기 때문에 순서바꾸면 안됨.
        self.info_ma3 = self.is_w_by_day(self.df,'ma3')
        self.info_ma3_backwards = self.is_w_by_day(backward_df,'ma3')
        self.info_ma5 = self.is_w_by_day(self.df,'ma5')
        self.info_ma20 = self.is_w_by_day(self.df,'ma20')
        self.info_ma20_backwards = self.is_w_by_day(backward_df,'ma20')
        self.info_ma60 = self.is_w_by_day(self.df,'ma60')
        self.info_ma120 = self.is_w_by_day(self.df,'ma120')
        self.info_ma240 = self.is_w_by_day(self.df,'ma240')
        
        
        self.info_ma3_ma20 = self._get_two_ma_info(self.df,'ma3','ma20')
        self.info_ma5_ma20 = self._get_two_ma_info(self.df,'ma5','ma20')
        
        self.info_ma3_ma60 = self._get_two_ma_info(self.df,'ma3','ma20')
        self.info_ma5_ma60 = self._get_two_ma_info(self.df,'ma5','ma20')
        
        self.info_ma20_ma60 = self._get_two_ma_info(self.df,'ma20','ma60')
        self.info_ma60_ma120 = self._get_two_ma_info(self.df,'ma60','ma120')
        self.info_ma120_ma240 = self._get_two_ma_info(self.df,'ma120','ma240')
        self.info_ma60_ma240 = self._get_two_ma_info(self.df,'ma60','ma240')
        self.info_ma120_ma240 = self._get_two_ma_info(self.df,'ma120','ma240')
        
        ## trix 
        self.info_trix = self._get_two_ma_info(self.df,'trix_sig','trix')
        
        ## 
        self.info_ma3_sun_max = self._get_two_ma_info(self.df,'ma3','sun_max')
        self.info_ma5_sun_max = self._get_two_ma_info(self.df,'ma5','sun_max')
        self.info_ma3_upper_bb = self._get_two_ma_info(self.df,'ma3','upper_bb')
        self.info_ma5_upper_bb = self._get_two_ma_info(self.df,'ma5','upper_bb')


    def _get_data_daum(self , option="30분봉",**kewords ):
        '''
        option : 30분봉, 5분봉, 주봉 , 월봉, 
        '''
        df = Daum.get_ohlcv(self.code, option,**kewords)
        df = self._add_ma(df , option=option)
        return df
    
    def get_ohlcv_min30(self,refresh = True):
        
        if 'df_min30' in dir(self):
            if refresh:
                self.df_min30 = self._get_data_daum('30분봉')
        else:
            self.df_min30 = self._get_data_daum('30분봉')
            
        self.info_min30_coke = self._coke_status(self.df_min30,candle = 'min')
        # ## 오류발행 가능성! data 부족시.
        self.info_min30_sun = self._sun_status(self.df_min30)
        self.info_min30_ac = self._ac_status()
        self.info_min30_ab_volume = self._ab_volume_status(self.df_min30)

        self.info_min30_ma10 = self.is_w_by_min30('ma10')
        self.info_min30_ma20 = self.is_w_by_min30('ma20')
        self.info_min30_ma40 = self.is_w_by_min30('ma40')
        self.info_min30_ma60 = self.is_w_by_min30('ma60')
        # self.info_min30_ma120 = self.is_w_by_min30('ma120')
        self.info_min30_ma240 = self.is_w_by_min30('ma240')
        
        self.info_min30_ma40_ma240 = self._get_two_ma_info(self.df_min30,'ma40','ma240')
        self.info_min30_ma60_ma240 = self._get_two_ma_info(self.df_min30,'ma60','ma240')
                
        ## trix 
        self.info_min30_trix = self._get_two_ma_info(self.df_min30,'trix_sig','trix')
        
        ## 
        self.info_min30_ma10_sun_max = self._get_two_ma_info(self.df_min30,'ma10','sun_max')
        self.info_min30_ma20_sun_max = self._get_two_ma_info(self.df_min30,'ma20','sun_max')
        self.info_min30_ma10_upper_bb = self._get_two_ma_info(self.df_min30,'ma10','upper_bb')
        self.info_min30_ma20_upper_bb = self._get_two_ma_info(self.df_min30,'ma20','upper_bb')
       
       
class Ant_tech(Anal_tech):
    

    def is_신규상장주(self):
        result = False
        정지예상기간 = len(self.df[self.df['Volume']==0])
        if 정지예상기간 > 2:
            return False
        
        cond_day = 25 < len(self.df) <= 240  ## 상장된지 얼마 안됐고.1년이내.

        start_close = self.df.Close[0]
        last_close = self.df.Close[-1]
        cond_price_loc = start_close * 0.7  < last_close < start_close * 1.1   ## 현재 위치가 거의다왔고.

        up_data = self.df['Close'] > start_close
        down_data = self.df['Close'] < start_close
        cond_flow = sum(up_data) < sum(down_data) ## 주가위치가 아래가 더 많았고.

        if len(self.info_ma3['저점날짜']):
            start_day = self.info_ma3['저점날짜'][0]
            ac_data = self.info_ac[0].loc[start_day:]
            cond_v = len(ac_data) > 0
        else:
            cond_v = False    ## 최근저점부터 현재까지 ac가 존재하고.
        if cond_day & cond_price_loc & cond_flow & cond_v:
            result = True
            
        return result


    def is_min30_breakthrough_coke(self,n=1):
        '''
        30분봉상 sun coke up 체크하기. 
        n = 만족하는 범위설정. 최근 n봉 에 조건만족이 있으면 return True
        '''  
        result = False
        if not "df_min30" in dir(self):
            return result
            
        df = self.df_min30.iloc[-(n+2):]

        cond몸통 = abs(df['Open'] - df['Close']  ) / (df['High'] - df['Low']) > 0.5
        cond아래꼬리 = ((df[['Open','Close']].min(axis=1)) - df['Low']  ) / (df['High'] - df['Low']) > 0.8
        cond_volume = df['Volume'] > df['vol20ma'].shift(1) * 1.3  ## 평균거래량 보다 2배조건
        
        cond_1 =  df['Low'] < df['upper_bb'].shift(1)  
        cond_1_1 = df['Close'].shift(1) < df['upper_bb'].shift(1)
        cond_2 = df['Close'] > df['upper_bb'].shift(1)   ## 돌파조건
        
        cond = (cond_1 | cond_1_1) & cond_2 & cond_volume & cond몸통      ### 몸통조건. 꼬리 큰거 빼고.아래꼬리도 포함해야하나.???
        result_coke = sum(cond.iloc[-n:])

        if result_coke > 0:
            result = True
        return result
    
    def is_min30_breakthrough_sun(self,n=1):
        '''
        30분봉상 뚫고 나가는 캔들 찾기 단타 타이밍에 사용.
        '''
        
        result = False
        if not "df_min30" in dir(self):
            print('30분봉 데이터 없음')
            return result
        
        # df = self.df_min30
        df = self.df_min30.iloc[-(n+2):]

        cond몸통 = abs(df['Open'] - df['Close']  ) / (df['High'] - df['Low']) > 0.5
        # cond아래꼬리 = ((df[['Open','Close']].min(axis=1)) - df['Low']  ) / (df['High'] - df['Low']) > 0.8
        cond_volume = df['Volume'] > df['vol20ma'].shift(1) * 1.3
        
        cond_1 =  df['Low'] < df['sun_max'].shift(1)
        cond_1_1 = df['Close'].shift(1) < df['sun_max'].shift(1) ## 갭상승포함.
        cond_2 = df['Close'] > df['sun_max'].shift(1)
        
        cond = (cond_1 | cond_1_1) & cond_2 & cond_volume & cond몸통
        result_sun = sum(cond.iloc[-n:])

        if result_sun > 0:
            result = True
        return result

    
    def is_buy_time_by30(self):
        '''
        # sun coke 상태 활용 방안. 신버전.
        
        coke돌파는 구현함.
        
        sun 은 어떻게 해야할지 구상중.
        '''
        
        result = False
        try:
            self.get_ohlcv_min30()
            
            if self.info_min30_ma10['현재방향'] == '상승':
                cond1 =  self.info_min30_ma40['result'] | self.info_min30_ma60['result'] ## 3 or 5일선.
                temp_start_time = self.info_min30_ma10['저점날짜'][-1]   ## 마지막 저점지점.
                temp_30df = self.df_min30.loc[temp_start_time:]    ## 마지막 저점지점부터자료만 가고.
                cond3 = self.info_min30_ma10['가격이격도'] < 100.2     ## 10선과 가격이격도.
                cond4 = temp_30df.iloc[-1]['Low'] / temp_30df.iloc[-1]['ma10'] <100.2  ## 또ㄴ 10선과저점이격도 만족
                
                # candle 위꼬리가 50% 넘지않고 양봉인것이나. 아래꼬리가 있는것. 0봉 변화없을시 1봉으로 체크함. ====== 보정필요.
                candle_rate_dic0 = Anal_tech.cal_candle_rate(self.df_min30) 
                candle_rate_dic1 = Anal_tech.cal_candle_rate(self.df_min30,n=2) 
                cond1 = (candle_rate_dic0['몸통비율'] == 0) & (candle_rate_dic1['아래꼬리비율'] > 0) | ((candle_rate_dic1['몸통변동율'] > 0) & (candle_rate_dic1['윗꼬리비율'] <= 50))
                cond2 = (candle_rate_dic0['아래꼬리비율'] > 0) | ((candle_rate_dic0['몸통변동율'] > 0) & (candle_rate_dic0['윗꼬리비율'] <= 50))
                cond_candle = cond1 | cond2
                
                if cond1 & (cond3 | cond4):
                    result = True
                    self.points_dic['buytime_30'] = True # 
                    self.points_dic['reasons'].append("10v_30분")
                
                try:
                    손절선 = self.df_min30.loc[ self.info_min30_ma10['고점날짜'][-1]: self.info_min30_ma10['저점날짜'][-1]]['Low'].min()
                    현재가 = self.df_min30['Close'][-1]
                    손절폭 = ((손절선/현재가) - 1) * 100
                    # txt = f"손절가: {int(손절선):,}({손절폭:.1f}%)"
                    # self.points_dic['reasons'].append(txt)
                    self.points_dic['손절가'] = int(손절선)
                    self.points_dic['손절폭'] = 손절폭
                    
                except:
                    pass
                    
            
            if self.is_min30_breakthrough_sun():
                words = ["coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","abc_거래만족후상승_*"]
                finded_word = [word for word in words if word in self.points_dic['reasons']]
                
                self.points_dic['buytime_30'] = True # 
                # if self.info_min30_coke['result']:
                if len(finded_word):
                    self.points_dic['reasons'].append("sun_돌파_30분_***")
                else:
                    self.points_dic['reasons'].append("sun_돌파_30분_**")
                # else:
                #     self.points_dic['reasons'].append("*30분sun돌파")
                self.points_dic['type'].append(1)
                result = True
            
            if self.is_min30_breakthrough_coke():
                words = ["coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","abc_거래만족후상승_*"]
                finded_word = [word for word in words if word in self.points_dic['reasons']]
                
                self.points_dic['buytime_30'] = True # 
                
                if len(finded_word):
                    self.points_dic['reasons'].append("코크_돌파_30분_***")
                else:
                    self.points_dic['reasons'].append("코크_돌파_30분_**")
                        
                
                self.points_dic['type'].append(1)
                result = True
            
            # reasons 중복제거.
            self.points_dic['reasons'] = sorted(list(set(self.points_dic['reasons'])))
            
        except Exception as e:
            print(f'is_buy_time_by30 오류 {e}')
            pass
        return result
    
    def check_df_min30(self):
        # 구버전
        result = False
        
        try:
            self.get_ohlcv_min30()

            # cond_40 = self.df_min30['ma40변화량'][-1] > 0  # 10선 우향이면. 
            # cond_60 = self.df_min30['ma60변화량'][-1] > 0  # 10선 우향이면. 
            cond_40 = self.info_min30_ma40['result']
            cond_60 = self.info_min30_ma60['result']
            
            ########################################################
            cond1_0 = self.info_min30_ma10['현재방향'] == "상승"
            cond1_1 = self.info_min30_ma10['변곡지속일'] == 1

            cond2_0 = self.info_min30_ma10['현재방향'] == "상승"
            cond2_1 = self.info_min30_ma10['변곡지속일'] <= 7
            cond2_2 = self.info_min30_ma10['가격이격도'] <= 100.5 # 가격이격도.

            cond_ma10 = (cond1_0 & cond1_1) | (cond2_0 & cond2_1 & cond2_2)
            
            if (cond_40 | cond_60) & cond_ma10 : # 
                result = True
        
        except:
            pass
        
        return result
            
        # self.info_min30_ma10 = self.is_w_by_day(self.df_min30,'ma10')
    
    
    def is_상투거래량(self):
        '''
        ## 기본정보가 있어야 비교 가능. 
        최근 6개월간 상투거래가 있는지 확인.  
        and 그 위치.(이평)이 nomalize에서 어느 위치인지 확인.
        
        '''
        dic = {}
        dic['result'] = False
        dic['상투거래일'] = []
        dic['상투거래위치'] = []
        
        if "상장주식수" in dir(self):
            df = self.df.iloc[-120:].copy()
            
            over_df = df[df['Volume'] > self.상장주식수]
            # 6개월 자르기. 120일로 지정. 
            
            if len(over_df)>0:
                last_over_v_days = over_df.index  ## 상투거래의심일 리스트
                nomalized_price = Sean_func.nomalize(self.df['Close']) ## 전체거래일의 가격을 위치값으로 normalize
                
                over_day_value = nomalized_price.loc[last_over_v_days]
                result  = over_day_value[over_day_value >0.5]
                # result  = nomalized_price.loc[last_over_v_days][nomalized_price.loc[last_over_v_days] > 0.8] 
                if len(result):
                    dic['result'] = True
                    dic['상투거래일'] = list(result.index)
                    dic['상투거래위치'] = list(result.values)
                    return dic
                else:
                    dic['result'] = False
                    dic['상투거래일'] = list(over_day_value.index)
                    dic['상투거래위치'] = list(over_day_value.values)
            
        return dic
    
    
    
    def is_바닥캔들_status(self, df):
        '''
        return : dict (주봉에 사용하기.)
        result, case, info
        '''
        temp_df = df.iloc[-5:,:4]
        dic = {}
        #111
        음 = temp_df.iloc[-3].to_dict()
        단 = temp_df.iloc[-2].to_dict()
        양 = temp_df.iloc[-1].to_dict()
        dic['111'] = {"음":음,
                    '단':단,
                    '양':양,}
        #121
        음 = temp_df.iloc[-4].to_dict()
        단 = {'Open': temp_df.iloc[-3:-1]['Open'].iloc[0],
            'High': temp_df.iloc[-3:-1]['High'].max(),
            'Low': temp_df.iloc[-3:-1]['Low'].min(),
            'Close': temp_df.iloc[-3:-1]['Close'].iloc[-1]}
        양 = temp_df.iloc[-1].to_dict()
        dic['121'] = {"음":음,
                    '단':단,
                    '양':양,}

        #112
        음 = temp_df.iloc[-4].to_dict()
        단 = temp_df.iloc[-3].to_dict()
        양 = {'Open': temp_df.iloc[-2:]['Open'].iloc[0],
            'High': temp_df.iloc[-2:]['High'].max(),
            'Low': temp_df.iloc[-2:]['Low'].min(),
            'Close': temp_df.iloc[-2:]['Close'].iloc[-1]}
        dic['112'] = {"음":음,
                    '단':단,
                    '양':양,}

        #131
        음 = temp_df.iloc[-5].to_dict()
        단 = {'Open': temp_df.iloc[-4:-1]['Open'].iloc[0],
            'High': temp_df.iloc[-4:-1]['High'].max(),
            'Low': temp_df.iloc[-4:-1]['Low'].min(),
            'Close': temp_df.iloc[-4:-1]['Close'].iloc[-1]}
        양 = temp_df.iloc[-1].to_dict()
        dic['131'] = {"음":음,
                    '단':단,
                    '양':양,}

        #122
        음 = temp_df.iloc[-5].to_dict()
        단 = {'Open': temp_df.iloc[-4:-2]['Open'].iloc[0],
            'High': temp_df.iloc[-4:-2]['High'].max(),
            'Low': temp_df.iloc[-4:-2]['Low'].min(),
            'Close': temp_df.iloc[-4:-2]['Close'].iloc[-1]}
        양 = {'Open': temp_df.iloc[-2:]['Open'].iloc[0],
            'High': temp_df.iloc[-2:]['High'].max(),
            'Low': temp_df.iloc[-2:]['Low'].min(),
            'Close': temp_df.iloc[-2:]['Close'].iloc[-1]}

        dic['122'] = {"음":음,
                    '단':단,
                    '양':양,}
        ### 여기까지는 경우의 수 범위 지정. 
        
        ## 실제 바닥캔들인지 확인하기. 
        result_dic = {}
        ls = []
        info = []
        for key, value in dic.items():
            첫째봉_음봉조건= value['음']['Open'] > value['음']['Close']
            두번째봉_아래꼬리조건 = value['단']['Close'] > value['단']['Low'] 
            두번째봉_고가조건 = value['음']['High'] > value['단']['High']
            세번째봉_전고돌파조건 = max(value['단']['Close'],value['단']['Open']) <  value['양']['Close']
            세번째봉_양봉조건 = value['양']['Open'] < value['양']['Close']

            if all([첫째봉_음봉조건,두번째봉_아래꼬리조건,두번째봉_고가조건,세번째봉_전고돌파조건,세번째봉_양봉조건]):

                ls.append(key)


                low = df['Low'].min() 
                try:
                    ma20_value = df.loc[ df['Low'].idxmin() ,'ma20']
                    이격도_20_저가 = round(low / ma20_value * 100,1)
                except:
                    이격도_20_저가 = np.nan
                try:
                    ma60_value = df.loc[ df['Low'].idxmin() ,'ma60']
                    이격도_60_저가 = round(low / ma60_value * 100,1)
                except:
                    이격도_60_저가 = np.nan





        if len(ls):
            result_dic["result"] = True
            result_dic['case'] = ls
            result_dic['info'] = {"이격도_20_저가":이격도_20_저가 , "이격도_60_저가":이격도_60_저가}
        else:
            result_dic["result"] = False
            result_dic['case'] = []
            result_dic['info'] = {"이격도_20_저가":np.nan , "이격도_60_저가":np.nan}

        
        
        
        return result_dic        
    
    def is_역배열_status(self):
        '''
        60이상 역배열. 
        이평정보가 없는경우 return : False 
        '''
        
        cnt = len(self.df)
        arr = np.array([60,120,240])
        check_mas = arr[arr<=cnt]
        # print('check_mas : ',check_mas)
        if len(check_mas)>1:
            # 두개이상일 경우 비교하기. 
            str_check_mas = [f'ma{str(ma)}' for ma in check_mas]
            result = []
            # 순수 배열확인.
            for i in range(len(str_check_mas)-1):
                if self.df.iloc[-1][str_check_mas[i]] < self.df.iloc[-1][str_check_mas[i+1] ]:
                    result.append(True)
                else:
                    result.append(False)
            # print('결과:',result)
            
            # ## 방향 확인. 
            # for ma in str_check_mas:
            #     if self.df.iloc[-1][ma] < self.df.iloc[-2][ma]: # 우하향 
            #         result.append(True)
            #     else:
            #         result.append(False)
            
            # ## 배열과 방향 모두 아래인것만 역배열 인정하기. !
            if all(result):
                return True
            else:
                return False
        else:
            return False
    
    def is_정배열_status(self):
        '''
        60이상 역배열. 
        이평정보가 없는경우 return : False 
        60만 존재하는경우 60의 방향으로 판단. 우향 - 정배열
        '''
        
        cnt = len(self.df)
        arr = np.array([60,120,240])
        check_mas = arr[arr<=cnt]
        # print('check_mas : ',check_mas)
        if len(check_mas)>1:
            # 두개이상일 경우 비교하기. 
            str_check_mas = [f'ma{str(ma)}' for ma in check_mas]
            result = []
            # 순수 배열확인.
            for i in range(len(str_check_mas)-1):
                if self.df.iloc[-1][str_check_mas[i]] > self.df.iloc[-1][str_check_mas[i+1] ]:
                    result.append(True)
                else:
                    result.append(False)
            # print('결과:',result)
            
 
            if all(result):
                return True
            else:
                return False
            
        elif len(check_mas) ==1:
            try:
                ma = check_mas[0]
                
                ma_info = eval(f"self.info_{ma}")
                cond_dir = ma_info['현재방향'] == '상승'
                cond_period = ma_info['변곡지속일'] > 3
                if cond_dir & cond_period:
                    return True
                else:
                    return False
            except:
                return False
        else:
            try:
                if cnt > 21:
                    ma_info = eval(f"self.info_{ma}")
                    cond_dir = ma_info['현재방향'] == '상승'
                    if cond_dir & cond_period:
                        return True
                    else:
                        return False
                else:
                    return False
            except:
                return False
            
    
    def is_alphabeta_status(self):
        '''
        alphabeta상태
        points
        '''
        result = False
        try:
            if "info_ma20" in dir(self):
                cond_alpah = self.info_ma20['alphabeta']
                if cond_alpah:
                    items = list(set([i for x in self.info_ma20["alphabeta_gc_ls"] for i in x]))
                    cond_only_alpha = 'ma20' not in items
        #             cond_only_alpha = 'ma20' not in [item[0] for item in cls.info_ma20["alphabeta_gc_ls"]]:
                    if cond_only_alpha:    
                        low_ma_price = min([self.df.iloc[-1][item] for item in items])
                        if low_ma_price < self.df.iloc[-1]['ma20']:
                            result = True
                            if "알파베타" not in self.points_dic['reasons']:
                                self.points_dic['point'] += 1  ## 감점
                                self.points_dic['reasons'].append("알파베타")
                # 1점
        except:
            result = False
        return result    
    
    ### 추적종목의 범위 정하기. 
    # 20선 w 또는 대기이면서 3선이 20위에 있는것.
    
    def is_gcv_2060_status(self):
        result = False
        저점날짜들 = self.info_ma20['저점날짜']
        고점날짜들 = self.info_ma20['고점날짜']
        if len(저점날짜들):
            if len(저점날짜들) >=2:
                low_date = 저점날짜들[-2]
                high_date= 고점날짜들[-1]
            if len(저점날짜들) ==1:
                if 저점날짜들[-1] < 고점날짜들[-1]:
                    low_date = 저점날짜들[-1]
                    high_date = 고점날짜들[-1]
                else:
                    return result

        try:
            gc_data = self.info_ma20_ma60.loc[low_date:high_date,'cross_status']
            if len(gc_data):
                if self.info_ma20_ma60.loc[low_date:high_date,'cross_status'][-1] == 'gc':
                    if len(self.info_ma20_ma60.loc[high_date:,'cross_status']) ==0:  ## 다시 dc 되지 않는것만 찾기.
                        result = True
        except:
            pass
        
        return result
    
    def is_gcv_status(self,long_ma= 'big_ac'):
    
        '''
        
        long_ma : 실제 df columns명  or 'big_ac'
        big_ac 는 알파거래량.
        '''
        result = False
        short_mas = ['ma3','ma5']

        
        for short_ma in short_mas:
            ma_info = eval(f"self.info_{short_ma}")
            if len(ma_info['result_type_ls']):
                if "w대기" in ma_info['result_type_ls']:  # w대기
                    first_low_date =  ma_info['저점날짜'][-1]
                    last_date = self.df.index[-1]
                elif 'w완성' in ma_info['result_type_ls']:
                    first_low_date = ma_info['저점날짜'][-2]
                    last_date = ma_info['저점날짜'][-1]
                else:
                    continue
                
                if long_ma == 'big_ac':
                    if len(self.info_ac[1]):
                        cond1 = first_low_date < self.info_ac[1].index[-1]
                        if cond1 :
                            result = True
                            break
                    else:
                        break
                else:
                    ## first_low_date, last_date, 사이에 long_ma 와 gc가 있는지 확인하기.
                    cond1 = self.df.loc[first_low_date , short_ma] < self.df.loc[first_low_date , long_ma] 
                    cond2 = self.df.loc[last_date , short_ma] > self.df.loc[last_date , long_ma] 
               
                    if cond1 & cond2:
                        result = True
                        break
        
        return result
        
    
    def _장기추세_status(self):
        '''
        60,120,240 선중 지속5일 이상    방향만 비교. 
        비율로 50프로이상이면 인정.2:1 1:1 까지 인정.
        '''
        cnt = len(self.df)
        arr = np.array([60,120,240])
        exist_mas = arr[arr<cnt]
        mas = [f"ma{ma}" for ma in exist_mas]

        if len(mas) == 0:
            return False
        result_ls = []
        for ma in mas:
            ma_info = eval(f"self.info_{ma}")
            ma_up_cond = ma_info['현재방향'] =='상승'
            ma_up_period = ma_info['변곡지속일'] >= 5
            boolean = ma_up_cond & ma_up_period
            result_ls.append(boolean)
        result = sum(result_ls) / len(result_ls)
        # print(result)
        if result >=0.5:
            return True
        else:
            return False
    
    def is_sun_status(self):
        '''
        역배열 아니고 info_sun result | info_coke
        '''
        result = False
        if not self.is_역배열_status():
            if self.info_sun['result']:
                if self._장기추세_status():
                    result = True
        return result
    
    def is_coke_status(self):
        '''
        역배열 아니고 info_sun result | info_coke
        '''
        result = False
        if not self.is_역배열_status():
            if self.info_coke['result']:
                if self._장기추세_status():
                    result = True
        return result
    
    def is_실적주_status(self,option = 'y'):
        '''
        option : y,q
        '''
        consen_year = Sean_func.실적기준구하기('y')[1][-1]
        consen_q = Sean_func.실적기준구하기('q')[1][-1]
        
        result = False
        if option == 'y':
            try:
                if consen_year in self.실적_y.index:
                    growth_rate_year = self.실적_y.loc[consen_year,'status']
                else:
                    growth_rate_year = None

                if growth_rate_year != None:
                    if type(growth_rate_year) == str:
                        if growth_rate_year == '턴어라운드':
                            result = True
                    elif type(growth_rate_year) == float:
                        if growth_rate_year >= 0.3:
                            result = True
            except Exception as e:
                print(e, self.code_name)
        
        elif option == 'q':
            try:
                if consen_q in self.실적_q.index:
                    growth_rate_yoy = self.실적_q.loc[consen_q,'yoy_status']
                    growth_rate_qoq = self.실적_q.loc[consen_q,'qoq_status']
                else:
                    growth_rate_yoy = None
                    growth_rate_qoq = None

                    if growth_rate_yoy!=None:
                        if type(growth_rate_yoy) == str:
                            if growth_rate_yoy == '턴어라운드':
                                result = True
                        if type(growth_rate_yoy) == float:
                            if growth_rate_yoy >= 0.3:
                                result = True
            except Exception as e:
                print(e, self.code_name)
                
                        
        return result
   
    def is_대기종목찾기_status(self):
        '''
        return : dict 
        ['result'] : True, False
        
        '''
        ## 역배열 제외
        
        result = {}
        
        if self.is_역배열_status():
            result['result'] =False
            return result
        
        hap = self.info_ma3['result_type_ls'] + self.info_ma5['result_type_ls']
        # if len([ls for ls in hap if 'w' in ls]):
        low_date = []
        if len(hap):
            ## w 이면 [-2]날짜부터. 
            if len(self.info_ma3['저점날짜']) >=2:
                low_date.append(self.info_ma3['저점날짜'][-2])
            elif len(self.info_ma3['저점날짜']) ==1:
                low_date.append(self.info_ma3['저점날짜'][-1])
            else:
                pass
            
            if len(self.info_ma5['저점날짜']) >=2:
                low_date.append(self.info_ma5['저점날짜'][-2])
            elif len(self.info_ma5['저점날짜']) ==1:
                low_date.append(self.info_ma5['저점날짜'][-1])
            else:
                pass
        
            ## 아니면 [-1]부터. 
                
            low_date = min(low_date)
            ## 날짜사이에 ac , gcv20 , gcv sun, gcv upper 있는지 확인하기.
            
            ################################################
            ac_cnt = len(self.info_ac[0].loc[low_date:])
            result['ac'] = ac_cnt
            
            ################################################
            gc_data = self.info_ma3_ma20.loc[low_date:,'cross_status']
            result['gcv20'] = False
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result['gcv20'] = True
            gc_data = self.info_ma5_ma20.loc[low_date:,'cross_status']
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result['gcv20'] = True
            
            ################################################3
            ## sun 과 gc
            result['gcv_sun'] = False
            gc_data = self.info_ma3_sun_max.loc[low_date: , "cross_status"]
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result['gcv_sun'] = True
            gc_data = self.info_ma5_sun_max.loc[low_date: , "cross_status"]
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result['gcv_sun'] = True
                    
            ################################################
            ## coke 과 gc
            result['gcv_coke'] = False
            gc_data = self.info_ma3_upper_bb.loc[low_date: , "cross_status"]
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result['gcv_coke'] = True
            gc_data = self.info_ma5_upper_bb.loc[low_date: , "cross_status"]
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result['gcv_coke'] = True
        
        result_cnt = [value for key ,value in result.items() if value ]
        result['result'] = len(result_cnt)
        
        return result
    
    def is_today_ac(self,pct = 0.02):
        '''
        2프로이상 상승.
        '''
        result = False
        try:
            last_ac_day = self.info_ac[0].index[-1] # 최근ac날짜
            cond_ac_today = len(self.df.loc[last_ac_day:]) == 1 # ac가 가장 최근일. 즉 오늘인 조건.
            # 전일종가대비 현재가가 높은것.
            plus_cond = self.df.Close[-1] > self.df.Close[-2]
            plus_cond1 = self.df['Close'].pct_change()[-1] >= pct
            
            if cond_ac_today & plus_cond & plus_cond1 :
                result =  True
        except:
            pass
        
        return result
    
    def is_대기종목찾기_status1(self,ma = 'ma3'):
        '''
        return : dict 
        ['result'] : True, False
        
        '''
        ## 역배열 제외

        result = {}

        if self.is_역배열_status():
            result['result'] =False
            return result
        if not self._장기추세_status():
            result['result'] =False
            return result
        
        ma_info = eval(f"self.info_{ma}")
        
        ma20_info = self.info_ma20
        # if len([ls for ls in hap if 'w' in ls]):
        
        if len(ma_info['result_type_ls']):
            ## w 이면 [-2]날짜부터. 
            if len(ma_info['저점날짜']) >=2:
                low_date = ma_info['저점날짜'][-2]
                high_date = ma_info['고점날짜'][-1]
            else:
                low_date = ma_info['저점날짜'][-1]
                high_date = ma_info['고점날짜'][-1]
                if low_date > high_date:
                    result['result'] = False
                    return result
                    
                    

            ## 날짜사이에 ac , gcv20 , gcv sun, gcv upper 있는지 확인하기.
            result['result'] = True   ## 단기파동. 대기종목으로 ok란 말임.
            result[f"type_{ma}"] = ','.join(ma_info['result_type_ls'])
            result[f'변곡지속일_{ma}'] = ma_info['변곡지속일']
            result[f'가격이격도_{ma}'] = ma_info['가격이격도']
            result[f'캔들저점과이격도_{ma}'] = int((self.df.Low[-1] / eval(f"self.df.{ma}[-1]")) *100)
            result[f'캔들저점과이격도_ma20'] = int((self.df.Low[-1] / self.df.ma20[-1]) *100)
            result[f'컨센현황'] = self.is_실적주_status()
            result[f'coke_status'] = self.is_coke_status()
            result[f'sun_status'] = self.is_sun_status()
            result['ma20'] = ','.join(self.info_ma20['result_type_ls'])
            
            # ab거래량비율 경과일 25일 이하만 취급.
            try:
                if self.info_ab_volume['ma20']['경과일'] <40:
                    ma20_abrate = self.info_ab_volume['ma20']['ab_v_rate']
                else:
                    ma20_abrate = 0
                if ma20_abrate == None:
                    ma20_abrate = 0
            except:
                ma20_abrate = 0
            try:
                if self.info_ab_volume['big_v']['경과일'] < 40:
                    big_v_abrate = self.info_ab_volume['big_v']['ab_v_rate']
                else:
                    big_v_abrate = 0
                if big_v_abrate == None:
                    big_v_abrate = 0
                
            except:
                big_v_abrate = 0
            
            result['ab_v'] = max(ma20_abrate,big_v_abrate)
            
            result['trix'] = self.info_trix['cross_status'][-1]
            if result['trix'] == 'gc':
                result['trix_gc_지속일'] = len(self.df.loc[self.info_trix.index[-1]:])
            else:
                result['trix_gc_지속일'] = 0
            ################################################
            ac_cnt = len(self.info_ac[0].loc[low_date:high_date])
            result['ac'] = ac_cnt
            result['today_ac'] = self.is_today_ac()
            
            ####################################
            ## 돌파여부 확인
            result['sun_up'] = False
            result['coke_up'] = False
            
            low = self.df.Low[-1],
            close = self.df.Close[-1] 
            sun_max = self.df.sun_max[-2]  
            upper_bb = self.df.upper_bb[-2]
            if low < sun_max < close:
                result['sun_up'] = True
            if low < upper_bb < close:
                result['coke_up'] = True
                    
            ################################################
            gc_data = eval(f"self.info_{ma}_ma20.loc[low_date:high_date,'cross_status']")
            
            result[f'gcv20_{ma}'] = False
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result[f'gcv20_{ma}'] = True

            ################################################3
            ## sun 과 gc
            result[f'gcv_sun_{ma}'] = False
            gc_data = eval(f"self.info_{ma}_sun_max.loc[low_date:high_date , 'cross_status']")
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result[f'gcv_sun_{ma}'] = True

            ################################################
            ## coke 과 gc
            result[f'gcv_coke_{ma}'] = False
            gc_data = eval(f"self.info_{ma}_upper_bb.loc[low_date:high_date , 'cross_status']")
            if len(gc_data):
                if gc_data[-1] == 'gc':
                    result[f'gcv_coke_{ma}'] = True

            ############################################
           
            
            
        # result_cnt = [value for key ,value in result.items() if value ]
        # result['result'] = len(result_cnt)
        if len(result) ==0:
            result['result'] = False
        return result
    
    def send_fig(self, msg):
        '''
        s : Stock 객체
        msg : msg객체
        자동메세지 보내기 에 사용  cnt 100적용.
        '''
        try:
            b = BytesIO()
            # b.name = 'image.jpg'
            f = self.plot(cnt = 100)
            f.savefig(b, format='png')
            b.seek(0)
            try:
                caption = f"{self.code_name} {self.called_cnt} \n"+ ",".join(self.points_dic['reasons'])
                print('메세지보내기 성공 send_fig')
            except:
                caption = f"{self.code_name} "+ ",".join(self.points_dic['reasons'])

            caption = caption + f",손절가:{self.points_dic['손절가']:,}, 손절폭({self.points_dic['손절폭']:.1f})"
            caption = caption + f"\n가격이격도(10ma):{self.info_min30_ma10['가격이격도']}"
            
            msg.send_photo(b, caption = caption)
            if not b.closed:
                b.close()
            pass
        except:
            pass
        
        
    
        
    def job1(q,q1,mode = 1,test_mode = False, new_5up_data = pd.DataFrame()):
        '''
        
        df 값 넣으면 타이밍 찾기. 
        df 값 없으면 대기종목들 찾기.
        mode = 1 : 장중 
        mode = 2 : 종가매수 전일추천종목() + 4프로이상종목
        mode = 3 : 시간외매수용.(전일 추천 전체종목 + 4프로이상상승종목)
        mode = 4 : 대기종목추출
        1,2 는 기존데이터로 작업.
        3 ,4은 전체 데이터로 작업.
        '''
        print()
        print('job1 ver.3 running...')
        print()
        msg = Mymsg()
        msg_recommended = Mymsg(chat_name="종목추천채널")
        msg_sean_group = Mymsg(chat_name='sean_group')
        
        data_path = '/home/sean/sean/data/'
        today = datetime.today().date()
        pickle_file_name = '/home/sean/sean/data/temp_sended_dic.pkl'
        
        with open(pickle_file_name,'rb') as f:
            sended_code_dic = pickle.load(f)
        
        try:
            if sended_code_dic['date'] != today:
                sended_code_dic['date'] = today
                sended_code_dic['sended'] = []
        except:
            sended_code_dic = {}
            sended_code_dic['date'] = today
            sended_code_dic['sended'] = []
        
        
        try:
            if len(new_5up_data):
                new_5up_data.sort_values('Change',ascending =False)
        except:
            pass
        
        print("=========  sended_code_dic 현황   ==========")
        print(sended_code_dic)
        print("==============================================")
        
        
        ## 메세지 보내는 일이 아니면 스레드를 미리 죽여놓는다.
        if (mode == 3) | (mode == 4):
            if mode == 4:
                q.put('end')
                q1.put('end')
                ## 6시이후 전체종목 가져와서 recommended 찾기.
                dfs = Fnguide.get_ticker_by_fnguide(data_path= data_path)[2:]
                dfs['cd'] = dfs['cd'].apply(lambda x : x[1:])
            
            if mode == 3:
                ## 전체종목중 어제 추적종목들만 상승률 순으로 작업.
                file_name = '/home/sean/sean/data/pickup_codes_all.xlsx'
                recommended_dfs = pd.read_excel(file_name,dtype = {'cd' : str})
                if len(new_5up_data):
                    # 넘겨받은 new_5up_data 중 동전주제외, 5프로 이상상승종목
                    new_5up_data = new_5up_data[new_5up_data['Close'] > 1500]
                    new_5up_data = new_5up_data [new_5up_data['Change'] >= -2]

                    new_5up_data1 = pd.merge(new_5up_data , recommended_dfs ,how = 'right',left_on='Code',right_on='cd')
                    new_5up_data1 = new_5up_data1[new_5up_data1['cd'].notnull()]
                    new_5up_data1 = new_5up_data1[["cd","nm","gb",'Change']]
                    dfs = new_5up_data1.sort_values('Change',ascending=False)
                    dfs = dfs.drop_duplicates('cd',keep = "last")
                else:
                    dfs = recommended_dfs
                    
        
        elif (mode == 1) | (mode == 2):  ## mode1, mode2 # 
            
            ############################################
            all_dfs = Fnguide.get_ticker_by_fnguide(data_path= data_path,option= 'db')[2:]
            all_dfs['cd'] = all_dfs['cd'].apply(lambda x : x[1:])


            file_name = '/home/sean/sean/data/pickup_codes.xlsx'
            recommended_dfs = pd.read_excel(file_name,dtype = {'cd' : str})
                        
            if len(new_5up_data):
                new_5up_data =  new_5up_data[new_5up_data['Change'] > -2]
                new_5up_data = new_5up_data[new_5up_data['Close'] > 1500]

                new_5up_data1   = pd.merge(new_5up_data , all_dfs ,how = 'left',left_on='Code',right_on='cd')
                len(new_5up_data1)

                new_5up_data1 = new_5up_data1[new_5up_data1['nm'].notnull()]
                new_5up_data1 = new_5up_data1[['cd','nm','gb','Change','Close','Volume']]
                new_5up_data1 = new_5up_data1.sort_values('Change',ascending = False)


                dfs = pd.merge(recommended_dfs , new_5up_data1 ,how = 'outer')
                dfs = dfs.sort_values('Change',ascending = False)
                if mode == 1:
                    dfs = dfs[dfs['Change'].fillna(0) < 20]  ## 없는값 0으로 채우고 20프로 이상은 일단 배제.
                
            else:
                dfs = recommended_dfs
            
            dfs = dfs[:400] ### 500개 한정. 40분간격이면 다르다.
            
        else: ## mode 1,2,3 이 아니면. 종료.
            q.put('end')
            q1.put('end')
            sys.exit()
        
        
        # ### Test mode 인지 체크 하고 random 범위 지정. 
        if test_mode: 
            test_start_num = random.choice(range((len(dfs)-20)))
            dfs = dfs[test_start_num:test_start_num + 20]


        print("================  dfs정보  ===================")
        print(f"mode: {mode}   loop cnt (dfs): {len(dfs)}")
        print(dfs.head(5))
        print(dfs.tail(5))
        print("===============================================")

        ## test 용 all_cls 저장
        result_all_cls = []  ## 전체 cls 목록
        result_ls = []
        
        all_cnt = len(dfs) # 스캔종목 개수.
        stocks = Stocks() ## 수급또는 이슈파일 
        
        ## 작업내용 출력.
        try:
            job_txt = f"loop_data_cnt:{all_cnt}, 작업 mode: {mode} 현재추천주개수 :{len(list(set(sended_code_dic['sended'])))} 기존자료_date : {sended_code_dic['date']} "
            if test_mode :
                job_txt = job_txt
            print(job_txt)
            msg.send_message(job_txt)
        except Exception as e:
            print(f'Exception : {e} \n여기여기3454번 오류!!!!!!!!!!!!!!!!!')
        
       
        ## 대량돌파종목또는 강력매수종목 list 
        recommended_list = []  ## 추천주목록
        recommended_dic = {}
        
        start_time = datetime.now()
        
        for i , row in dfs.iterrows():
            result_dic = {}
            if row['cd'][0] =="A":
                code = row['cd'][1:]
            else:
                code = row['cd']
            code_name = row['nm']
            gb = row['gb']
            print(f"{i}/{all_cnt}",code,code_name,end=".")
        

            try:
                ## 특정 시간 초과시 정지
                now = datetime.now()
                time_delta = now- start_time
                if mode == 1:
                    if time_delta.seconds > 30*60:
                        over_msg =  f'시간초과로 (30분) job1 stopped! last_code : {code_name}'
                        q.put('end')
                        q1.put('end')
                        print(over_msg)
                        sys.exit()
                        break 
            except:
                pass
            
            
            
            ## 메세지 보낸거면 패쓰. ( 단순 배제. 정교하게 할땐 제거햐야함. )
            if mode == 1:
                if code in sended_code_dic['sended'] :
                    if len(new_5up_data):
                        if not code in list(new_5up_data['Code']):  ## 현재 5프로이상 상승한 종목이 아니면.               
                            print('메세지 보낸파일에 있음. pass........')
                            cnt_sended = len([item for item in sended_code_dic['sended'] if item == code])
                            if cnt_sended > 4:  ## 3번 이전까지는 알리기.(기본적으로 10v존.sun돌파, coke 돌파.)
                                continue
                            
                
            try:
                
                s = Stock(code,code_name,gb,option='fdr')
                
                s.mode = mode   ## 객체만들어진 시기에 아예 mode 입력하기.
                #### insert reasonable ###########################################
                ## 최소.short_w 대기포함.
                
                ## min_coke이면 coke upper값
                if s.code in stocks.best_invest_codes:
                    s.points_dic['추적종목이유'].append("best_invest")
                    s.points_dic['point'] +=1
                if s.code in stocks.consen_up_codes:
                    s.points_dic['추적종목이유'].append(f"consen_up")
                    s.points_dic['point'] +=1
                if s.is_신규상장주(): 
                    s.points_dic['추적종목이유'].append("신규상장주")
                    s.points_dic['reasons'].append("신규상장주")
                    
                    s.points_dic['point'] +=1
                
                # # s.points_dic['point'] 에 넣기. 또는 확인하면서 점수 추가하기. 
                # ###################################################################
                
                if mode == 3:
                    result_all_cls.append(s) 
                
                if mode == 4: ## test용 all_cls 저장용. 
                    s.is_buy_time_by30()  ## 30분봉 자료 저장. 분석 테스트할때 사용
                    result_all_cls.append(s)
                
            except Exception as e:
                print(f'======== stock 객체생성 실패 =========={e}')
                continue
            
            # best_invest, consen_up
            if s.points_dic['추적종목여부']: ## 추적종목 df 만들기.
                result_dic['cd'] = code
                result_dic['nm'] = code_name
                result_dic['gb'] = gb
                result_dic['pnt'] = s.points_dic['point']
                result_dic['추적종목이유'] = s.points_dic['추적종목이유']
                
                 ## 대기종목저장용 추가.
                if mode == 4:
                    s.is_buy_time_by30()
                    ### 30분봉상태값을 저장하기 위함. 다음날 돌파찾기에 사용.
                    result_dic['coke_result'] = s.info_min30_coke['result']
                    result_dic['upper_current_value'] = s.info_min30_coke['upper_current_value']
                    result_dic['upper_current_status'] = s.info_min30_coke['upper_current_status']
                    result_dic['coke_width'] = s.info_min30_coke['coke_width']
                    
                    result_dic['sun_result'] = s.info_min30_sun['result']
                    result_dic['새출현재너비'] = s.info_min30_sun['새출현재너비']
                    result_dic['새출현재상단값'] = s.info_min30_sun['현재상단값']
                    try:
                        result_dic['매집비전체시작일'] = s._buy_rate_status().get('전체')[0].get('기간')[0]
                        result_dic['매집비최근시작일'] = s._buy_rate_status().get('구간별')[-1].get('기간')[0]
                    except:
                        pass
                    
                    result_ls.append(result_dic)
                    continue
            
                ###### 매수타임 알림.################################################ 
                # 장중 : 3_20gcv,sun_gcv, coke_gcv,
                # 장마감시: 3_20gcv,sun_gcv, coke_gcv, abc_vol만족,  sun_up, coke_up
                if s.points_dic['buytime']:   ### ???  if points_dic['buytime'] 으로 해야하나.>?
                    if mode == 1: ## 장중.
                        ## 장중에 30분봉확인후 메세지 보내기 
                        if s.is_buy_time_by30(): ## 신버전
                        # if s.check_df_min30():  ## 구버전
                            sended_code_dic['sended'].append(s.code)
                            cnt = len([item for item in sended_code_dic['sended'] if item == s.code])
                            s.called_cnt = cnt
                            
                            if test_mode == False:
                                if cnt < 5:     ## 5개이하면 신호전송.
                                    pass
                                    # q.put(s)
                            
                                #################### words 포함되면 알림할때 이렇게 하면됌.   ################################
                                words = ["coke_돌파_대량_**","coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","*30분sun돌파","*30분코크돌파","abc_거래만족후상승_*"]
                                # words = ["코크_돌파_30분_***","sun_돌파_30분_***"]
                                finded_word = [word for word in words if word in s.points_dic['reasons']]
                                if len(finded_word):
                                    # s.send_fig(msg_recommended)
                                    if cnt < 4:
                                        q.put(s)
                                    
                                words_30 = ["코크_돌파_30분_***","sun_돌파_30분_***"] ## --> 30분봉.
                                finded_word_30 = [word for word in words_30 if word in s.points_dic['reasons']]
                                if len(finded_word_30):
                                    q1.put(s)
                                    try:
                                        msg_sean_group.send_message(f"{s.code_name} 30분봉 돌파상황확인\n{'|'.join(s.points_dic['reasons'])}")
                                    except:
                                        print("30분돌파메세지 실패")
                                    
                                ####################    ################################
                            continue
                            
                    elif (mode == 2) | (mode == 3):  ## 장마감쯤. # 시간외용포함
                        if s.is_buy_time_by30():
                            sended_code_dic['sended'].append(s.code)
                            cnt = len([item for item in sended_code_dic['sended'] if item == s.code])
                            s.called_cnt = cnt
                            if test_mode == False:
                                q.put(s)

                            words_30 = ["코크_돌파_30분_***","sun_돌파_30분_***"] ## --> 30분봉.
                            finded_word_30 = [word for word in words_30 if word in s.points_dic['reasons']]
                            if len(finded_word_30):
                                
                                try:
                                    msg_sean_group.send_message(f"{s.code_name} 30분봉 돌파상황확인\n{'|'.join(s.points_dic['reasons'])}")
                                except:
                                    print("30분돌파메세지 실패")
                            
                            
                            
                        #################### words 포함되면 알림할때 이렇게 하면됌.   ################################
                        words = ["coke_돌파_대량_**","coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","*30분sun돌파","*30분코크돌파","abc_거래만족후상승_*","abc_거래량만족"]
                        finded_word = [word for word in words if word in s.points_dic['reasons']]
                        if len(finded_word):
                            q1.put(s)
                            recommended_dic = {}
                            recommended_dic['추천날짜'] = s.df.index[-1]
                            recommended_dic['code'] = s.code
                            recommended_dic['code_name'] = s.code_name
                            recommended_dic['당시point'] = s.points_dic['point']
                            recommended_dic['추천일등락율'] = s.df['Close'].pct_change()[-1] *100 
                            recommended_dic['추천기법'] = '|'.join(finded_word)
                            recommended_dic['추적이유'] = '|'.join(s.points_dic['추적종목이유'])
                            recommended_dic['매수사유'] = '|'.join(s.points_dic['reasons'])
                            recommended_list.append(recommended_dic)
                            
                            
                            
                        ####################    ################################      
                    else:
                        pass
            else:
                continue
            
        ## 반복문 모두 끝남.
        q.put('end')    ## 메세지보내는 쓰레드 중지시키기.
        q1.put('end')    ## 메세지보내는 쓰레드 중지시키기.
         
        
        
        
        
        
        
        ## recommended_list 리스트 다시 보내기 .  
        print()
        print(f"{len(recommended_list)} 개자료 추천주")
        
        if len(recommended_list):
            temp_recommended_list = [item['code_name'] for item in recommended_list]
            txt ='\n'.join(temp_recommended_list)
            txt = f'추천종목({len(recommended_list)}개)\n'+ txt
            msg_recommended.send_message(txt)
            
            ## recommended_list - dataframe으로 변형해서 db로 추천종목리스트 db로 저장해놓기.
            if mode == 3:  
                #### recommended_list 임시로 pkl 저장하기.
                try:
                    recommended_list_file_name = '/home/sean/sean/data/temp_recommended_list.pkl'
                    with open(recommended_list_file_name,'wb') as f:
                        pickle.dump(recommended_list, f , protocol= pickle.HIGHEST_PROTOCOL)
                    msg.send_message(f"mode : {mode} recommended_list 임시 저장 완료.")
                except Exception as e:
                    print(f'recommended_list피클저장 오류,{e}')
                
                
                ## 
                try:
                    recommended_df = pd.DataFrame(recommended_list)
                    con = sqlite3.connect(f'{data_path}추천종목백업.db')
                    with con:
                        recommended_df.to_sql('recommended',con,if_exists='append',index=False)
                        try:
                            today = datetime.today().date()
                            기준날짜 = today - timedelta(days= 60)  ## 두달이상데이터 삭제.
                            기준날짜 = str(기준날짜)
                            cursor = con.cursor()
                            sql = f'DELETE FROM "recommended" WHERE 추천날짜 < "{기준날짜}"'
                            cursor.execute(sql)
                            con.commit()
                        except Exception as e:
                            print('추천종목백업 과거데이터 삭제 실패!',e)
                    
                
                except Exception as e:
                    print(f'추천종목  db저장 실패. {e}') 
                    msg.send_message(f"추적종목 임시 db저장 실패.{e}")
        
        
        ###### 데이터 정리.#########
        # 메세지 보낸메세지 목록 임시저장.
        if mode == 1:
            with open(pickle_file_name,'wb') as f:
                pickle.dump(sended_code_dic, f , protocol= pickle.HIGHEST_PROTOCOL)
                
        
        
        # con = sqlite3.connect(f'{data_path}추천종목백업.db')
        # sql = 'select * from "recommended"'
        # 추천종목백업 = pd.read_sql(sql,con)

        # 총데이터일 = len(추천종목백업['추천날짜'].unique())
        # if 총데이터일 >5:
        #     분석n = 5
        # else:
        #     분석n = 총데이터일 -1
        
        
        if mode == 3 :
            try:
                all_cls_file_name1 = '/home/sean/sean/data/all_cls_mode3.pkl'
                with open(all_cls_file_name1,'wb') as f:
                    pickle.dump(result_all_cls, f , protocol= pickle.HIGHEST_PROTOCOL)
                msg.send_message(f"mode : {mode} all_cls_mode3.pkl 저장 완료.")
            except Exception as e:
                print(e,'all_cls_mode3.pkl 저장실패')
                msg.send_message(f"{e},all_cls 저장실패 mode3 ")
                pass
        
            try:
                ## 저장후 과거추천주 분석하기 위한 데이터 저장해놓기.  ------>> filename 주의  n은 추후에 5로 고정하기.
                Sean_func.anal_recommended_data(all_cls_file_name1, n = 5)
                Sean_func.anal_recommended_data(all_cls_file_name1, n = 3)
                Sean_func.anal_recommended_data(all_cls_file_name1, n = 1)
                try:
                    msg.send_message(f"http://122.34.201.82:8501/")
                except:
                    print('분석서버 보내기 오류2')
                # try:
                #     sys.exit() ## mode =3  이 자꾸 종료가 되지 않아 억지 종료해봄.
                # except:
                #     pass
                
            except Exception as e:
                print(e,'Sean_func.anal_recommended_data 작동안함.')
                pass
            
        ## 다음날 체크할 대기종목 임시 dfs로 저장하기.
        if mode == 4:
            new_df = pd.DataFrame(result_ls)
            ## 여기서 갯수 줄이는 명령으로 줄여야함.
            기존개수 = len(new_df)
            
            # pnt 로 소팅. (추가 소팅값 이 있을때 소팅해야만 의미가 있다. 
            # new_df = new_df.sort_values(by = ['pnt',"cd"],ascending= [False,False]) # ascending False 내림차순.
            new_df = new_df.sort_values(by = 'pnt' ,ascending= False) # ascending False 내림차순.

            new_df.to_excel(f"{data_path}pickup_codes_all.xlsx", index= False)
            s
            ## 200 ~250까지만 허용하겠다는 의지.
            if len(new_df) >= 200:
                기준포인트 = new_df.iloc[199]['pnt']
                while True:
                    new_df = new_df[new_df['pnt'] >= 기준포인트 ]
                    if len(new_df) > 250:
                        break
                    else:
                        if 기준포인트 ==0:
                            break
                        기준포인트 -=1
            new_df = new_df[:249]
    
            ##########################################################
            new_df.to_excel(f"{data_path}pickup_codes.xlsx", index= False)
            msg.send_message(f"전체종목:{all_cnt}개, 추적만족종목: {기존개수}, 추적종목저장:{len(new_df)}개")
            try:
                msg.send_file(f"{data_path}pickup_codes.xlsx")
            except:
                pass
            
            #### all_cls.pkl 저장하기.
            all_cls_file_name = '/home/sean/sean/data/all_cls.pkl'
            try:
                with open(all_cls_file_name,'wb') as f:
                    pickle.dump(result_all_cls, f , protocol= pickle.HIGHEST_PROTOCOL)
                msg.send_message(f"mode : {mode} all_cls.pkl 저장 완료.")
            except Exception as e:
                msg.send_message(f"mode : {mode} all_cls.pkl 저장 실패..{e}")
                pass
            
            try:
                ## 저장후 과거추천주 분석하기 위한 데이터 저장해놓기.  ------>> filename 주의  n은 추후에 5로 고정하기.
                Sean_func.anal_recommended_data(all_cls_file_name, n = 5)
                Sean_func.anal_recommended_data(all_cls_file_name, n = 3)
                Sean_func.anal_recommended_data(all_cls_file_name, n = 1)
                
                try:        
                    msg.send_message(f"http://122.34.201.82:8501/")
                except:
                    print('분석서버 전송 실패')
                    
            except Exception as e:
                print(e,'Sean_func.anal_recommended_data 작동안함.')
                pass
            
            
        # else:
        #     try:
        #         q.put('end')
        #         q1.put('end')
                
        #     except:
        #         pass
    # def job2(q2, mode, all_cls ,stocks, new_ohlcv_data = pd.DataFrame()):
    
    def job2(all_cls, mode ,stocks, new_ohlcv_data, test_mode):
        '''
        mode = 1 : 장중 
        mode = 2 : 종가매수 전일추천종목() + 4프로이상종목
        mode = 3 : 시간외매수용.(전일 추천 전체종목 + 4프로이상상승종목)
        mode = 4 : 대기종목추출
        1,2 는 기존데이터로 작업.
        3 ,4은 전체 데이터로 작업.
        '''
        print()
        print('job2 ver.0.2 running...')
        print()
        
        def _send_pic(q):
            # q를 (s객체, msg 객체) 튜플로 받음.
            seconds_ = 3
            while True:
                if q.qsize():
                    try:
                        x = time.time() - last_s_time 
                        if x < seconds_:
                            print(f"{x}초 대기")
                            time.sleep(seconds_-x)
                            print(f"{x}초 대기 완료")
                    except Exception:
                        print('첫타임 오류')
                        pass
                    
                    data = q.get()
                    if type(data) == str:
                        break
                    data[0].send_fig(data[1])
                    last_s_time = time.time()
            
            print(f'{os.getpid()}_thread finished!')
        q1 = queue.Queue()
        if mode != 4:
            t1 = threading.Thread(target=_send_pic, args=(q1,))
            t1.start() # 마지막에 join해줌. 혹시 남아있는 q 있으면 처리하고 끝내야하니까.
        
        msg = Mymsg()
        msg_recommended = Mymsg(chat_name="종목추천채널")
        msg_sean_group = Mymsg(chat_name='sean_group')
        
        data_path = '/home/sean/sean/data/'   ####----------------------------?
        
        ######################################### sended_dic arrange  ##############################
        today = datetime.today().date()
        pickle_file_name = f'{data_path}temp_sended_dic.pkl'
        with open(pickle_file_name,'rb') as f:
            sended_code_dic = pickle.load(f)
        try:
            if sended_code_dic['date'] != today:
                sended_code_dic['date'] = today
                sended_code_dic['sended'] = []
        except:
            sended_code_dic = {}
            sended_code_dic['date'] = today
            sended_code_dic['sended'] = []
        print("=========  sended_code_dic 현황   ==========")
        print(f"{len(sended_code_dic['sended'])}개 중 고유개수 {len(set(sended_code_dic['sended']))}개 ")
        print("==============================================")
        ############################################################################################


        all_dfs = Fnguide.get_ticker_by_fnguide(data_path= data_path,option= 'db')[2:]
        all_dfs['cd'] = all_dfs['cd'].apply(lambda x : x[1:])
                        
        ### all_cls 와 mode 에따라 dfs 생성.
        if mode == 4:
            dfs = Fnguide.get_ticker_by_fnguide(data_path= data_path)[2:]
            dfs['cd'] = dfs['cd'].apply(lambda x : x[1:])
            # dfs = dfs.sample(30)  ## -------------------------------------->>>>>>>>>>>.임시로 지정.
        else:
            temp_ls = []
            for cls in all_cls:
                dic={}
                dic['cd'] = cls.code
                dic['nm'] = cls.code_name
                dic['gb'] = cls.gb
                temp_ls.append(dic)
            dfs = pd.DataFrame(temp_ls)    
        
        print("================  all_cls로 만들어진 dfs 정보  ===================")
        print("===============================================")
        print(f"mode: {mode}   loop cnt (dfs): {len(dfs)}")
        print(dfs.head(5))
        print("===============================================")
        print("===============================================")
        ##########3##########3##########3##########3##########3##########3##########3##########3##########3
        

        result_all_cls = []  ## 전체 cls 목록
        result_ls = [] ## 내일 추적해야하는 종목. 
        recommended_list = []  ## 오늘 추천주 목록.db저장용.

        
        all_cnt = len(dfs)
        print(f'{all_cnt}개 작업 시작.!!!!!!!!!!!!!!!')        
        
        # 2프로이상상승만 추출
        new_ohlcv_data_5 = new_ohlcv_data.loc[new_ohlcv_data['Change'] >= 2]
        
        ##############################  순환시작.  ############################################
        for i, row in dfs.iterrows():
            
            if row['cd'][0] =="A":
                code = row['cd'][1:]
            else:
                code = row['cd']
            code_name = row['nm']
            gb = row['gb']
            print(f"{i+1}/{all_cnt}",code,code_name,end=".")

            ## 메세지 보낸거면 패쓰. ( 단순 배제. 정교하게 할땐 제거햐야함. )
            if mode == 1:
                if code in sended_code_dic['sended']:
                    if len(new_ohlcv_data_5):
                        if not code in list(new_ohlcv_data_5['Code']):  ## 현재 2프로이상 상승한 종목이 아니면.               
                            print('메세지 보낸파일에 있음. pass........')
                            cnt_sended = len([item for item in sended_code_dic['sended'] if item == code])
                            if cnt_sended > 5:  ## 3번 이전까지는 알리기.(기본적으로 10v존.sun돌파, coke 돌파.)
                                continue

            if mode == 4 :
                try:
                    s = Stock(code,code_name,gb,option='fdr') ## 이렇게 하면 멀티로 쓸때 데이터 가져오기 문제됨. mode4는 job1으로.
                    ## 해결방법. - main45.py 에서 mode 4 인경우. process 자체를 하나만 만들어 통째로 넘겨준다.  
                except Exception as e:
                    print(f'mode4 새로운 Stock 객체 생성 실패 : {e}')
                    continue
            else: # mode1,2,3
                try:
                    x = [cls for cls in all_cls if cls.code == code]
                    if len(x):
                        s = x[0]
                        s._refresh_ohlcv_with_all_current_price(new_ohlcv_data)
                        print('all_cls객체 refresh_ohlcv_with_all_current_price 성공.!!')
                        s.anal_stock()
                        print('cls객체 분석 성공.')
                        print(f"ppid: {os.getppid()}, pid:{os.getpid()}")
                    else:
                        print(f'{code_name} {code} pass....................................')
                        continue
                except Exception as e:
                    print(f"all_cls에서 객체 가져오기 실패 또는 업대이트 실패.{e} type(s) = {type(s)}")
                    continue
            
            s.mode = mode   ## 객체만들어진 시기에 아예 mode 입력하기.
            #### insert reasonable ###########################################
            ## 최소.short_w 대기포함.
            
            ## min_coke이면 coke upper값
            if s.code in stocks.best_invest_codes:
                s.points_dic['추적종목이유'].append("best_invest")
                s.points_dic['point'] +=1
            if s.code in stocks.consen_up_codes:
                s.points_dic['추적종목이유'].append(f"consen_up")
                s.points_dic['point'] +=1
            if s.is_신규상장주():  
                s.points_dic['추적종목이유'].append("신규상장주")
                s.points_dic['point'] +=1
            
            # # s.points_dic['point'] 에 넣기. 또는 확인하면서 점수 추가하기. 
            # ###################################################################
            
            buy_time_30 = s.is_buy_time_by30()
            if mode == 4:
                # best_invest, consen_up
                if s.points_dic['추적종목여부']: ## 추적종목 df 만들기.
                    result_dic = {}
                    result_dic['cd'] = code
                    result_dic['nm'] = code_name
                    result_dic['gb'] = gb
                    result_dic['pnt'] = s.points_dic['point']
                    result_dic['추적종목이유'] = s.points_dic['추적종목이유']
                    ## 대기종목저장용 추가.
                    ### 30분봉상태값을 저장하기 위함. 다음날 돌파찾기에 사용.
                    try:
                        result_dic['coke_result'] = s.info_min30_coke['result']
                        result_dic['upper_current_value'] = s.info_min30_coke['upper_current_value']
                        result_dic['upper_current_status'] = s.info_min30_coke['upper_current_status']
                        result_dic['coke_width'] = s.info_min30_coke['coke_width']
                    except:
                        result_dic['coke_result'] = False
                        result_dic['upper_current_value'] = 0
                        result_dic['upper_current_status'] = 0
                        result_dic['coke_width'] = 0
                    try:
                        result_dic['sun_result'] = s.info_min30_sun['result']
                        result_dic['새출현재너비'] = s.info_min30_sun['새출현재너비']
                        result_dic['새출현재상단값'] = s.info_min30_sun['현재상단값']
                    except:
                        result_dic['sun_result'] = False
                        result_dic['새출현재너비'] = 0
                        result_dic['새출현재상단값'] = 0
                        
                    ### 다음날 추적할 종목들 뽑아내기.
                    result_ls.append(result_dic)  
                   
                
                
            ###### 매수타임 알림.################################################ 
            # 장중 : 3_20gcv,sun_gcv, coke_gcv,
            # 장마감시: 3_20gcv,sun_gcv, coke_gcv, abc_vol만족,  sun_up, coke_up
            if s.points_dic['buytime']:   ### ???  if points_dic['buytime'] 으로 해야하나.>?
                if mode == 1: ## 장중.
                    ## 장중에 30분봉확인후 메세지 보내기 
                    if buy_time_30: ## 30분봉 True 나오면.
                        cnt = len([item for item in sended_code_dic['sended'] if item == s.code])
                        s.called_cnt = cnt
                        sended_code_dic['sended'].append(s.code)
                        
                        ## 특별히 알림할것 아래와 같이 추가할수 있다. 
                        
                        
                        #################### words 일봉관련   ################################
                        words = ["coke_돌파_대량_**","coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","*30분sun돌파","*30분코크돌파","abc_거래만족후상승_*"]
                        # words = ["코크_돌파_30분_***","sun_돌파_30분_***"]
                        finded_word = [word for word in words if word in s.points_dic['reasons']]
                        if len(finded_word):
                            if cnt < 8:
                                q1.put((s,msg))
                            
                        #################### words 30분봉관련   ################################
                        words_30 = ["코크_돌파_30분_***","sun_돌파_30분_***"] ## --> 30분봉.
                        finded_word_30 = [word for word in words_30 if word in s.points_dic['reasons']]
                        if len(finded_word_30):
                            q1.put((s,msg_recommended))
                            try:
                                last_t = s.df_min30.index[-1]
                                msg_sean_group.send_message(f"{last_t.hour}시{last_t.minute}분\n{s.code_name} 30분봉 돌파상황확인\n{'|'.join(s.points_dic['reasons'])}")    ###????????????
                            except:
                                print("30분돌파메세지 실패")
                            
                        ##########################################################################
                        result_all_cls.append(s)
                        continue
                        
                elif (mode == 2) | (mode == 3):  ## 장마감쯤. # 시간외용포함
                    if mode == 2:
                        result_all_cls.append(s)
                    if buy_time_30:
                        sended_code_dic['sended'].append(s.code)
                        cnt = len([item for item in sended_code_dic['sended'] if item == s.code])
                        s.called_cnt = cnt
                        
                        # s.send_fig(msg)       #########???????????????????????????????????????
                        
                        #################### words 일봉관련   ################################
                        words = ["coke_돌파_대량_**","coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","abc_거래만족후상승_*","abc_거래량만족"]
                        finded_word = [word for word in words if word in s.points_dic['reasons']]
                        if len(finded_word):
                            if cnt < 8:
                                q1.put((s,msg_recommended))
                                # s.send_fig(msg_recommended)    ####????????????????????????????????????????
                                pass
                        
                        words_30 = ["코크_돌파_30분_***","sun_돌파_30분_***"] ## --> 30분봉.
                        finded_word_30 = [word for word in words_30 if word in s.points_dic['reasons']]
                        if len(finded_word_30):
                            try:
                                last_t = s.df_min30.index[-1]
                                msg_sean_group.send_message(f"{last_t.hour}시{last_t.minute}분\n{s.code_name} 30분봉 돌파상황확인\n{'|'.join(s.points_dic['reasons'])}")    ###????????????
                                pass
                            except:
                                print("30분돌파메세지 실패")
                else:
                    pass
                
            #################### words 현재종가기준 추천주.   ################################
            if (mode == 3) | (mode == 4):            
                words = ["coke_돌파_대량_**","coke_240_돌파_*" ,"coke_60_돌파_*","sun_돌파_*","3w_20w_ac_*","coke_ac_gcv_*","sun_돌파_gcv_*","*30분sun돌파","*30분코크돌파","abc_거래만족후상승_*","abc_거래량만족"]
                finded_word = [word for word in words if word in s.points_dic['reasons']]
                if len(finded_word):
                    recommended_dic = {}
                    recommended_dic['추천날짜'] = s.df.index[-1]
                    recommended_dic['code'] = s.code
                    recommended_dic['code_name'] = s.code_name
                    recommended_dic['당시point'] = s.points_dic['point']
                    recommended_dic['추천일등락율'] = s.df['Close'].pct_change()[-1] *100 
                    recommended_dic['추천기법'] = '|'.join(finded_word)
                    recommended_dic['추적이유'] = '|'.join(s.points_dic['추적종목이유'])
                    recommended_dic['매수사유'] = '|'.join(s.points_dic['reasons'])
                    
                    recommended_list.append(recommended_dic)
             
                    #######################################################################      
                result_all_cls.append(s)       ## 3, 4 저장함. 3은 모드3으로 저장될것임. 
           
        ## 반복문 모두 끝남. 
        result_data = (recommended_list, sended_code_dic, result_all_cls, result_ls)
        
        print(f"recommended_list:{len(recommended_list)} ,result_all_cls:{len(result_all_cls)}, result_ls:{len(result_ls)}")
        ## 데이터 q로 전송후 받은 데이터 main.py 에서 처리.
        
        if mode != 4:
            q1.put('end')
            t1.join()
        print(f'{os.getpid()} process finished!!')
        
        return  result_data

        
class Stock(Ant_tech):
    
    def __init__(self,code, code_name = 'no_info',gb = 'no_info',option ='db',data_path='/home/sean/sean/data', n=0, exchange = "KS" ):
        '''
        option = 'db','fdr'
        '''

        # self.cnt_sended = 0
        self.mode = 0
        self.called_cnt = 0
        self.n = n
        self.code = code 
        self.code_name = code_name
        self.gb = gb
        if data_path[-1] != "/":
            data_path = data_path + "/"
        self.data_path = data_path 
        self.exchange = exchange
        
        self.points_dic = {}
        self.points_dic['point']= 0 
        self.points_dic['추적종목여부']= False
        self.points_dic['추적종목이유'] = []
        self.points_dic['buytime']= False
        self.points_dic['buytime_30'] = False
        self.points_dic['type'] = []
        self.points_dic['reasons']=[]
        self.points_dic['손절가'] = -1
        self.points_dic['손절폭'] = -1
                # self.sorting_point = 0
        
        # 기술분석 
        if self.exchange =="KS":
            self._refresh_ohlcv(by=option)
            
            # 구간 수급비율현황.
            try:
                self.info_buy_rate= self._buy_rate_status()
            except:
                self.info_buy_rate = None
            
            self._get_finance_table_from_db()
            
            self._arrange_data_재무()
            self._arrange_basic_info()
            self._arrange_issue_info()
        else:
            self._refresh_ohlcv()
        
        # self.find_stock_any_anal() ## 구버전
        self.anal_stock()
        
        
        '''
        유보율, 부채비율, 실적현황. 수급현황, 수급비율현황, 이슈현황.
        '''
    
    
    ## check하기 함수 만들기. ex) 실적주냐., 수급만족이냐. ab파동이냐. 매수타임이냐.
    def cal_수익율(self,n=5):
        '''
        n = int(days)
        
        활용: 
        1, n일간 수익율 최고수익율 너무 큰거는 배제하기. 
        2. 기법에 따라 n일간 최고수익율 체크(수익율통계)
        '''
        try:
            result_dic = {}
            temp_df = self.df.iloc[-(n+1):]  ## check 하기. 
            
            buy_value = temp_df.iloc[0]['Close']
            수익율s = ((temp_df['Close']/buy_value)-1 ) * 100
            high수익율s = ((temp_df['High']/buy_value)-1 ) * 100
            
            
            result_dic['최고수익율'] = max(high수익율s[1:])
            result_dic['최저수익율'] = min(수익율s)
            result_dic['현재수익율'] = 수익율s.iloc[-1]
        except:
            result_dic['최고수익율'] = 0
            result_dic['최저수익율'] = 0
            result_dic['현재수익율'] = 0 
        return result_dic

    
    
    
    def get_news(self):
        file_name = "/home/sean/sean/data/stockplus_news.db"
        conn = sqlite3.connect(file_name)
        table_name ="news"
        sql = f"select * from '{table_name}' where title like '%{self.code_name}%'"

        # sql = f"select * from '{table_name}'"
        data = pd.read_sql(sql,conn)
        data['createdAt'] = pd.to_datetime(data['createdAt'])
        data['Date'] = data['createdAt'].dt.date
        self.news_df = data
        return data
        
    
    
    def is_good_consen(self):
        '''
        올해 0.5이 넘고 다음해 0.5가 넘는종목이나.
        올해관계없이 다음해 턴어라운드나 1이 넘는것들 
        올해도 0.5 이상 내년도 0.5이상
        '''
        try:
            check_year1 = Sean_func.실적기준구하기()[1][0]
            check_year2 = Sean_func.실적기준구하기()[1][1]

            value1 = self.실적_y.loc[check_year1,'status']
            value2 = self.실적_y.loc[check_year2,'status']

            value1_cond = 0
            if type(value1) == str:
                if value1 == '턴어라운드':
                    value1_cond = -1
            if type(value1) == float:
                if value1 > 0:
                    value1_cond = value1

            value2_cond = 0
            if type(value2) == str:
                if value2 == '턴어라운드':
                    value2_cond = -1
            if type(value2) == float:
                if value2 >0:
                    value2_cond = value2

            cond1 = (value1_cond >= 0.5) & (value2_cond >=0.5)
            cond2 = (value2_cond == -1) | (value2_cond >= 0.5)
            cond = cond1 | cond2
            # print(cond, cond1, cond2)
            return cond
        except Exception as e:
            # print(e)
            return False
    
    
    def is_disparity_cond(self,min_disparity = 100, max_disparity = 101.1):
        '''
        20 60 120 240 과 이격만족하는거 찾을때 사용. ma3 기준!
        어느하나라도 만족하면 True 반환. 
        '''
        cnt = len(self.df)
        if cnt <20:
            return False
        arr = np.array([5,20,60,120,240])
        exist_mas = arr[arr<=cnt]
        mas = [f"ma{ma}" for ma in exist_mas]

        result_ls = []
        for ma in mas:
            ma_info = eval(f"self.info_{ma}")
            ma_up_cond = ma_info['현재방향'] =='상승'
            
            disparity = self.df.iloc[-1]['ma3']  / self.df.iloc[-1][ma]  *100
            # disparity = self.df.iloc[-1]['Close']  / self.df.iloc[-1][ma]  *100
            
            #ma 이격   ##현재가는 이격보다 큰거만 봐야함. (w독사에 이미 포함.)
            disparity_cond = min_disparity <= disparity <= max_disparity
            
            close_loc_cond = ma_info['가격이격도'] > 99
            
            ## 캔들저점이격.
            disparity_low_cond = min_disparity <= ma_info['현재저점이격도'] <= max_disparity
            # 해당이평 방향 추가하기. 상승일때만 취급하하기.
            
            
            temp_cond = ma_up_cond & ((disparity_cond & close_loc_cond) | disparity_low_cond)
            result_ls.append(temp_cond)
            
            # print(disparity)
            
            if temp_cond:
                print(ma,"이평상태","이격도:",disparity,ma_up_cond, "이평이격",disparity_cond,"캔들저점이격",disparity_low_cond )


        # if any(result_ls):
        #     print("이격도:",disparity)
        return any(result_ls)
    
    #######################  기술분석 타이밍 관련  ######################
    
    def find_대기종목(self):
        '''
        상태로만 체크하기. w_대기 , w, today ac 
        sun, coke. 20w대기 w, abc상태, short_w  상태. 전체종목 추가후, 대기종목 줄이기. (점수.? )
        '''
        result = False
        
        cond_array = (not self.is_역배열_status()) & (self._장기추세_status()) | self.is_정배열_status() 
        cond_20 = len(self.info_ma20['result_type_ls']) > 0
        cond3_w = len(self.info_ma3['result_type_ls'] ) > 0
        cond5_w = len(self.info_ma5['result_type_ls'] ) > 0 
        current_price = self.df.Close[-1]
        cond_price = 3000 < current_price < 100000
        
        cond_coke = self.info_coke['result']
        cond_sun = self.info_sun['result']
        
        
        if cond_price & cond_array & cond_20 & (cond3_w | cond5_w) & (cond_coke | cond_sun):
            self.set_array_pnt()
            result = True
        
        return result
    
    
    ###################### new_simple_function.########################
    # points_dic 
    # 'point': int 점수
    # '추적종목여부': bool , 
    # '추적종목이유': list : ['3_20_gcv', 'ac_sun1'], > 
    # 'buytime': False, boolean
    # 'type': [1, 2], > 사용되는 mode _type 입력 > 장중 시간외. 마감후.. 
    # 'reasons': ['3_20_gcv', 'ac_sun1']} > 매수사유 > 공시, 뉴스, 실적, 기술적분석 등.
    # self.points_dic[''] = value
    #######개별함수로 만들기. 테스트용도로 만든후 어떻게 활용할지 고민.

  
    def anal_stock(self,):
        '''
        
        '''
        self.points_dic = {}
        self.points_dic['point']= 0 
        self.points_dic['추적종목여부']= False
        self.points_dic['추적종목이유'] = []
        self.points_dic['buytime']= False
        self.points_dic['buytime_30'] = False
        self.points_dic['type'] = []
        self.points_dic['reasons']=[]
        self.points_dic['손절가'] = -1
        self.points_dic['손절폭'] = -1
        
        
        # 일단 reasons 는 넣자. 그러고 대기목록인지 판별하기. 여기서 포인트 삽입하기. 
        
        ## 실적주 +1
        if self.is_good_consen():
            self.points_dic['point'] +=1
            
        ## 유보율 +1
        if '유보율' in dir(self):
            if self.유보율 > 500:
                self.points_dic['point'] +=1
            if '부채비율' in dir(self):
            # 부채비율 감점
                if (self.유보율 < 1000) & (self.부채비율 >60):
                    self.points_dic['point'] -=1
                
        
        ## ab거래량 +1
        try:
            if self.get_ab_v_rate() >= 2:
                self.points_dic['point'] +=1
                self.points_dic['추적종목이유'].append("ab거래량")
        except:
            pass
        
        ## ab파동 +1
        try:
            if 'info_ma20' in dir(self):
                if self.info_ma20['ab기간파동']:
                    self.points_dic['추적종목이유'].append("ab기간파동")
                    self.points_dic['point'] +=1
        except:
            pass
        ## 수급. +1 -- 이건 메인에서 넣을까.?
        # Stocks = Stocks()
        
        ## 의미있는 수급 +1 정의하기가 애매함. -->  구간별데이터의 매집비가 기준비율보다 큰값이 반이상이면 조건만족.
        if "info_buy_rate" in dir(self):
            기준비율 = 105
            try:
                cnt = len(self.info_buy_rate['구간별'])
                if cnt >= 2:
                    cond_list = [dic['주도기관'] for dic in self.info_buy_rate['구간별'] if  dic['매집비'] >= 기준비율]
                    over_cnt = len(cond_list)
                    if over_cnt / cnt >=0.49: 
                        주도기관 = list(set([item for items in cond_list for item in items]))
                        self.points_dic['추적종목이유'].append(f'매집비_{"_".join(주도기관)}')
                        self.points_dic['point'] +=1
            except:
                pass
            
        
        
        cond_3w20w = self.is_3_20()
        self.cond_sun_돌파 = self.is_3_sun()
        self.cond_coke_돌파 = self.is_3_coke()
        self.cond_abc = self.is_abc()
        
        
        
        
        ####### 여기서 추적종목 제외하기 
        ## 상투거래량 존재하는것 거르기.
        상투 = self.is_상투거래량()  
        if 상투['result']:
            self.points_dic['추적종목여부'] = False
            self.points_dic['추적종목이유'].append("상투거래량")
        ## 완전역배열 제외.?
        
        
        ## 동전주 제외.?
        if self.df.iloc[-1]['Close'] < 1500:
            self.points_dic['추적종목여부'] = False
            self.points_dic['추적종목이유'].append("동전주")
            
        ## points_dic 정리. 혹시 중복이 있으면 제거.
        self.points_dic['type'] = list(set(self.points_dic['type']))
        self.points_dic['추적종목이유'] = list(set(self.points_dic['추적종목이유']))
        self.points_dic['reasons'] = list(set(self.points_dic['reasons']))

        ## sorting
        self.points_dic['추적종목이유'] = sorted(self.points_dic['추적종목이유'])
        self.points_dic['reasons'] = sorted(self.points_dic['reasons'])
        self.points_dic['type'] = sorted(self.points_dic['type'])
        
        
        # reasons 목록 ####3
        # @거래 ,@@거래, 
        # 2060gcv, 60w, 3_20_gcv완성, 3_20_gcv_ac,3_20_gcv_ac_20up,3_20_gcv_ac_2060up,3w20w완성,3w20w완성_ac,
        # sun_gcv완성 ,sun_gcv_ac, sun돌파,coke_gcv완성,coke_gcv_ac,coke돌파,coke돌파대량,abc매수가능
        
        
    
    def is_w_contains(self):
        '''
        단순히 3일선 w완성, 대기 체크. 1차 거르기용. --> 심각한 배열 제외 필요.
        '''
        result = False
        result_type_ls = self.info_ma3['result_type_ls']
        w_list =  [item for item in result_type_ls if "w" in item ]  ## w 포함 종목
        if len(w_list):
            result = True
        return result
    
    def is_3_20(self):
        '''
        이평선 파동에 관한 사항들.  개별함수에도 점수 넣기. ------> 정리 필요. 20 60 w 3w 인경우
        '''
        result = False
        start_day = self.info_ma3['start_day']
        end_day = self.info_ma3['end_day']
        if (start_day =="") | (end_day ==""):
            return False
        
            ## ac 존재여부
        if "info_ac" in dir(self):
            ac_df0 = self.info_ac[0]
            if len(ac_df0):
                temp_df0 = ac_df0.loc[start_day : end_day ]
                if len(temp_df0 ):
                    self.points_dic['reasons'].append("@거래")
                    self.points_dic['point'] +=1
                    # print('추적이유 : @거래')
                    ##만약 현재거래량이 평균거래량보다 작다고 단봉이면. "@거래_거래량만족"
                    
            ## bic_ac 존재여부
            ac_df1 = self.info_ac[1]
            if len(ac_df1):
                temp_df1 = ac_df1.loc[start_day : end_day ]
                if len(temp_df1 ):
                    self.points_dic['reasons'].remove("@거래")
                    self.points_dic['reasons'].append("@@거래")
                    self.points_dic['point'] +=1
                    # print('추적이유 : @@거래')
                    ##만약 현재거래량이 평균거래량보다 작다고 단봉이면. "@거래_거래량만족"
                    
                    
        ## 20 gcv 존재여부
        if "info_ma20" in dir(self):
            result_type_ls20 = self.info_ma20['result_type_ls']
            ## 2060 gcv 존재여부
            cond_2060gcv = ('ma20', 'ma60') in self.info_ma20['alphabeta_gc_ls']
        else:
            result_type_ls20 = []
            cond_2060gcv = False
            
        if "info_ma60" in dir(self):
            result_type_ls60 = self.info_ma60['result_type_ls']
        else:
            result_type_ls60 = []
            
        result_type_ls3 = self.info_ma3['result_type_ls']
        
        ## 60 w 여부
        w60_list =  [item for item in result_type_ls60 if "w" in item ]  ## w 포함 종목
        w20_list =  [item for item in result_type_ls20 if "w" in item ]  ## w 포함 종목
        
        if len(w20_list):       ##  이조건 안맞으면 아래는 다 소용없음. 수정필요.
            if cond_2060gcv:
                # print('추적이유 : 2060gcv')
                self.points_dic['추적종목이유'].append("2060_gcv")
                self.points_dic['reasons'].append("2060_gcv")
                self.points_dic['point'] += 1
                
            if len(w60_list):
                # print('추적이유 : 60w')
                self.points_dic['reasons'].append("60_w")
                self.points_dic['point'] += 1
            
            ### 3_20 gcv 만 확인하는것.    
            temp_df = self.info_ma3_ma20.loc[start_day : end_day]
            if len(temp_df):
                if temp_df.iloc[-1]['cross_status'] == 'gc':  ## 3_20 gcv 이면. 
                    
                    # w완성이면
                    w3완성_list =  [item for item in result_type_ls3 if "w완성" in item ]
                    if len(w3완성_list):   
                        # if self.info_ma3['변곡지속일'] ##### >>>> 너무 상승한거 제외하기. 필요.
                        self.points_dic['point'] += 1
                        self.points_dic['buytime'] = True # 
                        self.points_dic['reasons'].append("320_gcv")
                        self.points_dic['type'].append(1)
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("320_gcv")
                        
                        # print('3_20_gcv완성')
                        
                    # w 대기이면.
                    w3대기_list =  [item for item in result_type_ls3 if ("w대기" in item) | ("w독사" in item) ]
                    if len(w3대기_list):
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("320_gcv_대기")
                        # print('3_20_gcv대기') ## 30분 10 v필요.
                        
                    # today_ac 이면
                    if ac_df0.index[-1] == self.df.index[-1]:
                        self.points_dic['point'] += 1
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("*320_gcv_ac_*")
                        self.points_dic['type'].append(1)
                        self.points_dic['type'].append(2)
                        self.points_dic['type'].append(3)
                        self.points_dic['buytime'] = True # 
                        self.points_dic['reasons'].append("*320_gcv_ac_*")
                        # print('3_20_gcv_ac')
                        # pass
                        
                        try:
                            cond_candle_up20 = self.df.iloc[-1]['Low'] < self.df.iloc[-1]['ma20'] < self.df.iloc[-1]['Close']
                        except:
                            cond_candle_up20 = False
                        try:
                            cond_candle_up60 = self.df.iloc[-1]['Low'] < self.df.iloc[-1]['ma60'] < self.df.iloc[-1]['Close']
                        except:
                            cond_candle_up60 = False
                            
                        # 이 캔들이 20선 또는 60선 돌파라면.
                        if cond_candle_up20 | cond_candle_up60:
                            self.points_dic['point'] += 1
                            self.points_dic['buytime'] = True
                            self.points_dic['reasons'].append("320_gcv_ac_2060_up")
                            self.points_dic['type'].append(1)
                            self.points_dic['type'].append(2)
                            self.points_dic['type'].append(3)
                            self.points_dic['추적종목여부'] = True
                            self.points_dic['추적종목이유'].append("320_gcv_ac_2060_up")
                            # print('3_20_gcv_ac_20up')
                            pass   
            
            w20_완성 = len([item for item in result_type_ls20 if "w완성" in item ])>0
            if w20_완성:
                # w완성이면
                w3완성_list =  [item for item in result_type_ls3 if "w완성" in item ]
                if len(w3완성_list):   
                    # if self.info_ma3['변곡지속일'] ##### >>>> 너무 상승한거 제외하기. 필요.
                    # self.points_dic['point'] +=1
                    self.points_dic['buytime'] = True # >> 30분봉 10선과 이격도 활용해서 
                    self.points_dic['reasons'].append("3w_20w")
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("3w_20w")
                    
                    # print('3_20_gcv완성')  
                    
                # w 대기이면.
                w3대기_list =  [item for item in result_type_ls3 if ("w대기" in item) | ("w독사" in item) ]
                if len(w3대기_list):
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("3w_20w")
                    # print('3_20_gcv대기') ## 30분 10 v필요.
                    
                # today_ac 이면
                if ac_df0.index[-1] == self.df.index[-1]:
                    self.points_dic['point'] +=1
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("3w_20w_ac_*")
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['buytime'] = True # 
                    self.points_dic['reasons'].append("3w_20w_ac_*")
                    # print('3_20_gcv_ac')
                    # pass
                    result = True
                    
            
        return result
    
    def is_3_sun(self):
        result = False
        start_day = self.info_ma3['start_day']
        end_day = self.info_ma3['end_day']
        if (start_day =="") | (end_day ==""):
            return False            
        
        ac_df0 = self.info_ac[0]
        ac_df1 = self.info_ac[1]
        result_type_ls3 = self.info_ma3['result_type_ls']
        ## sun상태이고 # sun 사이에 gc 가 있는지 확인. 

        temp_index = ac_df0.loc[self.info_ma3['start_day']:self.info_ma3['end_day']].index
        temp_df  = self.df.loc[temp_index]
        cond1= temp_df['Open'] < temp_df['sun_max'] 
        cond2 = temp_df['sun_max'] < temp_df['Close']
        ac_cnt_between_start_end = sum(cond1 & cond2)
        

        
        
        if self.info_sun['result']:
            cross_df = self.info_ma3_sun_max
            if len(cross_df):
                cross_df = self.info_ma3_sun_max.loc[start_day : end_day]
                if len(cross_df):
                    if cross_df.iloc[-1]['cross_status'] == 'gc':
                        self.points_dic['point'] +=1
                        
                    
                        # w완성이면
                        w3완성_list =  [item for item in result_type_ls3 if "w완성" in item ]
                        if len(w3완성_list): #>> 변곡지속일 체크하기.
                            # self.points_dic['point'] +=1
                            self.points_dic['buytime'] = True # >> 30분봉 10선과 이격도 활용해서 
                            self.points_dic['reasons'].append("sun_gcv")
                            self.points_dic['type'].append(1)
                            self.points_dic['type'].append(2)
                            self.points_dic['type'].append(3)
                            self.points_dic['추적종목여부'] = True
                            self.points_dic['추적종목이유'].append("sun_gcv")
                            # print('sun_gcv완성')
                            if ac_cnt_between_start_end > 0:
                                self.points_dic['reasons'].append("sun_돌파_gcv_*")
        
                            pass

                        # w 대기이면.
                        w3대기_list =  [item for item in result_type_ls3 if ("w대기" in item) | ("w독사" in item) ]
                        if len(w3대기_list):
                            # self.points_dic['point'] +=1
                            # self.points_dic['buytime'] = False # >> 30분봉 10선과 이격도 활용해서 
                            # self.points_dic['reasons'].append("sun_gcv_대기")
                            # self.points_dic['type'].append(0)
                            self.points_dic['추적종목여부'] = True
                            self.points_dic['추적종목이유'].append("sun_gcv_대기")
                            # print('sun_gcv대기')
                            pass

                        # today_ac 이면
                        if ac_df0.index[-1] == self.df.index[-1]:
                            # self.points_dic['point'] +=1
                            self.points_dic['buytime'] = True # >> 30분봉 10선과 이격도 활용해서 
                            self.points_dic['reasons'].append("sun_gcv_ac")
                            self.points_dic['추적종목여부'] = True
                            self.points_dic['type'].append(1)
                            self.points_dic['type'].append(2)
                            self.points_dic['type'].append(3)
                            self.points_dic['추적종목이유'].append("sun_gcv_ac")
                            # print('sun_gcv_ac')
                            if ac_cnt_between_start_end > 0:
                                self.points_dic['reasons'].append("sun_돌파_gcv_*")
            
            
            # today_ac 이면
            if ac_df0.index[-1] == self.df.index[-1]:
                ## 돌파인지 확인하기.  Low 냐 Open이냐.
                try:
                    cond_candle_up_sun = self.df.iloc[-1]['Open'] < self.df.iloc[-1]['sun_max'] < self.df.iloc[-1]['Close']
                except:
                    cond_candle_up_sun = False
                
                candle_rate_dic = Anal_tech.cal_candle_rate(self.df) 
                cond_candle = candle_rate_dic['몸통비율'] > 50
                # 이 캔들이 sun 돌파라면.
                if cond_candle_up_sun & cond_candle:
                    # print('sun 돌파') ## 캔들정의해야하나.??? # 몸통 50 이상.
                    self.points_dic['point'] +=1
                    self.points_dic['buytime'] = True # >> 30분봉 10선과 이격도 활용해서 
                    self.points_dic['reasons'].append("sun_돌파_*")
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['추적종목이유'].append("sun_돌파_*")
                    result = True
                    
        return result
    
    def is_3_coke(self):
        
        result = False
        start_day = self.info_ma3['start_day']
        end_day = self.info_ma3['end_day']
        if (start_day =="") | (end_day ==""):
            return False            
        
        ac_df0 = self.info_ac[0]
        ac_df1 = self.info_ac[1]
        result_type_ls3 = self.info_ma3['result_type_ls']

        temp_index = ac_df0.loc[self.info_ma3['start_day']:self.info_ma3['end_day']].index
        temp_df  = self.df.loc[temp_index]
        cond1= temp_df['Open'] < temp_df['upper_bb'] 
        cond2 = temp_df['upper_bb'] < temp_df['Close']
        ac_cnt_between_start_end = sum(cond1 & cond2)
        
        coke_width_start_day = self.df.loc[start_day,"coke_width"]

        ## coke상태이고 # coke 사이에 gc 가 있는지 확인. 
        # if self.info_coke['result']:
        cross_df = self.info_ma3_upper_bb
        if len(cross_df):
            cross_df = cross_df.loc[start_day : end_day]
            if len(cross_df):
                if cross_df.iloc[-1]['cross_status'] == 'gc':
                    
                    # w완성이면
                    w3완성_list =  [item for item in result_type_ls3 if "w완성" in item ]
                    if len(w3완성_list):
                        # print('coke_gcv완성')
                        # self.points_dic['point'] +=1
                        self.points_dic['buytime'] = True
                        self.points_dic['reasons'].append("coke_gcv")
                        self.points_dic['type'].append(1)
                        self.points_dic['type'].append(2)
                        self.points_dic['type'].append(3)
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("coke_gcv")

                        if ac_cnt_between_start_end > 0:
                            self.points_dic['reasons'].append("coke_ac_gcv_*")
                        if coke_width_start_day < 55:
                            self.points_dic['reasons'].append(f"coke_width_{coke_width_start_day:.1f}")
                    # w 대기이면.
                    w3대기_list =  [item for item in result_type_ls3 if ("w대기" in item) | ("w독사" in item) ]
                    if len(w3대기_list):
                        # print('coke_gcv대기')
                        # self.points_dic['point'] +=1
                        # self.points_dic['buytime'] = True
                        # self.points_dic['reasons'].append("coke_gcv_대기")
                        # self.points_dic['type'].append(1)
                        # self.points_dic['type'].append(2)
                        # self.points_dic['type'].append(3)
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("coke_gcv_대기")

                    # today_ac 이면
                    if ac_df0.index[-1] == self.df.index[-1]:
                        print('coke_gcv_ac')
                        # self.points_dic['point'] +=1
                        self.points_dic['buytime'] = True
                        self.points_dic['reasons'].append("coke_gcv_ac")
                        self.points_dic['type'].append(1)
                        self.points_dic['type'].append(2)
                        self.points_dic['type'].append(3)
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("coke_gcv_ac")
                        if ac_cnt_between_start_end > 0:
                            self.points_dic['reasons'].append("coke_ac_gcv_*")
                        if coke_width_start_day < 55:
                            self.points_dic['reasons'].append(f"coke_width_{coke_width_start_day:.1f}")
        # today_ac 이면
        if ac_df0.index[-1] == self.df.index[-1]:
            ## coke 돌파인지 확인하기. 
            try:
                cond_candle_up_coke = self.df.iloc[-1]['Low'] < self.df.iloc[-1]['upper_bb'] < self.df.iloc[-1]['Close']
            except:
                cond_candle_up_coke = False
            
            
            
            try:
                cond_60_in_240_1 = (self.df.iloc[-1]['upper_bb'] > self.df.iloc[-1]['upper_bb_60'])
                cond_60_in_240_2 = (self.df.iloc[-1]['lower_bb'] < self.df.iloc[-1]['lower_bb_60'])
                cond_60_in_240 = cond_60_in_240_1 & cond_60_in_240_2
                cond_candle_up_coke60 = self.df.iloc[-1]['Low'] < self.df.iloc[-1]['upper_bb_60'] < self.df.iloc[-1]['Close']
            except:
                cond_candle_up_coke60 = False
                cond_60_in_240 = False
            
            
            candle_rate_dic = Anal_tech.cal_candle_rate(self.df) 
            cond_candle = candle_rate_dic['몸통비율'] > 50
            
            
            # 이 캔들이 sun 돌파라면.
            if (cond_candle_up_coke | (cond_candle_up_coke60 & cond_60_in_240 )) & cond_candle:
                print('coke 돌파') ## 캔들정의해야하나.???
                if cond_candle_up_coke:
                    # self.points_dic['point'] +=1
                    self.points_dic['buytime'] = True
                    self.points_dic['reasons'].append("coke_240_돌파_*")
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("coke_240_돌파_*")
                    if coke_width_start_day < 55:
                        self.points_dic['reasons'].append(f"coke_width_{coke_width_start_day:.1f}")
                    result = True
                
                if (cond_candle_up_coke60 & cond_60_in_240 ):
                    # self.points_dic['point'] +=1
                    self.points_dic['buytime'] = True
                    self.points_dic['reasons'].append("coke_60_돌파_*")
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("coke_60_돌파_*")
                    if coke_width_start_day < 55:
                        self.points_dic['reasons'].append(f"coke_width_{coke_width_start_day:.1f}")
                    result = True
                
                
                
                if len(ac_df1):
                    if ac_df1.index[-1] == self.df.index[-1]:
                        print('coke 돌파 대량')
                        
                        # self.points_dic['point'] +=1
                        self.points_dic['buytime'] = True
                        self.points_dic['reasons'].append("coke_돌파_대량_**")
                        self.points_dic['type'].append(1)
                        self.points_dic['type'].append(2)
                        self.points_dic['type'].append(3)
                        self.points_dic['추적종목여부'] = True
                        self.points_dic['추적종목이유'].append("coke_돌파_대량_**")
                        if coke_width_start_day < 55:
                            self.points_dic['reasons'].append(f"coke_width_{coke_width_start_day:.1f}")
                        result = True
        return result  
  
    def is_abc(self):
        result = False
        start_day = self.info_ma3['start_day']
        if start_day == "":
            # print('Start_day False')
            return False

        if "info_ac" not in dir(self):
            # print('info_ac False')
            return False
        
        # 거래량 있는지 확인. 
        big_ac_df = self.info_ac[1]
        
        if len(big_ac_df)==0:
            # print('대량거래없음.')
            return False
            
        if self.df.index[-1] == big_ac_df.index[-1]:
            # print('오늘거래터짐.')
            return False
        
        big_ac_df = big_ac_df.loc[start_day:]
        
        if len(big_ac_df) == 0:
            pass
            # print('대량거래 데이터 없음')
        
        if len(big_ac_df):
            first_big_ac_date = big_ac_df.index[0] # 여러거래량일시 필요.
            last_big_ac_date = big_ac_df.index[-1]
            current_value = self.df.iloc[-1]['Close']

            대량거래이전평균거래량 = self.df.loc[:first_big_ac_date,'vol20ma'][-2]
            abc_b_value = big_ac_df.iloc[-1]['abc_b']
            abc_a_value = big_ac_df.iloc[-1]['abc_a']
            기준거래량 = max( big_ac_df.iloc[-1]['Volume'] /10 , 대량거래이전평균거래량)

            # 기준거래량일부터 데이터중. 기준거래량보다 작은게 1개이고 
            temp_df = self.df.loc[last_big_ac_date:] ## 대량거래이후 데이터.(대량거래일 포함. )
            abc_b_value = big_ac_df.iloc[-1]['abc_b']
            
            cond_data_cnt = len(temp_df) >=2
            cond_short_term_big_v = temp_df.iloc[1]['Volume'] <= 기준거래량 
            basic_cond = cond_data_cnt & cond_short_term_big_v
            
    #         if basic_cond:
    #             print('기준거래량',기준거래량)
    #             print('대량거래이후 급격감소했던 것들.') ## 제외하자.## 하루 튀고 바로 다음날 기준거래량으로 들어온것들은 제외하자.그냥.   
    #             return False
                
            short_cond = (temp_df['Close'] / temp_df["Open"]) - 1  >= -0.003  ## -3프로이상
            cond_locate_ma3 = temp_df['ma3'] > abc_b_value * 0.9  ## 조금 여유두기.
            cond_except = temp_df['Close'] < abc_a_value  ## a값 밑으로 내려간게 하나라도 있으면 패쓰.
            vol_cond = temp_df['Volume'] <= 기준거래량

            if sum(cond_except)>0:
                # print('a아래로 내려간적있어 패쓰..')
                return False
            
            # trix cond
            cond_trix0 = self.info_trix.index[-1] == temp_df.index[-1]
            cond_trix1 = self.info_trix.index[-1] == temp_df.index[-2]
            cond_trix2 = self.info_trix.iloc[-1]['cross_status'] == 'gc'
            if (cond_trix0 | cond_trix1) & cond_trix2:
                # print('abc중 trix gc')
                self.points_dic['추적종목여부'] = True
                self.points_dic['추적종목이유'].append("abc")
                self.points_dic['type'].append(1)
                self.points_dic['type'].append(2)
                self.points_dic['type'].append(3)
                self.points_dic['buytime'] = True # 
                self.points_dic['reasons'].append("abc_trixup")
                
                
                    
            
            
            기준거래량만족데이터 = temp_df.loc[short_cond & cond_locate_ma3 & vol_cond]

            if (len(기준거래량만족데이터) == 1 ) & (len(temp_df) == 2):  
                # print("대량거래이후급격감소상태") # 이걸 포함해야하나 말아야하나. ex)...025530 SJM홀딩스, 011320 유니크, 023900 풍국주정
                pass

            elif (len(기준거래량만족데이터) == 0) & (len(temp_df) >= 2):
                # print('추적종목 추가')
                self.points_dic['point'] +=2
                self.points_dic['추적종목여부'] = True
                self.points_dic['추적종목이유'].append("abc")
                # self.points_dic['type'].append(1)
                # self.points_dic['type'].append(2)
                # self.points_dic['type'].append(3)
                # self.points_dic['buytime'] = True # 
                # self.points_dic['reasons'].append("abc대기")
                pass

            elif len(기준거래량만족데이터) > 0 & (len(temp_df) >= 2):
                # print('현재 조건만족중 ')
                
                cond_1 = len(기준거래량만족데이터) <= 4 # 만족함이 1 ~ 2개 일때.
                cond_2 = 기준거래량만족데이터.index[-1] == temp_df.index[-1] # 조건만족이 오늘인 조건
                # cls.info_ma20['현재방향'] == '상승'
                
                if len(기준거래량만족데이터) >= 4:
                    # print("기준거래량이 4일이상 지속되는것들")
                    pass
                    
                if cond_1 & cond_2 :
                    # print('기준거래량이 3일이하임') # --> 추적종목 추가 , 종가매수 가능. 
                    # self.points_dic['point'] +=1
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("abc")
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['buytime'] = True # 
                    self.points_dic['reasons'].append("abc_거래량만족")
                    
                    result = True
                
                if (len(기준거래량만족데이터) > 0 )  & (temp_df.iloc[-1]["Volume"] > 기준거래량) & (temp_df.iloc[-1]["Change"] > 0):
                    ## 단타용으로 볼만함. 올라갈때 찾는것.
                    # print('만족이후 거래량증가.')
                    self.points_dic['추적종목여부'] = True
                    self.points_dic['추적종목이유'].append("abc")
                    self.points_dic['type'].append(1)
                    self.points_dic['type'].append(2)
                    self.points_dic['type'].append(3)
                    self.points_dic['buytime'] = True # 
                    self.points_dic['reasons'].append("abc_거래만족후상승_*")
                    pass
        
        return result


    ##### find_stock_any_anal 지우고 새로 만들기. init 에도 삭제. 
    ## 분석함수로 추적종목사우 점수 모두 하나로 
    
    
    
    
    
    
    
    def __get_start_day(self,info_ma,변속지속일기준 = 15):
        '''
        info_ma dict   
        return : datetime, None
        '''
        # result_type_ls = info_ma['result_type_ls']
        # 변곡지속일 = info_ma['변곡지속일']

        # finded_item = [item for item in result_type_ls if 'w' in item ]
        # if len(finded_item):
        #     if (finded_item[0] =='w') & (변곡지속일 > 변속지속일기준 ): # w형성후 오래상승중인거 제외.
        #         return None
        
        return info_ma['start_day']
    
    def __get_대기종목(self):
        '''
        단순 60 20 파동조건임.
        
        '''
        
        result = False
        if "info_ma60" in dir(self):
            # cond1 = self.info_ma60['현재방향']=='상승'
            cond1 = len([item for item in self.info_ma20['result_type_ls'] if 'w' in item ]) > 0
        else:
            cond1 = True
        if "info_ma20" in dir(self):
            cond2 = len([item for item in self.info_ma20['result_type_ls'] if 'w' in item ]) > 0
        else:
            cond2 = True
        
        
        if cond1 & cond2:
            result = True
            
        if self.info_coke['result'] :
            result = True
            
            
            
        return result
        
    def find_stock_any_anal(self):
        '''
        + set_point 
        
        return dict 
        type : mode 와 같이 1은 장중. 2는 장마감시 , 3, 시간외(전체), 4. 다음장 대기종목,
        
        {'point': 3,
        '추적종목여부': True,
        '추적종목이유': ['3_20_gcv', 'ac_sun1'],
        'buytime': False,
        'type': [1, 2],
        'reasons': ['3_20_gcv', 'ac_sun1']}
        
        
        활용법. type 1 이 있으면 check30 해서 결정하기.
        reasons 별로 하려면 키워드 포함여부 사용.
        type 3 이있으면 대기종목으로 넣기.
        
        
        # 배열로 판단. 추후 대량거래 abc 상태 , 나 sun 돌파 코크상태에서 돌파 는 다시 포함시키기. 
        
        '''
        ## 배열조건. 
        cond_array = (not self.is_역배열_status()) & (self._장기추세_status()) | self.is_정배열_status() 
        ### 나중에 추가하기로 함. 일단 원하는 그림인지 확인한 후  조건 추가하기. 
        if cond_array:
            self.points_dic['추적종목여부'] = True
            self.points_dic['추적종목이유'].append("배열상태11")
            
        ## 추적종목인지 확인.
        if 'info_ma3' in dir(self)  :
            # try:
            start_day = self.__get_start_day(self.info_ma3)  ## 
            high_day = self.info_ma3['고점날짜'][-1]
    
    
            if (start_day != None) & (self.__get_대기종목()   ):
                self.points_dic['추적종목여부'] = True
                self.points_dic['추적종목이유'].append("20파동11")
                self.points_dic['type'].append(3)
            
            # if start_day > high_day:  ## 이게 무슨의미엿을가.? Nonetype error
            #     pass
                    # return result_dic
            # except:
            #     print('데이터 부족. find_stock_any_anal.py ',self.code_name)
          
        
        ## 거래량abc에서 기준거래량 찾기.
        if "info_ac" in dir(self):
            big_ac_df = self.info_ac[1].loc[start_day:]
            if len(big_ac_df):
                first_big_ac_date = big_ac_df.index[0] # 여러거래량일시 필요.
                last_big_ac_date = big_ac_df.index[-1]
                대량거래이전평균거래량 = self.df.loc[:first_big_ac_date,'vol20ma'][-2]
                abc_b_value = big_ac_df.iloc[-1]['abc_b']
                current_value = self.df.iloc[-1]['Close']
                기준거래량 = max( big_ac_df.iloc[-1]['Volume']/10 , 대량거래이전평균거래량)

                # 기준거래량일부터 데이터중. 기준거래량보다 작은게 1개이고 
                temp_df = self.df.loc[last_big_ac_date:]
                short_cond = abs((temp_df['Close'] / temp_df["Open"]) - 1)  <= 0.002
                vol_cond = temp_df['Volume'] < 기준거래량
                기준거래량만족데이터 = temp_df.loc[short_cond & vol_cond]
                
                ### 현재가 위치 조건 추가필요.
                cond_current_value = abc_b_value * 0.9 <= current_value ### 조금 여유두기.
                ###******************************** 정리필요.
                ## len(temp_df) ==2 & len(기준거래량만족데이터) == 1 --> 바로 거래 줄어든것.제외하기.
                if (len(기준거래량만족데이터) ==1 ) & (len(temp_df) ==2):
                    self.points_dic['추적종목여부']= False
                
                if (len(기준거래량만족데이터) <= 1) & (len(temp_df) !=2) & cond_current_value :
                    self.points_dic['추적종목여부']= True
                    self.points_dic['추적종목이유'].append("abc_vol대기11")
                    self.points_dic['reasons'].append("abc_vol대기")
                if len(기준거래량만족데이터)>0:
                    cond_1 = len(기준거래량만족데이터) <= 2 
                    cond_2 = 기준거래량만족데이터.index[-1] == temp_df.index[-1]
                    cond_short_candle = abs(1 - self.df.iloc[-1]['Close'] / self.df.iloc[-1]['Open']) <= 0.02

                    if cond_1 & cond_2 & cond_short_candle & cond_current_value:
                        self.points_dic['추적종목이유'].append("abc_vol만족11")
                        self.points_dic['reasons'].append("abc_vol만족")
                        self.points_dic['type'].append(2)
                        
        # ma20 확인.
        try:
            cond1 = self.df.loc[start_day,'ma3'] < self.df.loc[start_day,'ma20'] # 출발시는 long_ma 아래 있음.
            cond2 = self.df.loc[high_day,'ma3'] > self.df.loc[high_day,'ma20'] # 고점에서는 long_ma 위에 있음.
            cond3 = self.df.iloc[-1]['ma3'] > self.df.iloc[-1]['ma20'] # 현재도 long_ma 위에 있음.
            if cond1 & cond2 &  cond3:
                self.points_dic['reasons'].append("3_20_gcv")
                self.points_dic['추적종목이유'].append("3_20_gcv11")
                self.points_dic['type'].append(1)
                self.points_dic['type'].append(2)
        except:
            pass #  데이터가 없는경우 
        
        # gcv sun 확인
        try:
            sun_max_values = self.df['sun_max'].shift(1)  # sun maxvalue shift 하기. 전일값기준
            
            cond1 = self.df.loc[start_day,'ma3'] < sun_max_values.loc[start_day] # 출발시는 long_ma 아래 있음.
            cond2 = self.df.loc[high_day,'ma3'] > sun_max_values.loc[high_day]   # 고점에서는 long_ma 위에 있음.
            cond3 = self.df.iloc[-1]['ma3'] > sun_max_values.iloc[-1] # 현재도 long_ma 위에 있음.
            start_width = round(self.info_ma3['sun_gc_width_value'],0)
            if cond1 & cond2 & cond3 :
                self.points_dic['reasons'].append(f"sun_gcv({start_width}%)11")
                self.points_dic['추적종목이유'].append("sun_gcv11")
                self.points_dic['type'].append(1)
                self.points_dic['type'].append(2)
                
        except:
            pass 
        
        try:
            ## coke 확인
            coke_up_values = self.df['upper_bb'].shift(1) # coke shift , 전일값기준.
            
            cond1 = self.df.loc[start_day,'ma3'] < coke_up_values.loc[start_day] # 출발시는 long_ma 아래 있음.
            cond2 = self.df.loc[high_day,'ma3'] > coke_up_values.loc[high_day]   # 고점에서는 long_ma 위에 있음.
            cond3 = self.df.iloc[-1]['ma3'] > coke_up_values.iloc[-1] # 현재도 long_ma 위에 있음.
            start_width = round(self.info_ma3['bb_gc_width_value'],0)
            if cond1 & cond2 & cond3 :
                self.points_dic['reasons'].append(f"coke_gcv({start_width}%)11")
                self.points_dic['추적종목이유'].append("coke_gcv11")
                self.points_dic['type'].append(1)
                self.points_dic['type'].append(2)
        except:
            pass 
        
        # ac sun 돌파 확인. ac_sun_ls
        if "info_ac" in dir(self):
            ac_info = self.info_ac[0]  ## 일반적 ac 
            ##########   돌파당시에 sun   coke   상태 확인.  추가필요.
            
            ## 거래량터진지 3일까진 지켜보기. 
            if len(self.df.loc[ac_info.index[-1]:]) >4:
                self.points_dic['추적종목여부']= True
                self.points_dic['추적종목이유'].append("ac_sun11")
                self.points_dic['reasons'].append("ac_sun11")

                
                
            if len(ac_info):
                today_ac_cond = self.df.index[-1] == ac_info.index[-1] # 오늘 대량거래조건.
                if today_ac_cond:
                    try:
                        # sun
                        cond_sun1 = self.df.iloc[-1]['Open'] <  sun_max_values.iloc[-1] < self.df.iloc[-1]['Close'] 
                        cond_sun2 = self.df.iloc[-2]['Close'] <  sun_max_values.iloc[-1] < self.df.iloc[-1]['Close'] 
                        cond_sun = cond_sun1 | cond_sun2
                        sun_width_value = int(self.df['sun_width'].iloc[-2])
                        if cond_sun:
                            self.points_dic['reasons'].append(f"sun_up({sun_width_value}%)11")
                            self.points_dic['type'].append(2)
                            self.points_dic['type'].append(3)
                    except:
                        pass
                    
                    try:
                        # coke
                        cond_coke1 = self.df.iloc[-1]['Open'] < coke_up_values.iloc[-1] < self.df.iloc[-1]['Close']
                        cond_coke2 = self.df.iloc[-2]['Close'] < coke_up_values.iloc[-1] < self.df.iloc[-1]['Close']
                        cond_coke = cond_coke1 | cond_coke2
                        
                        coke_width_value = int(self.df['coke_width'].iloc[-2])
                        if cond_coke:
                            self.points_dic['reasons'].append(f"coke_up({coke_width_value}%)11")
                            self.points_dic['추적종목이유'].append("ac_coke11")
                            self.points_dic['type'].append(2)
                            self.points_dic['type'].append(3)

                    except:
                        pass
                    
    
    
    
    
    
    ##########################################################
    ## 함수 완성후 main.py 수정필요. 
    # 1. mode2 
    #############################################
    def plot(self,option= "일봉",cnt = 240,add_title="", **keywords):
        '''
        option : 일봉,주봉,월봉,30분봉,5분봉,15분봉 ... 
        **keywords 기본 line, key값에 scatter 들어있으면 scatter ,vline 추가하기.
        '''
        plt.rc('axes', unicode_minus = False)
        
        need_ma5 = {'ma10':{'color':'pink','width':2}, 
                    'ma20':{'color':'red','width':2}, 
                    'ma240':{'color':'darkgray','width':3}}
        need_ma30 = {'ma10':{'color':'black','width':2},
                     'ma20':{'color':'red','width':2},
                     'ma40':{'color':'red','width':2},
                     'ma60':{'color':'blue','width':3},
                     'ma240':{'color':'darkgray','width':3},
                     }
        need_ma_d  = {'ma3':{'color':'black','width':1.5},
                      'ma5':{'color':'darkgray','width':1},
                      'ma20':{'color':'red','width':3},
                      'ma60':{'color':'blue','width':3},
                      'ma120':{'color':'purple','width':1.5},
                      'ma240':{'color':'darkgray','width':3},
                      }
        
        need_ma_week  = {'ma3':{'color':'gray','width':1.5},
                         'ma20':{'color':'red','width':2},
                         'ma60':{'color':'blue','width':2},
                         }
        
        need_ma_mon  = {'ma3':{'color':'gray','width':1.5},
                'ma20':{'color':'red','width':2},}
        
        if option =='일봉': # 일봉
            df = self.df
            ma_dics = need_ma_d
        elif option == '30분봉': # 30분봉
            if "df_min30" not in dir(self): 
                self.df_min30 = self._get_data_daum('30분봉')
            df = self.df_min30
            ma_dics = need_ma30
        elif option == '5분봉': #5분봉
            if "df_min5" not in dir(self): 
                self.df_min5 = self._get_data_daum('5분봉')
            df = self.df_min5
            ma_dics = need_ma5
        elif option == '주봉': # 주봉
            if "df_w" not in dir(self): 
                self.df_w = self._get_data_daum('주봉')
            df = self.df_w
            ma_dics = need_ma_week
        elif option == '월봉': # 월봉
            if "df_m" not in dir(self): 
                self.df_m = self._get_data_daum('월봉')
            df = self.df_m
            ma_dics = need_ma_mon
        else:
            return None
        df = df[-cnt:].copy()
        
        
        # 그래프 구역 나누기
        fig = plt.figure(figsize=(25,15))
        
        ## 그리드설정(다시해보기 )
        from matplotlib.gridspec import GridSpec
        fig = plt.figure(figsize=(25, 15))
        gs = GridSpec(nrows=4, ncols=4 )   # width_ratios=[3, 1], height_ratios=[3, 1]
        ax_consen_y = fig.add_subplot(gs[0, 0])
        ax_consen_q = fig.add_subplot(gs[1, 0])
        ax_cost = fig.add_subplot(gs[2, 0])
        ax_cash_flow = fig.add_subplot(gs[3, 0])

        top_axes = fig.add_subplot(gs[:3, 1:])
        bottom_axes = fig.add_subplot(gs[3, 1:])


        # 인덱스 설정
        df.index = df.index.astype('str') # 수정

        # 인덱스 표시하기 매월 1일 인날만 표시하기. . 
        index_name = [day if day[-2:]=="01" else "" for day in df.index]

        # 이동평균선 그리기
        for ma_dic in ma_dics:
            ma = ma_dic
            if ma not in df.columns:
                continue
            color = ma_dics[ma_dic]['color']
            width = ma_dics[ma_dic]['width']
        #     print(ma,color,width)
            top_axes.plot(df.index, df[ma], label=ma, linewidth=width,color = color)  # 수정
            
        # ## 정지구간 있는지 확인. 
        정지데이터 = df.loc[ df['High']==0]
        try:
            if len(정지데이터):
                # line 그리기
                # 정지데이터.iloc[0]['Close'] , 정지데이터.iloc[-1]['Close']
                # 정지데이터 처리. 
                df.loc[(df['Open']==0) & (df['High']==0),['Open','High','Low']] = df['Close']
                
                # 정지구간 문자입력
                props = dict(boxstyle = 'round',facecolor = 'red', alpha = 0.5)
                top_axes.text(정지데이터.index[0] , 정지데이터['Close'].iloc[0] * 1.05,'정지구간',fontsize  = 16, bbox = props )
        except:
            pass
            
            
        ## bb그리기. 
        top_axes.plot(df.index, df['lower_bb'], label='lower', linewidth=1.2, color = 'grey') #수정
        top_axes.plot(df.index, df['upper_bb'], label='upper', linewidth=1.2, color = 'grey') # 수정

        ## sun_rise fill
        if df['sun_width'][-1]>30:
            temp_color='r'
        else :
            temp_color='c' 
        top_axes.fill_between(df.index,df['sun_min'], df['sun_max'],alpha = 0.2,color = temp_color) #수정

        ## width value 표시. 
        sun_width_value = round(df['sun_width'][-1],1)
        coke_width_value = round(df['coke_width'][-1],1)
        # top_axes.text(df.index[-1], df['sun_min'][-1],f"Mas : {sun_width_value}%",fontsize=20) # 수정
        # top_axes.text(df.index[-1], df['lower_bb'][-1],f"BB : {coke_width_value}%",fontsize=20) # 수정
        
        ## 그물망 bb 너비 한번더 표기. 
        props = dict(boxstyle = 'round',facecolor = 'yellow', alpha = 0.3)
        # top_axes.text(df.index[-1], df['upper_bb'][-1] , f"Mas : {sun_width_value:,.0f}%\nBB : {coke_width_value:,.0f}%" , fontsize=20, bbox = props)
        # top_axes.text(df.index[-1], df['lower_bb'][-1] , f"Mas : {sun_width_value:,.0f}%\nBB : {coke_width_value:,.0f}%" , fontsize=20, bbox = props)
        top_axes.text(0.7, 0.85, f"Mas : {sun_width_value:,.0f}%\nBB : {coke_width_value:,.0f}%" ,transform=top_axes.transAxes, fontsize=20, bbox = props)
        
        # 최근 새출 코크 최소값 표시 필요. 
        표시날짜 = self._get_low_high(df['sun_width'])[1]
        표시날짜 = 표시날짜[abs(표시날짜) >1].index
        display_sun = df.loc[표시날짜,['sun_min','sun_width']]
        for i,row in display_sun.iterrows():
            x = i
            y = row['sun_min']
            value = round(row['sun_width'],1)
            top_axes.text(x, y * 0.9 ,f"Mas_min : {value}%",fontsize=12)
        
        
        ## 선 그리기. (매물대. )
        try:
            매물대 = self._get_매물대(df)
            top_axes.axhline(y=매물대[0], color='orange', linewidth=5,alpha = 0.5)
            top_axes.text(df.index[-1],매물대[0],f'매물대:{매물대[0]:,} ' ,fontsize  = 12)

            top_axes.axhline(y=매물대[1], color='orange', linewidth=2,alpha = 0.5)
            top_axes.text(df.index[-1],매물대[1],f'매물대:{매물대[1]:,} ' ,fontsize  = 12)
        except:
            pass
        
        
        ## 매집비 표시. 외인금투연_구간별 , 외인금투연
        if option =='일봉':
            # try:
            props = dict(boxstyle = 'round',facecolor = 'yellow', alpha = 0.5)
            try:
                info_buy_rate = self.info_buy_rate['구간별']
            
                if len(info_buy_rate):
                    for dic in info_buy_rate:
                        x = dic['기간'][0]
                        y = dic['start_ma3_value']
                        v = dic['매집비']
                        if v >= 100:
                            순매수금액 = dic.get('순매수대금_억')
                            주도기관 = dic.get('주도기관')
                            풀매수여부 = dic.get('풀매수여부')
                            top_axes.text( x , y*0.9 , f'{v}% \n{순매수금액}억' ,fontsize  = 12, bbox = props)
                            top_axes.text( x , y*0.85 , f'{v}% \n{주도기관} {풀매수여부}' ,fontsize  = 12, bbox = props)
                        else:
                            top_axes.text( x , y*0.9 , f'{v}% ' ,fontsize  = 12, bbox = props)
                    
            except:
                info_buy_rate ={}
                print('info_buy_rate 자료 없음_ 그리기 오류')
                
                    # print(x,y,v)
            # except:
            #     print('매집비 없음.')

        #########################################################
        # ## 추가 라인 그리기. args keyworsds
        시작지점 = df.index[0]
        
        if len(keywords): 
            for key, value in keywords.items(): # Series 나 DataFrame 형태
                print(f'key : {key} , value : {type(value)}')
                if type(value)==pd.Series:   ## Series 이면 DataFrame 으로 변형
                    value = value.to_frame()
                    # print('df로 변환성공')
                if type(value) != pd.DataFrame:
                    print('Input Series or DataFrame!')
                    continue
                
                value.index = value.index.astype('str') ## 인덱스 str 형태로 변형
                value = value[ value.index >= 시작지점 ]
                
                # print('입력값 type : ',type(value.index))
                for col in value.columns:
                    if 'line' in key:
                        top_axes.plot(value.index, value[col] , label='lower', linewidth=2, color = 'black', alpha=0.8)
                    elif "scatter" in key:
                        top_axes.scatter(value.index,value[col],marker= "^",color='purple',s = 70, alpha = 0.7 )
                    elif "v" in key:
                        
                        for x in value.index:
                            top_axes.axvline( x ,c='r',alpha = 0.7)
                        pass
                    
                    else:
                        top_axes.plot(value.index, value[col] , label='lower', linewidth=2, color = 'black', alpha=0.6)
                        
                    # top_axes.plot(value.index, value[col] , label='lower', linewidth=6,  alpha=1 ,color = 'g')
                    # 옵션에 따라 점 표시 추가하기. 
        
        ## 거래량 그리기
        # ac 컬러지정.
        cond_v  = (df['Volume'] > df['pre_V']*2) |  (df['Volume'] > df['pre_pre_V']*2) # 전일 또는 전전일보다 2배인경우.
        cond_candle = df['Close'].shift(1) < df['Close'] # 전일종가보다 높은종가
        v_ac_color_list = ['red' if c&v else 'blue' if ~c&v else 'black'  for c,v in zip(cond_candle,cond_v)]
        
        # 상장주식수. 
        if "상장주식수" in dir(self):
            if self.상장주식수:
                cond_over_상장주식수 = df['Volume'] > self.상장주식수 
                if sum(cond_over_상장주식수):
                    bottom_axes.axhline(y=self.상장주식수, color='r', linewidth=1,label = '상장주식수')
                    
                v_ac_color_list = ['yellow' if cond else color for cond,color in zip(cond_over_상장주식수,v_ac_color_list)]
        # 유통주식수 추가하기. . 
        
        
        bottom_axes.bar(df.index, df['Volume'], width=0.5, align='center',color=v_ac_color_list)
        bottom_axes.plot(df.index, df['vol20ma'], label='ma20V', linewidth=1, color = 'r')
        
        # 캔들차트 그리기
        candlestick2_ohlc(top_axes, df['Open'], df['High'], 
                        df['Low'], df['Close'],
                        width=0.5, colorup='r', colordown='b')
        
        ## 이슈정보 표시.
        props = dict(boxstyle='round', facecolor='ivory', alpha=0.3)
        props_bad = dict(boxstyle='round', facecolor='red', alpha=0.3)
        props_good = dict(boxstyle='round', facecolor='blue', alpha=0.3)
        
        if "iss_name" in dir(self):
        # if self.iss_name !="":
            textstr = f'{self.iss_date_str} {self.iss_name} {self.iss_title}'
            top_axes.text(0.05, 0.20, textstr, transform=top_axes.transAxes, fontsize=18,
                    horizontalalignment='left', verticalalignment='bottom', bbox=props)
            textstr = f'{self.iss_company_info} {self.iss_other_issue}'
            top_axes.text(0.05, 0.15, textstr, transform=top_axes.transAxes, fontsize=18,
                    horizontalalignment='left', verticalalignment='bottom', bbox=props)
        try:
            ## naver_테마, 업종 표기. 
            if self.naver_themes !="":
                top_axes.text(0.05, 0.10, self.naver_themes, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=props)
            if self.naver_upjong !="":
                top_axes.text(0.05, 0.05, self.naver_upjong, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=props)
        except:
            pass
        
        ## 네이버 기본정보 표시
        if "유보율" in dir(self):
            if self.유보율: 
                textstr = f'유보율 : {self.유보율:,.1f}% 부채비율 : {self.부채비율:,.1f}% 당좌비율 : {self.당좌비율:,.1f}%'
                bbox = props
                if self.유보율 >500:
                    bbox = props_good
                if self.부채비율 > 100:
                    bbox = props_bad
                top_axes.text(0.05, 0.95, textstr, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=bbox)
        
                
                    
                    
                    
                    
        if "시가총액_억" in dir(self):
            if self.시가총액_억:
                textstr = f'시가총액_억 : {self.시가총액_억:,.0f} 시총순위 : {self.시총순위:,.0f}위'
                top_axes.text(0.05, 0.90, textstr, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=props)
        if ("액면가" in dir(self)) & ("상장주식수" in dir(self)):
            if self.액면가:
                textstr = f'액면가 : {self.액면가:,.0f}원 상장주식수 :{self.상장주식수:,.0f}'
                top_axes.text(0.05, 0.85, textstr, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=props)
        
        ### 매수사유 및 point정보 입력
        #  {'point': 2,
        #  '추적종목여부': True,
        #  '추적종목이유': ['20파동11'],
        #  'buytime': False,
        #  'type': [3],
        #  'reasons': []}
        
        if ("points_dic" in dir(self)) :
            textstr = f"point : {self.points_dic['point']:,.0f}점 ,추적종목여부:{self.points_dic['추적종목여부']} ,추적종목이유 :{self.points_dic['추적종목이유']}  "
            top_axes.text(0.05, 0.80, textstr, transform=top_axes.transAxes, fontsize=18,
                    horizontalalignment='left', verticalalignment='bottom', bbox=props)
    
            textstr = f"buytime:{self.points_dic['buytime']:}, type:{self.points_dic['type']:}, reasons:{self.points_dic['reasons']:} "
            top_axes.text(0.05, 0.75, textstr, transform=top_axes.transAxes, fontsize=18,
                    horizontalalignment='left', verticalalignment='bottom', bbox=props)

        # 현금가 표시
        if "현금가대비현재가" in dir(self):
            bbox = props
            if self.현금가대비현재가 <= 30:
                bbox = props_good
            textstr = f'현금가 : {self.현금가:,.0f}원 현재가 : {self.현재가:,.0f}원 현재가/현금가 : {self.현금가대비현재가:,.1f}%'
            top_axes.text(0.05, 0.70, textstr, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=bbox)
        
        
        ## best_invest 포함여부. or 실적주 여부.  
        try:
            stocks = Stocks()
            textstr = ""
            if self.code in stocks.best_invest_codes:
                textstr = "contains Best_Investor"
            if self.code in stocks.consen_up_codes:
                textstr = f"{textstr} 컨센상향조정"
            if self.is_good_consen():
                textstr = f"{textstr} 실적주"

            top_axes.text(0.05, 0.65, textstr, transform=top_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=props_good)
        except:
            print('실적, bestinvest체크 오류')
        ## 최근 뉴스정보 title 표시.
        # pass


        ### 마지막봉 종가 표시
        try:
            props = dict(boxstyle = 'round',facecolor = 'limegreen', alpha = 0.5)
            close = df.iloc[-1]['Close']
            today_change = df["Close"].pct_change(1)[-1] * 100
            top_axes.text( df.index[-1] , close + 100 , f'현재가:{int(close):,},등락율:{today_change:.2f}' ,fontsize  = 16, bbox = props)
        except Exception as e:
            print(e)

        
        ###new_표시 
        # bottom_axes
        try:
            if option =='일봉': 
                news_df = self.get_news()  ## 오래된기사가 많아지면 표시할때 self.df.index[0] 보다 큰 기사만 취급해야함.
                if len(news_df):
                    news_df1  = news_df[-10:]  ## 기사내용만 10개 제한 하고 표시는 모두.
                    props = dict(boxstyle = 'round',facecolor = 'y', alpha = 0.2)
                    textstr = news_df1['title'].to_string()
                    bottom_axes.text(0.05, 0.05, textstr, transform=bottom_axes.transAxes, fontsize=18,
                        horizontalalignment='left', verticalalignment='bottom', bbox=props)
                    for date in news_df['Date']:
                        try:
                            strdate = date.strftime('%Y-%m-%d')
                            top_axes.axvline(strdate, c = 'y',alpha = 0.1,linewidth = 15) ## 
                        except:
                            pass
                        
                        # top_axes.vlines(date,ymin=self.df.loc[date,'High'], ymax=self.df.loc[date,'High'] *1.5 ,  c = 'b' ,alpha = 0.5,linewidth = 20) ## 
                    
        except Exception as e:
            print('뉴스자료없음.',e)
            
            
        ## 거래량에 rsi 그리기. trix 아니면 rsi 둘중 하나만 그리기. 
        rsi_df = df.copy()
        rsi_df['rsi'] = talib.RSI(rsi_df['Close'])
        rsi_axes = bottom_axes.twinx()
        rsi_axes.plot(rsi_df.index,rsi_df['rsi'],color= 'k',label = 'RSI')
        rsi_axes.axhline(y=30, color='r', linewidth=1,label = '')
        rsi_axes.axhline(y=70, color='b', linewidth=1,label = '')
        rsi_axes.legend(loc = 'upper left')
        
        ## trix plot 
        trix_df = df.copy()
        trix = talib.TRIX(trix_df['Close'],7)
        trix_s = talib.MA(trix,5)
        
        trix_axes = bottom_axes.twinx()
        trix_axes.plot(trix.index,trix,color= 'r',label = 'trix7')
        trix_axes.plot(trix_s.index,trix_s,color= 'b',label = 'sig')
        trix_axes.legend(loc = 'upper center')

        # X축 티커 숫자 20개로 제한
        top_axes.xaxis.set_major_locator(ticker.MaxNLocator(10))
        bottom_axes.xaxis.set_major_locator(ticker.MaxNLocator(10))
        
        # 그리드설정
        top_axes.grid(True, axis='x',color = 'k',alpha = 0.5, linestyle = "--")
        bottom_axes.grid(True, axis='x',color = 'k',alpha = 0.5, linestyle = "--")
        
    
        # 그래프 title 지정
        title_name = f'{self.code_name}_({self.code})_{option} {add_title}'
        top_axes.set_title(title_name, fontsize=30)


        실적연도기준 = Sean_func.실적기준구하기("y")[1][1] # str
        실적분기기준 = Sean_func.실적기준구하기("q")[1] # list[ str, ]

        ## 실적표기하기. 
        # ax_consen_y = fig.add_subplot(gs[0, 0])
        # ax_consen_q = fig.add_subplot(gs[1, 0])
        # ax_cost = fig.add_subplot(gs[2, 0])
        # ax_cash_flow = fig.add_subplot(gs[3, 0])
        bbox ={'facecolor':'y','edgecolor':'r','boxstyle':'round','alpha':0.5}

        ## 연성장률 표기
        if "실적_y" in dir(self):
            ax_consen_y.plot(self.실적_y.index,self.실적_y['영업이익'],label = '영업이익',c = 'b')
            ax_consen_y.plot(self.실적_y.index,self.실적_y['당기순이익'],label = '당기순이익',c = "g")
            ax_consen_y.axhline(0,c = 'k',alpha = 0.5,linewidth = 1) #  흑자적자 구분선. 0 
            ax_consen_y.axvline(실적연도기준, c = 'y',alpha = 0.1,linewidth = 30) ## 실적연도기준 강조.

            for x, row in self.실적_y.loc[실적연도기준:].dropna().iterrows():
                y  = row['영업이익']
                status = row['status']
                if type(status) == str:   ## "적자" 등 값이 문자일때 처리. 
                    status = status
                else:
                    status = f"{round(row['status']*100)}%"
                ax_consen_y.text(x , y , status ,bbox=bbox)

            ax_consen_y.legend(loc='upper left')
            ax_consen_y.set_title('컨센서스')
            ax_consen_y_1 = ax_consen_y.twinx() ## 매출액 secondary_y 로 입력.
            ax_consen_y_1.plot(self.실적_y.index,self.실적_y['매출액'],label = '매출액',c="r")
            ax_consen_y_1.legend(loc = 'lower right')


        ## 분기 성장률 표기 (yoy, qoq ) 
        if "실적_q" in dir(self):
            ax_consen_q.plot(self.실적_q.index,self.실적_q['영업이익'],label = '영업이익',c = 'b')
            ax_consen_q.plot(self.실적_q.index,self.실적_q['당기순이익'],label = '당기순이익',c = "g")
            ax_consen_q.axhline(0,c = 'k',alpha = 0.5,linewidth = 1) #  흑자적자 구분선. 0 

            # for 기준분기 in list(set(실적분기기준) - set(self.실적_q.index) ):
            for 기준분기 in 실적분기기준:
                ax_consen_q.axvline(기준분기, c = 'y',alpha = 0.1,linewidth = 30) ## 실적연도기준 강조.
            ## 없는 자료 그리기. 강조자리. 

            ## 분기 yoy qoq 표기. 
            for q,row in self.실적_q.dropna().iterrows():
                y = row['영업이익'] 
                yoy_status = row['yoy_status']
                qoq_status = row['qoq_status']
                
                if type(yoy_status)==str:
                    yoy_status = yoy_status
                else: 
                    yoy_status = f"{yoy_status *100:,.0f}%"
                
                if type(qoq_status)==str:
                    qoq_status = qoq_status
                else: 
                    qoq_status = f"{qoq_status *100:,.0f}%"

                ax_consen_q.text(q , y , f"yoy:{yoy_status}\nqoq:{qoq_status}" ,bbox=bbox)



            ax_consen_q.legend(loc='upper left')
            ax_consen_q.set_title('컨센서스(분기)')

            ax2 = ax_consen_q.twinx() ## 매출액 secondary_y 로 입력.
            ax2.plot(self.실적_q.index,self.실적_q['매출액'],label = '매출액',c="r")
            ax2.legend(loc = 'lower right')

        ## 판관비 인건비 표기 
        # temp_df 가져오기. 
        try:
            manage_df = Fnguide.get_table_재무제표_from_db(self.code) # Fn재무제표
            

            manage_df = manage_df.loc[['판매비와관리비','매출원가']].T

            ax_cost.plot(manage_df.index,manage_df['판매비와관리비'],label = '판관비',c = 'r')
            ax_cost.legend(loc='center left')
            ax_cost.set_title('판매관리비')

            ax_cost2 = ax_cost.twinx() ## 매출액 secondary_y 로 입력.
            ax_cost2.plot(manage_df.index,manage_df['매출원가'],label = '매출원가',c ='b')
            ax_cost2.legend(loc = 'center right')
        except Exception as e:
            print(e)
            pass





        ## cash flow 표기
        if "현금흐름표" in dir(self):
            if len(self.현금흐름표):
                ax_cash_flow.plot(self.현금흐름표.index, self.현금흐름표["영업활동현금흐름"] ,label = '영업활동',c = "b")
                ax_cash_flow.plot(self.현금흐름표.index, self.현금흐름표["투자활동현금흐름"] ,label = '투자활동',c = "r")
                ax_cash_flow.plot(self.현금흐름표.index, self.현금흐름표["재무활동현금흐름"] ,label = '재무활동',c = "k")
                ax_cash_flow.legend(loc='upper left')
                ax_cash_flow.set_title('현금흐름표')
                ax_cash_flow.axhline(0,c = 'k',alpha = 0.5,linewidth = 1) #  흑자적자 구분선. 0 


        top_axes.legend()
        bottom_axes.legend()
        # plt.tight_layout()
        
        # ## 파일 저장하기 
        # try:
        #     if not os.path.exists("images"):
        #         os.mkdir("images")
        #     # fig.savefig('images/testchart.png',dpi=200)
        #     plt.savefig('images/testchart_30.png',dpi=200)
        # except:
        #     pass
        
        return fig
    
    #################### dart 관련 #######################
    def get_dart(self):
        '''
        dict 형태 {공시종류:df}
        '''
        fn = '/home/sean/sean/dart/all_dart.db'
        conn = sqlite3.connect(fn)
        
        table_name_ls = Sean_func.get_table_list_from_db(fn)
        dart_dict = {}
        for table_name in table_name_ls[1:]:

            sql = f"select * from '{table_name}' where code = '{self.code}'"
            data =  pd.read_sql(sql, conn)
            if len(data)>0:
                dart_dict[table_name] = data
        self.dart_dict = dart_dict
        
        return dart_dict
    
    
    
    
    
    def __str__(self):
        return_txt = f'{self.code} {self.code_name}'
        return return_txt
        
    def __repr__(self):
        return_txt = f'{self.code} {self.code_name}'
        return return_txt

class Stocks():
    def __init__(self,data_path='/home/sean/sean/data'):
        
        ## pickle 로 저장리스트 가져오기 .
        ## chart, earning , dart, buying, 
        
        if data_path == None:
            print('데이터 경로 없음')
            return
        elif data_path[-1]!="/":
            data_path += "/"
            
        self.data_path = data_path
            
        # self.__업종별구성종목가져오기()
        # self._get_업종리스트()
        # self._전체종목df = Fnguide.get_ticker_by_fnguide(data_path)
        
        self.best_invest_codes = self._extract_best_invest_codes()
        self.issue_codes = self._extract_issue_codes()
        self.consen_up_codes = self._extract_consen_up()
        # self.code_df = self._get_all_code_df()


    def _extract_best_invest_codes(self):
        '''
        특정기간동안 시총대비 매수비 상위종목들.
        '''
        
        invest0 = Sean_func.read_pickle('invest_tops_for_0day.pickle',self.data_path)
        invest4 = Sean_func.read_pickle('invest_tops_for_4day.pickle',self.data_path)
        invest7 = Sean_func.read_pickle('invest_tops_for_7day.pickle',self.data_path)
        invest15 = Sean_func.read_pickle('invest_tops_for_15day.pickle',self.data_path)
        best_invests_ls = list(set(pd.concat([invest0,invest4,invest7,invest15]).index))
        return best_invests_ls

    def _extract_issue_codes(self):
        '''
        최근한달 데이터로 division_n 으로 나눠서 확인하기. 
        '''
        division_n = 4
        
        # data (df)
        issue = Sean_func.read_pickle('issue_list.pickle',self.data_path)
        issue_code = Sean_func.read_pickle('issue_code_list.pickle', self.data_path)

        # 이슈에 있는 날짜 소팅해서 분할하기. 
        x = sorted(list(issue['날짜'].unique()))
        group_days = Sean_func.split_data(x,division_n)

        # 분할된 데이터 옆으로 합치기.
        ls = []
        for days in group_days:
            temp = issue[issue['날짜'].isin(days)]['이슈'].value_counts().reset_index().set_index('index')
        #     display(temp)
            ls.append(temp)
        # pd.concat(ls,concat = 1)
        result = pd.concat(ls, axis = 1).fillna(0)
        result.columns = range(division_n)

        # 총 언급횟수의 합계, 추이, 점수() 추측함.  
        move_count = []
        for i in range(division_n-1):
            temp = result.apply(lambda x : x[i+1]-x[i] * (i+1)* 0.5   ,axis = 1)
            move_count.append(temp)

        result['합계'] = result.apply(lambda x : sum(x), axis = 1)
        result['추이'] = sum(move_count).sort_values(ascending = False)
        result['점수'] = result.apply(lambda x : x['합계'] + x['추이'], axis= 1)
        result = result.sort_values('점수',ascending = False)

        ## 조건 추출.
        ## 점수가 8이상이고 추이가 0.5이상 이거나.  합계가 10이상이고 추이가 -3 초과인종목들 추출.
        result = result.loc[((result['점수'] >= 8) & (result['추이'] >= 0.5)) | ((result['합계'] > 10) & (result['추이'] > -3))]
        selected_issue_code_list = list(set(issue_code[issue_code['이슈'].isin(result.index)].code))

        return selected_issue_code_list
    
    def _extract_consen_up(self,n=40,item = '영업이익'):
        '''
        컨센서스 상향조정 종목 
        n: 최근n일간.
        item : 영업이익, 매출액,  EPS(원), 당기순이익
        변동율 : 0.1 # 10% 이상 변동된경우만 찾기.
        '''
        try:
            ## 최근 30(n)일간 컨센서스 상향.
            start_date = datetime.now().date() - timedelta(days = n) ## 감지날짜 지정.
            str_start_date = start_date.strftime("%Y%m%d")
            
            db_file_name = f'{self.data_path}/재무제표_변경.db'
            con = sqlite3.connect(db_file_name)
            
            table_name = Sean_func.get_table_list_from_db(db_file_name) ##  db테이블이름 가져오기
            연도별 = Sean_func.실적기준구하기('Y')[1] # 변동되는 연도별 기준
            분기별 = Sean_func.실적기준구하기('Q')[1] # 변동되는 분기별 기준. 
            
            ### sql 조건으로만 가져오기 함. 좀더 정교함이 필요함. 변화량. 과 변화율 
            sql = f'''
                select * from {table_name[0]} where 
                감지날짜 > "{str_start_date}" and 
                변동율 > 0.1 and 이전값 >0 and 
                row ="{item}" and 
                col in ("{연도별[-2]}","{연도별[-1]}","{분기별[-1]}")
                '''
                # 변화량 > 10 and
            with con :
                changed_finance_df = pd.read_sql(sql , con)
                changed_finance_df['감지날짜'] = pd.to_datetime(changed_finance_df['감지날짜'])

            result = list(changed_finance_df['종목코드'])
            result = list(set(result))
            return result
        except:
            return None
        
    def _extract_codes_from_upjong(self,upjong):
        '''
        upjong 단어 포함된 업종 코드 리스트 반환.
        '''
        upjong_df = Sean_func.read_pickle('naver_upjong_df.pickle',self.data_path)
        choice_df = upjong_df[upjong_df['name'].str.contains(f'{upjong}')]
        upjong_codes = list(choice_df['code'])
        return upjong_codes
    
    def _extract_codes_from_theme(self,theme):
        '''
        theme 단어 포함된 업종 코드 리스트 반환.
        '''
        theme_df = Sean_func.read_pickle('naver_theme_df.pickle',self.data_path)
        choice_df = theme_df[theme_df['name'].str.contains(f'{theme}')]
        theme_codes = list(choice_df['code'])
        return theme_codes



   
def update_db_date(n = 20):
    '''
    n일간 전체중 n일만 작업하기.
    '''
    job_result = True, pd.DataFrame()
    
    today = datetime.today().date()

    ### update  ohlcv 
    db_file_name = '/home/sean/sean/data/ohlcv_date.db'
    con = sqlite3.connect(db_file_name)
    # db불러와서 최근날짜 가져오기.
    try:
        # 랜덤으로 여러종목 선택해서 마지막날짜가 가장 많은 날짜지정하기.
        table_names = Sean_func.get_table_list_from_db(db_file_name)
        random_codes = random.choices(table_names,k = 30)
        last_date_temps  = [pd.read_sql(f'select max(Date) from "{code}"',con).iloc[0,0] for code in random_codes]
        last_date = pd.to_datetime(Counter(last_date_temps).most_common()[0][0])
        option ="db에서 가져온."
    except Exception as e:
        print(e, 'last_date 잡기 오류발생하여 종료함.')
        last_date = today - timedelta(days=450)
        option = "첨부터 "

    print(f"{option} {last_date} 부터 받기")
    # 최근날짜에서 현재까지 날짜 받기
    date_range = pd.date_range(start=last_date, end=today,freq = "B")
    date_range = date_range[ :n+1]
    print(f'date_range 를 {n}개로 지정. ')
    print(f'date_range : {date_range}')
    
    # 마지막날 전체 데이터 받기
    tickers = Fnguide.get_ticker_by_fnguide('/home/sean/sean/data')
    유효코드들  = [code[1:] for code in tickers['cd']]
    
    data_ls = []
    for date in date_range:        
        t_data = Sean_func.get_all_current_price(date)
        t_data = t_data[t_data['Code'].isin(유효코드들)]
        print(f'{date} {len(t_data)}개 데이터 다운로드..')
    
        if len(t_data):
            data_ls.append(t_data)
        time.sleep(2)
    
    ## 현재의 t_data 는 마지막날 받은 데이터임. 그러프로 이게 현재 데이터가 없다면 최근데이터는 현재 없는상태임.
    if not len(t_data):
        job_result = False
        return job_result, pd.DataFrame()
    print("########### 최근받은데이터 ###############")
    print(t_data.head())
    print("###########           ################")
        
    data = pd.concat(data_ls)
    # code, date 로 소팅.
    try:
        data = data.sort_values(['Code','Date'],ascending=[True,True])
        # data['Change'] = data['Close'].pct_change()
        # data = data.sort_values('Change',ascending = False)
    except Exception as e:
        print('sorting 에러', e)
    

    col = ["Date","Open","High","Low","Close","Volume","Amount","Change"]

    # 새로 받은 데이터 순환. 
    new_codes = data['Code'].unique()
    all_cnt = len(new_codes)
    
    for i, code in enumerate(new_codes):
        print('.',end="")
        if divmod(i,500)[1] == 0:
            print(f"{i}/{all_cnt}")
            print(f"받은데이터 범위 : {data['Date'].min()}부터 {data['Date'].max()}까지")
           
            
        extract_df = data[data['Code']==code]
        extract_df = extract_df.loc[:,col]
        try:
            import_query = f'select * from "{code}"'
            import_data = pd.read_sql(import_query, con)
            
            
            # import 된 데이터에 마지막날짜 가져옴. A날짜 
            import_last_dates = list(pd.to_datetime(import_data['Date']))
            # code에 해당하는 새로운데이터에서  A날짜 가 있으면 ok
            new_data_last_date = min(data[data['Code'] == code ]['Date'])
            new_data_last_date = pd.to_datetime(new_data_last_date) 
            # 없으면 A날짜부터 새로운데이터 첫데이터전날까지 데이터 다운로드 후 합치기.
            if not new_data_last_date in import_last_dates:  # 데이터 날짜가 없으면 전체 추가로 받기.
                if len(import_last_dates):
                    print(f'{code} : 다른 날짜 데이터 추가중...')
                    start = import_last_dates[-1]
                    end = new_data_last_date - timedelta(days = 1)
                    
                    # 이구간 데이터 받아서 data 에 추가하고 정렬하기.(주로 거래정지되었던것들임.재상장등.)
                    x = fdr.DataReader(code,start,end)
                    import_data = pd.concat([import_data,x.reset_index()])
                    import_data = import_data.reset_index(drop=True)
                    
                else:
                    import_data = pd.DataFrame()
                    print('오류')
                    continue
                    
                    
        except Exception as e:
            import_data = pd.DataFrame()
            print(f"db저장 실패 {e}")
            continue
        # 합치고 재저장하기. 
        x = pd.concat([import_data,extract_df])
        x['Date'] = pd.to_datetime(x['Date'])
        x = x.drop_duplicates(['Date'],keep='last')
        x = x.set_index('Date')
        #400개이상이면 제거 하기. 
        x = x[-400:]
        x.to_sql(code ,con,if_exists='replace')

        ## 오늘 당일 데이터만 반환.
        last_day = data['Date'].max()
        data = data[data['Date'] == last_day]
        
    return job_result, data
