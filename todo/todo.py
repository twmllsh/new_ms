import os 
import pickle
import numpy as np
import pandas as pd
import requests
import time
import FinanceDataReader as fdr
import tqdm
import parmap
import multiprocessing as mp
from newms import newms as ms
from datetime import datetime, timedelta

# 01 투자자
class Worker_investor:
    '''
    투자자정보 저장하기. 4 시나 6 시이후에 객체생성시 작업시작.!
    '''
    def __init__(self, data_path=None):
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
            
        else:
            if data_path[-1] != "/":
                data_path += "/"
            self.data_path = data_path
        
        ## make folder 
        if not os.path.exists(data_path):
            os.mkdirs(data_path)
            print(f'maked {data_path} directory')

        
        ## Message 
        with open('{current_folder_path}token/telegram_token.txt','r') as f:
            self.token = f.readlines()[0].strip()
        
        self.msg = ms.Mymsg(self.token,'sean78_bot')
        script_name = __file__.split("/")[-1]


        try:
            result1 = ms.Investor.arrange_investor_lastdays(self.data_path)
            
            ms.Investor.investor_to_db(self.data_path)

            # result1 기간별 시총대비 ranking df 담음 list.
            temp_txt = ""
            for temp_df in result1:
                temp_txt += f'{len(temp_df)} ' 

            self.msg.send_message(f'{script_name} {self.__class__.__name__} 데이터개수 : {temp_txt}')

        except:
            self.msg.send_message(f'{script_name} {self.__class__.__name__}  error occcured!')

# 02 업종
class Worker_upjong:
    '''
    네이버 업종 테마 정보 업데이트
    '''
    def __init__(self, data_path=None):
        
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
        else:
            
            if data_path[-1] != "/":
                data_path += "/"
            self.data_path = data_path

        
        ## Message 
        token_file = current_folder_path + 'token/telegram_token.txt'
        with open(token_file,'r') as f:
            self.token = f.readlines()[0].strip()
        
        self.msg = ms.Mymsg(self.token,chat_name='sean78_bot')
        script_name = __file__.split("/")[-1]

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  start! ')

        ms.Naver.save_theme_upjong_list(self.data_path)

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  finished! ')

# 03 기본정보
class Worker_basic_info:
    '''
    기본정보 저장하기. 
    '''
    def __init__(self,data_path=None):
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
        else:
            if data_path[-1] != "/":
                data_path += "/"
            self.data_path = data_path
        
        ## Message 
        with open(current_folder_path + 'token/telegram_token.txt','r') as f:
            self.token = f.readlines()[0].strip()
        
        self.msg = ms.Mymsg(self.token,chat_name='sean78_bot')
        script_name = __file__.split("/")[-1]

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  start! ')

        ms.Naver._get_all_basic_info(self.data_path)
        # ms.Naver.get_basic_info(self.data_path)  ## 이두개를 어떻게 해결하나.

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  finished! ')

## 04 재무제표 네이버.
class Worker_재무제표:
    '''
    네이버 재무제표 db로 저장하기. 
    '''
    def __init__(self, data_path=None):
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
        else:
            if data_path[-1] != "/":
                data_path += "/"
            self.data_path = data_path
        
        ## Message 
        with open(current_folder_path+'token/telegram_token.txt','r') as f:
            self.token = f.readlines()[0].strip()
        
        self.msg = ms.Mymsg(self.token,chat_name='sean78_bot')
        script_name = __file__.split("/")[-1]

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  start! ')

        ms.Naver.naver_finance_update_db(self.data_path)

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  finished!! ')

# 05 이슈
class Worker_issue:
    '''
    thinkpool 이슈정보 업데이트 
    '''
    def __init__(self, data_path=None):
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
        else:
            if data_path[-1] != "/":
                data_path += "/"
            self.data_path = data_path
    
        ## Message 
        with open(current_folder_path+'token/telegram_token.txt','r') as f:
            self.token = f.readlines()[0].strip()
        
        self.msg = ms.Mymsg(self.token,chat_name='sean78_bot')
        script_name = __file__.split("/")[-1]

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  start! ')

        result = ms.Thinkpool.update_issue_code(self.data_path)
        cnt = len(result)

        self.msg.send_message(f'{script_name} {self.__class__.__name__} {cnt}개 이슈 업데이트 finished! ')

## 06 fnguide table 재무제표        
class Worker_table_data_to_db:
    '''
    fnguide 테이블재무제표 저장하기.
    '''
    def __init__(self, data_path=None):
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
        else:
            if data_path[-1] != "/":
                data_path += "/"
            self.data_path = data_path

        
        ## Message 
        with open(current_folder_path + 'token/telegram_token.txt','r') as f:
            self.token = f.readlines()[0].strip()
        
        self.msg = ms.Mymsg(self.token,chat_name='sean78_bot')
        script_name = __file__.split("/")[-1]

        self.msg.send_message(f'{script_name} {self.__class__.__name__}  start! ')

        ms.Fnguide.table_재무제표_to_db(self.data_path)
        # result = ms.Thinkpool.update_issue_code(self.data_path)

        self.msg.send_message(f'{script_name} {self.__class__.__name__} 테이블재무제표 저장 finished! ')
        
## 07 매일 지수 확인. 
class Worker_get_index():
    def __init__(self):
        self.get_index()
        
    def get_index(self):

        # start_day
        today = datetime.now()
        start_day = today - timedelta(days=365)
        start_day = start_day.strftime('%Y-%m-%d')

        current_folder_path = os.getcwd()  +"/"
        data_path = current_folder_path + "datas/"
        
        # 환율 달러 원화
        usdkrw = fdr.DataReader('USD/KRW', start_day)
        # S&P 500 vix 지수
        vix = fdr.DataReader('VIX', start_day)
        # 코스피지수
        kospi = fdr.DataReader('KS11', start_day)
        kosdaq = fdr.DataReader('KQ11', start_day)
        # s&p 지수
        sp500 = fdr.DataReader('US500', start_day)
        # DJI 지수
        dji = fdr.DataReader('DJI', start_day)
        # IXIC 지수
        ixic = fdr.DataReader('IXIC', start_day)

        df = pd.concat([vix['Close'], kospi['Close'], kosdaq['Close'],
                        sp500['Close'], dji['Close'], ixic['Close'], usdkrw['Close']],
                    axis=1)
        df.columns = ['VIX', 'KOSPI', 'KOSDAQ', 'S&P500', 'DJI', 'IXIC', 'USDKRW']
        # df = df [['VIX', 'KOSPI', 'S&P500','USDKRX']]
        df_nomal = ms.Sean_func.nomalize(df)

        temp_df = df
        temp_df['vix_line'] = 50
        temp_df['usdkrx_top'] = 1250
        temp_df['usdkrx_bottom'] = 1050

        result_ls = [temp_df, df_nomal]

        with open(f"{data_path}index_df.pickle", 'wb') as f:
            pickle.dump(result_ls, f, protocol=pickle.HIGHEST_PROTOCOL)

## 한달에 한번 휴장일 데이터 받기. 
class Worker_get_holiday_data():
    def __init__(self):
        ms.Sean_func.get_휴장일()

## news 가져오기 1시간마다 실행. 
class Worker_get_news_from_stockplus():

    def __init__(self):
        self.news_job()
        
    def get_news_from_stockplus_today(self):
        '''
        1시간마다 가져오기.
        '''
        url = "https://mweb-api.stockplus.com/api/news_items/all_news.json?scope=popular&limit=1000"
        params = {"user-agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}
        resp = requests.get(url,params=params)

        js = resp.json()
        df = pd.DataFrame(js['newsItems'])
        df["relatedStocks"] = df.apply(lambda x : ','.join(  [ i['shortCode'][1:] for i in x['relatedStocks'] ]   )   ,axis = 1)
        col = ['id', 'title', 'createdAt', 'relatedStocks','writerName','url']
        df['createdAt'] = pd.to_datetime(df['createdAt']).dt.tz_convert('Asia/Seoul')
        df = df.sort_values(by='createdAt')
        df = df[col]
        
        
        return df

    def news_job(self):
        
        current_folder_path = os.getcwd()  +"/"
        data_path = current_folder_path + "datas/"
        db_info_file = data_path + 'db_info.json'
        
        mymsg = ms.Mymsg() ## 알림채널 지정해주기.
        
        # 새로운데이터 받아서
        new_data = self.get_news_from_stockplus_today()
        
        # 기존데이터에 있는지 확인
        # fn = '/home/sean/sean/data/stockplus_news.db'
        # conn = sqlite3.connect(fn)
        table_name = 'news'
        sql = f"select * from {table_name}"
        # pre_data = pd.read_sql(sql, conn)
        
        db = ms.Db(db_info_file_path=db_info_file,db_name='mystock')
        pre_data = db.get_db(sql)
        
        pre_data['createdAt'] = pd.to_datetime(pre_data['createdAt'])
        
        ## 없는 진짜 새로운 데이터추출
        if len(new_data):
            real_new_data = new_data.loc[     ~(new_data['id'].isin(pre_data['id']))     ]
        
            # 데이터 베이스에 추가
            if len(real_new_data)>0:
                
                # 데이터 추가. 
                # real_new_data.to_sql(table_name,conn,if_exists='append',index=False)
                db.put_db(real_new_data,table_name,if_exists='append',index=False)
                
                
                print(f'{len(real_new_data)}개 데이터 저장됨.')
                # 특정 뉴스 걸리면 메세지 보내기. 
                
                msg_cond = real_new_data['title'].str.contains("특징주|턴어라운드|사상|계약|호황")
                msg_df = real_new_data.loc[msg_cond]
                print(f"총 {len(real_new_data)}개 뉴스중 {len(msg_df)}개 추출되어 메세지 보내짐.")
                txt_ls = mymsg.df_to_msgtext(msg_df,extract=['title','url','createdAt','relatedStocks'])
                for txt in txt_ls:
                    print(txt)
                    print("==="*5)
                    mymsg.send_message(txt)
                    time.sleep(5)

        else:
            print('뉴스자료가 없음.')
        return real_new_data




        
if __name__ == "__main__":
    
    print(ms.Sean_func.is_휴장일())
    
    
    
    
    # num_cores = mp.cpu_count()
    
    # result = parmap.map(
    #     func,
    #     new_ls,
    #     pm_pbar=True,
    #     pm_processes=num_cores,
    # )
    
    