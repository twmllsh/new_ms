import pytz
import os
import numpy as np
import pandas as pd
import parmap
import multiprocessing as mp
from newms import newms as ms
#### 하나하나 워크로 만들기. 가능하면 parmap 쓰자. 
from datetime import datetime
import pickle
import time


## 03 처럼 parmap 해보기.

def finance_job(tickers):
    current_folder_path = os.getcwd()  +"/"
    data_path = current_folder_path + "datas/"
    token_path = current_folder_path + 'token/'
    db = ms.Db(f'{token_path}db_info.json','financial_state')
    change_ls = []
    for idx,row in tickers.iterrows():
        code = row['cd'][1:]
        code_name = row['nm']
        print( idx, code_name )
        
        for gb in ['연도별', '분기별']:
            try:
                df = ms.Naver.get_naver_finance(code,gb)
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
                pre_df = db.get_db(query,index_col='주요재무정보')
                print(code_name,'db 가져오기 성공.')
                # with conn:
                #     pre_df = pd.read_sql(query , con = conn,index_col='주요재무정보')
            except:
                print(f'{code_name}db가져오기 실패')
                pre_df = pd.DataFrame()
                ## db에 저장하고 continue
                try:
                    db.put_db(df=df, table_name=tableName,if_exists='replace')
                    print('mysql db 새로 저장 성공!')
                    # with conn:
                    #     df.to_sql(tableName,conn,if_exists = 'replace')
                    #     print('db 새로 저장 성공!')
                except:
                    print('mysql db 새로 저장 실패')
                
                continue
                
            
            # change_df = Sean_func.find_difference_two_df(pre_df,df,종목코드 = code , 종목명 = code_name, 감지날짜 = str_today,구분 = gb)
            
            ## 변경사항이 있으면 변경내용을 change_ls에 추가하고  새로운 df 를 db로 저장.
            try:
                change_df = ms.Sean_func.find_difference_two_df(pre_df,df,종목코드 = code , 종목명 = code_name, 감지날짜 = str_today,구분 = gb)
                if len(change_df):
                    change_ls.append(change_df)
                    db.put_db(df=df, table_name=tableName,if_exists='replace')
                    # with conn:
                    #     df.to_sql(tableName,conn,if_exists = 'replace')
                    print(f'{code_name} {gb} 데이터갱신성공',end = "")
                else:
                    print(f'{code_name}종목의 변경사항없음..')
            except Exception as e:
                change_df = pd.DataFrame()
                print(f'{e} : {code_name} {code} 변경사항 저장 오류.....')
    return change_ls






current_folder_path = os.getcwd()  +"/"
data_path = current_folder_path + "datas/"
token_path = current_folder_path + 'token/'
## data폴더가 없으면 만들기.
if not os.path.isdir(data_path):
    os.mkdirs(data_path)

KST = pytz.timezone('Asia/Seoul')
today = datetime.now(KST)
str_today = today.strftime("%Y%m%d %H:%M")

tickers = ms.Fnguide.get_ticker_by_fnguide(data_path)[2:]
db = ms.Db(f'{token_path}db_info.json','financial_state')

num_cores = mp.cpu_count()
job_list = ms.Sean_func.split_data(tickers,num_cores)

## 멀티작업!!
change_ls = parmap.map(finance_job,job_list,pm_processes=num_cores)
change_ls  = [item for items in change_ls for item in items]
# change_ls = finance_job(tickers[:4],db)



## change_ls 임시 저장. =========== > 이상없다면 지워야함. 
temp_pkl_file_name =  f"{current_folder_path}change_ls_temp.pkl"
with open(temp_pkl_file_name,'wb') as f:
    pickle.dump(change_ls,f,protocol=pickle.HIGHEST_PROTOCOL)

## 검색자료가 있으면 ...
if len(change_ls):
    db = ms.Db(f'{token_path}db_info.json','mystock')
    
    result_df = pd.concat(change_ls)
    result_df.reset_index(drop=True,inplace=True)
    result_df['변동구분'] = result_df.apply(lambda x: '신규' if x['이전값']==0 and x['최근값']!=0 else '삭제' if x['이전값']!=0 and x['최근값']==0 else '변동'  , axis=1   )
    result_df['변동율'] = result_df.apply(lambda x :   0 if x['변동구분']=='삭제' else  x['최근값'] if x['변동구분']=='신규'  else (x['최근값']/x['이전값'])-1  ,axis=1  )
    
    ## 변경사항 저장. 
    try:
        db.put_db(result_df,'financial_state_change',if_exists='append',index=False)
        # con = sqlite3.connect(f'{data_path}재무제표_변경.db')
        # with con:
        #     result_df.to_sql('change_list',con,if_exists='append',index=False)
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

