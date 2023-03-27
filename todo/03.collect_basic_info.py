import os
import numpy as np
import pandas as pd
import multiprocessing as mp
from newms import newms as ms
import parmap
#### 하나하나 워크로 만들기. 가능하면 parmap 쓰자. 
import pickle

'''
전체  df 를 n으로 나누고 결과 결과 return 하는 함수만든후 
결과 모아서 저장.

'''

current_folder_path = os.getcwd()  +"/"
data_path = current_folder_path + "datas/"

## make folder 
if not os.path.exists(data_path):
    os.mkdir(data_path)
    print(f'maked {data_path} directory')
    
codes_df= ms.Fnguide.get_ticker_by_fnguide(data_path)
codes_df = codes_df[2:]
all_count = len(codes_df)

num_cores = mp.cpu_count()
job_list = ms.Sean_func.split_data(codes_df, num_cores )

def mp_job(codes_df):
    concat_list = []
    for i,row in codes_df[:].iterrows():
        
        code =row['cd'][1:]
        code_name = row['nm']
        try:
            item_info  = ms.Naver._get_item_info(code)
            item_info['code'] = code
            item_info['code_name'] = code_name
        except Exception as e:
            print('오류나서 pass')
            continue
        print(i,code_name , round(i/all_count * 100,1),'작업중..')
        concat_list.append(item_info)
    return concat_list

## 멀티작업!!
concat_list = parmap.map(mp_job,job_list,pm_processes=num_cores)

## 임시저장.
try:
    ## concat_list 일단 저장하기 .
    temp_file_name2 = f"{data_path}basic_info_temp_list.pkl","wb"
    with open(temp_file_name2, 'wb') as f:
        pickle.dump(concat_list, f, protocol=pickle.HIGHEST_PROTOCOL)
    print('concat_list 임시저장 완료!')
    ## 나중엔 없애야함.  ## 파일도 지워야함. 
except:
    pass
        
concat_list = [item for items in concat_list for item in items]  ## 1차원으로 

        


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
