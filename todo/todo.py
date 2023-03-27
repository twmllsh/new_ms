import os 
import numpy as np
import pandas as pd
import tqdm
import parmap
# import newms as ms
from newms import newms as ms
import multiprocessing as mp




class Worker_investor:
    '''
    투자자정보 저장하기. 4 시나 6 시이후에 객체생성시 작업시작.!
    '''
    def __init__(self, data_path=None):
        if data_path == None:
            current_folder_path = os.getcwd()  +"/"
            data_path = current_folder_path + "datas/"
            pass
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

def func(x,y,z):
    return x*y*z


if __name__ == "__main__":
    num_cores = mp.cpu_count()
    ls = list(range(1,100))
    new_ls = [(x,x,x) for x in ls]
    new_ls = ms.Sean_func.split_data(new_ls,num_cores)
    print(new_ls)
    # split_input = np.array_split(np.array(ls),num_cores)
    # split_input = list([i for i in split_input])
    
    result = parmap.map(
        func,
        new_ls,
        pm_pbar=True,
        pm_processes=num_cores,
    )
    
    # result = []
    # for i in ls:
    #     x = func(i)
    #     result.append(x)
    # print(result)
    