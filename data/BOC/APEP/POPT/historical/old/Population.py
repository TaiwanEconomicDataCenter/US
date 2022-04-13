# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from urllib.error import HTTPError
#from US_concat import CONCATE, readExcelFile

ENCODING = 'utf-8-sig'
out_path = "./output/"
data_path = './output/'

def takeFirst(alist):
    return alist[0]

# 回報錯誤、儲存錯誤檔案並結束程式
def ERROR(error_text):
    print('\n\n= ! = '+error_text+'\n\n')
    with open('./ERROR.log','w', encoding=ENCODING) as f:    #用with一次性完成open、close檔案
        f.write(error_text)
    sys.exit()

def readFile(dir, default=pd.DataFrame(), acceptNoFile=False,header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,encoding_=ENCODING,engine_='python',sep_=None):
    try:
        t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,skipfooter=skipfooter_,\
                        names=names_,usecols=usecols_,nrows=nrows_,encoding=encoding_,engine=engine_,sep=sep_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except HTTPError as err:
        if acceptNoFile:
            return default
        else:
            ERROR(str(err))
    except:
        try: #檔案編碼格式不同
            t = pd.read_csv(dir, header=header_,skiprows=skiprows_,index_col=index_col_,\
                        engine='python')
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

def readExcelFile(dir, default=pd.DataFrame(), acceptNoFile=True, \
             header_=None,names_=None,skiprows_=None,index_col_=None,usecols_=None,skipfooter_=0,nrows_=None,sheet_name_=None):
    try:
        t = pd.read_excel(dir,sheet_name=sheet_name_, header=header_,names=names_,index_col=index_col_,skiprows=skiprows_,skipfooter=skipfooter_,usecols=usecols_,nrows=nrows_)
        #print(t)
        return t
    except FileNotFoundError:
        if acceptNoFile:
            return default
        else:
            ERROR('找不到檔案：'+dir)
    except:
        try: #檔案編碼格式不同
            t = pd.read_excel(dir, header=header_,skiprows=skiprows_,index_col=index_col_,sheet_name=sheet_name_)
            #print(t)
            return t
        except:
            return default  #有檔案但是讀不了:多半是沒有限制式，使skiprow後為空。 一律用預設值

POP = {}
OUT = {}
OUT['Resident'] = {}
OUT['RPPAFO'] = {}
OUT['Civilian'] = {}
OUT['Resident']['Total'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['Resident']['Male'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['Resident']['Female'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['RPPAFO']['Total'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['RPPAFO']['Male'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['RPPAFO']['Female'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['Civilian']['Total'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['Civilian']['Male'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
OUT['Civilian']['Female'] = pd.DataFrame(index=[999]+list(range(100)), columns=list(range(1980,1990)))
for h in ['R','P','C']:
    for i in range(80, 90):
        POP['19'+str(i)+str(h)] = readFile(data_path+'E'+str(i)+str(i+1)+str(h)+'QI.TXT', index_col_=(0,1), acceptNoFile=False, sep_='\\s+', usecols_=[1,2,3,4,5], engine_='python', names_=['year','age','Total','Male','Female'])
        #print(POP['19'+str(i)+str(h)])
        for j in range(POP['19'+str(i)+str(h)].shape[0]):
            if str(POP['19'+str(i)+str(h)].index[j][0])[:1] == '7':
                if h == 'R':
                    OUT['Resident']['Total'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Total']
                    OUT['Resident']['Male'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Male']
                    OUT['Resident']['Female'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Female']
                elif h == 'P':
                    OUT['RPPAFO']['Total'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Total']
                    OUT['RPPAFO']['Male'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Male']
                    OUT['RPPAFO']['Female'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Female']
                elif h == 'C':
                    OUT['Civilian']['Total'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Total']
                    OUT['Civilian']['Male'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Male']
                    OUT['Civilian']['Female'].loc[int(POP['19'+str(i)+str(h)].index[j][1]),int('19'+str(i))] = POP['19'+str(i)+str(h)].iloc[j]['Female']

for h in ['Resident','RPPAFO','Civilian']:
    with pd.ExcelWriter(out_path+h+".xlsx") as writer: # pylint: disable=abstract-class-instantiated
        for i in OUT[h]:
            OUT[h][i].to_excel(writer, sheet_name = i)
        