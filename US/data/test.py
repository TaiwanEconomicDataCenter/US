# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import math, re, sys, calendar, os, copy, time
import pandas as pd
import numpy as np
from datetime import datetime, date

ENCODING = 'utf-8-sig'
out_path = './output/'
column = ['code','snl', 'frequency', 'from', 'to', 'description', 'a1', 'a2', 'a3', 'a4', 'a5', 'unit', 'source']

def takeFirst(alist):
    return alist[0]
"""
tStart = time.time()
print('Reading file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = readExcelFile(data_path+'QNIA_key'+NAME1+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
print('Reading file: QNIA_key'+NAME2+', Time: ', int(time.time() - tStart),'s'+'\n')
df_key = readExcelFile(data_path+'QNIA_key'+NAME2+'.xlsx', header_ = 0, acceptNoFile=False, index_col_=0, sheet_name_='QNIA_key')
#print('Reading file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
#DATA_BASE_t = readExcelFile(data_path+'MEI_database.xlsx', header_ = 0, index_col_=0, acceptNoFile=False)
"""
FREQ = ['AB_A','C_A','DN_A','OZ_A','AM_M','NT_M','UZ_M','AT_Q','UZ_Q','_DFSW']
lines = []
for fr in FREQ:
    with open('./US'+fr+'.txt','r',encoding='ANSI') as f:
        line = f.readlines()
    for l in range(len(line)):
        line[l] = line[l].replace('\n','').replace('"','')
    lines.extend(line)
#print(lines)

#frequency = 'DAILY'
usmsd = []
g_t = []
sort_key = []
code = ''
snl = 1
frequency = ''
fromt = ''
to = ''
description = ''
unit = ''
#base = ''
#quote = ''
source = ''
#attribute = ''
#note = ''
#last = ''
countS = 0
ignore = False
for l in range(len(lines)):
    #print(lines[l])
    sys.stdout.write("\rLoading...("+str(int((l+1)*100/len(lines)))+"%)*")
    sys.stdout.flush()
    if l+1 >= len(lines):
        usmsd.append(g_t)
        break
    if not lines[l] or lines[l] == ' ':
        if lines[l+1].find('SERIES') >= 0:
            if g_t != []:
                usmsd.append(g_t)
            g_t = []
            code = ''
            frequency = ''
            fromt = ''
            to = ''
            description = ''
            unit = ''
            #base = ''
            #quote = ''
            source = ''
            #attribute = ''
            #note = ''
            ignore = False
    elif ignore == True:
        continue
    elif lines[l].find('#') >= 0:
        continue
    else:
        if lines[l].find('SERIES') >= 0:
            countS+=1
            loc1 = lines[l].find(':')+1
            loc2 = lines[l].find(' ', loc1)
            code = lines[l][loc1:loc2]
            g_t.append(code)
            g_t.append(snl)
            sort_key.append([code, snl])
            snl+=1
        elif lines[l].find('Data for') >= 0:
            locf1 = lines[l].find('Data')-1
            frequency = lines[l][:locf1].replace('    ','')
            g_t.append(frequency)
            loc3 = lines[l].find('from')+5
            loc4 = lines[l].find('to')-2
            loc5 = lines[l].find('to')+3
            #loc6 = lines[l].find('2', loc5)+4
            fromt = lines[l][loc3:loc4]
            to = lines[l][loc5:]
            if frequency == 'ANNUAL':
                #print(lines[l])
                try:
                    fromt = int(fromt)
                    to = int(to)
                except:
                    fromt = fromt
                    to = to
            g_t.append(fromt)
            g_t.append(to)
        else:
            d = lines[l]
            des = ''
            m = l
            head = True
            while lines[m+1].find('SERIES') < 0 and lines[l].find('#') < 0:
                if d != '':
                    if d[:1].isupper() == True and d[1:2].islower() == True:
                        des = des+' '+d
                    else:
                        des = des+d
                m+=1
                d = lines[m]
                if m+1 >= len(lines):
                    break
            dnew = des.replace('  ',' ').replace('Pp','PP').replace('United States National Income And Production Accounts ','National Income And Production Accounts, ').strip()
            g_t.append(dnew)
            dnew = dnew.replace(' - United States','')
            att = dnew.split(',',4)
            if len(att) < 5:
                for a in range(5-len(att)):
                    att.append('')
            for word in att:
                g_t.append(word.strip())
            if des.title().find('Units:') >= 0:
                loc7 = des.title().find('Units:')+7
                loc8 = des.title().find('Source')
                unit = des[loc7:loc8].replace(':','')
            elif des.title().find('Units') >= 0:
                loc7 = des.title().find('Units')+6
                loc8 = des.title().find('Source',loc7)
                unit = des[loc7:loc8].replace(':','')
            if des.title().find('Source:') >= 0:
                loc8 = des.title().find('Source:')+8
                source = des[loc8:].replace(':','')
            elif des.title().find('Source') >= 0:
                loc8 = des.title().find('Source')+8
                source = des[loc8:].replace(':','')
            if des.title().find('Exchange Rates') >= 0:
                #loc10 = des.find('-')+1
                #loc11 = des.find('- ', loc10)
                #loc12 = des.find('- ', loc10)+2
                loc13 = des.title().find('Units:')+7
                loc14 = des.title().find('Source:')
                loc15 = des.title().find('per')+4
                loc16 = des.title().find(',',loc15)
                #source = des[loc10:loc11]
                #attribute = des[loc12:]
                base = des[loc15:loc16]
                quote = des[loc13:loc14]
            #base = lines[l][loc12:loc13]
            #quote = lines[l][loc10:loc11]
            g_t.append(unit)
            g_t.append(source)
            #g_t.append(base)
            #g_t.append(quote)
            
            ignore = True
        #else:
        #    g_t.append(lines[l])
        
    #last = l
sys.stdout.write("\n\n")
#print(usmsd)
"""
for g in usmsd:
    if len(g) > 13:
        print(g)
"""
sort_key.sort(key=takeFirst)
repeated = 0
for i in range(1, len(sort_key)):
    if sort_key[i][0] == sort_key[i-1][0]:
        repeated += 1
        for key in usmsd:
            if key[1] == sort_key[i][1]:
                if key[5].find('2012') >= 0:
                    for key2 in usmsd:
                        if key2[1] == sort_key[i-1][1]:
                            usmsd.remove(key2)
                            break
                else:
                    usmsd.remove(key)
                    break
    sys.stdout.write("\r"+str(repeated)+" repeated key(s) found")
    sys.stdout.flush()
sys.stdout.write("\n")

print(countS-repeated)
ger = pd.DataFrame(usmsd, columns=column)
print(ger)
ger.to_excel(out_path+"usmsd.xlsx", sheet_name='usmsd')



"""
print('Concating file: QNIA_key'+NAME1+', Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = pd.concat([KEY_DATA_t, df_key], ignore_index=True)

print('Concating file: MEI_database, Time: ', int(time.time() - tStart),'s'+'\n')
for d in DB_name_A:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_A[d])
    else:
        DATA_BASE_t[d] = DB_A[d]
sys.stdout.write("\n")
for d in DB_name_Q:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_Q[d])
    else:
        DATA_BASE_t[d] = DB_Q[d]
sys.stdout.write("\n")
for d in DB_name_M:
    sys.stdout.write("\rConcating sheet: "+str(d))
    sys.stdout.flush()
    if d in DATA_BASE_t.keys():
        DATA_BASE_t[d] = DATA_BASE_t[d].join(DB_M[d])
    else:
        DATA_BASE_t[d] = DB_M[d]
sys.stdout.write("\n")

print('Time: ', int(time.time() - tStart),'s'+'\n')
KEY_DATA_t = KEY_DATA_t.sort_values(by=['name', 'db_table'], ignore_index=True)
unrepeated = 0
#unrepeated_index = []
for i in range(1, len(KEY_DATA_t)):
    if KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i-1] and KEY_DATA_t['name'][i] != KEY_DATA_t['name'][i+1]:
        print(list(KEY_DATA_t.iloc[i]),'\n')
        unrepeated += 1
        #repeated_index.append(i)
        #print(KEY_DATA_t['name'][i],' ',KEY_DATA_t['name'][i-1])
        #key = KEY_DATA_t.iloc[i]
        #DATA_BASE_t[key['db_table']] = DATA_BASE_t[key['db_table']].drop(columns = key['db_code'])
        #unrepeated_index.append(i)
        
    #sys.stdout.write("\r"+str(repeated)+" repeated data key(s) found")
    #sys.stdout.flush()
#sys.stdout.write("\n")
print('unrepeated: ', unrepeated)
#for i in unrepeated_index:
    #sys.stdout.write("\rDropping repeated data key(s): "+str(i))
    #sys.stdout.flush()
    #KEY_DATA_t = KEY_DATA_t.drop([i])
#sys.stdout.write("\n")

KEY_DATA_t.reset_index(drop=True, inplace=True)
if KEY_DATA_t.iloc[0]['snl'] != 1:
    KEY_DATA_t.loc[0, 'snl'] = 1
for s in range(1,KEY_DATA_t.shape[0]):
    sys.stdout.write("\rSetting new snls: "+str(s))
    sys.stdout.flush()
    KEY_DATA_t.loc[s, 'snl'] = KEY_DATA_t.loc[0, 'snl'] + s
sys.stdout.write("\n")
"""
