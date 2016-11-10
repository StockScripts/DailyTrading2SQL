#! /usr/bin/python
# -*- coding: utf-8 -*-

'''
This is a py3.4 version.

Created on ${161004}
 
@author: ${ChenCHEN}
'''


import pandas as pd
import pymysql as pms

from datetime import datetime, timedelta

from WindPy import *

import re
zhPattern = re.compile(u'[\u4e00-\u9fa5]+') #用来判断一段文本中是否包含简体中文
numPattern = re.compile('[0-9]') #判断是否包含数字
ytm_types = ['行权','到期','永续','新发','repo']
discard_lists = ['Depo','tkn','gvn','Tkn','Gvn','Shibor','.','shibor'] # 凡是在这个list里面的元素都是无用信息，弃之
replace_lists = ['?'] #凡是信息包含这个list里的元素就把该元素去掉

def  getdata_XYZ(x): #新的，参考位置的信息判断提取函数 @161103
    tmp = ['0']*5 #初始化一个列表
    ytm_type = '0'
    useful_count = 0 
    useful_dict = {1:2,2:0,3:4} #用来对应到tmp顺序的词典
    if len(x) <= 4 :#如果少于4个信息，按利率债处理
        for u in x :
            if u in discard_lists:
                continue
            if zhPattern.search(u) :
                if u.find('.') == -1 :#没点且有中文
                    tmp[1] = u #债券名称
                    continue
            useful_count += 1
            if useful_count != 4 :               
                tmp[useful_dict[useful_count]] = u #!!!有问题，地方债问题没有解决
    else : #多于4个信息就按信用债提取数据
        for u in x :
            if u in discard_lists:
                continue
            if u in ytm_types:
                ytm_type = u
                continue              
            useful_count += 1
            if useful_count == 1:#第一个出现的是期限
                tmp[2] = u
            elif useful_count in [2,3]:#2,3位出现的是债券代码或者名字
                if u.isdigit() or u.find('.S') != -1 or u.find('.s') != -1 or u.find('I') != -1 or  u.find('i') != -1:
                    tmp[0] = u #债券代码
                else :
                    tmp[1] = u #债券名字
            elif useful_count in [4,5]:#4,5位出现的是评级或者YTM
                if u.find('A') != -1 or u.find('a') != -1 : #评级
                    tmp[3] = u.replace('.','') #去掉有时误多出的'.'
                else :
                    tmp[4] = u #YTM
        if ytm_type != '0':
            tmp[4] = tmp[4] + ytm_type
    
    return tmp
                    
            
            
#def  getdata_XYZ(x): #此函数用于对一行信用债的信息进行解析提取数据,'x'代表'line'
#    tmp = ['0']*5 #初始化一个列表
#    ytm_type = '0'        
#   #下面开始依据字符匹配将各类数据归类
#    for u in x:
#        #sg = u.encode("utf-8") #先统一为utf-8编码   
#        if u in discard_lists:
#            continue
#        sg = u
#        match = zhPattern.search(sg)
#        if match :
#            if sg.find('.') == -1:#含有中文且没点的可能为债券名称
#                name_check = 0
#                for tmp_type in ytm_types:#去排除列表里面排除一下，以防是’行权‘之类的字段
#                    if sg.find(tmp_type) != -1 :
#                        ytm_type = tmp_type#如果是行权之类的字段，就加到ytm_type里
#                        name_check = 1
#                        break
#                if name_check == 0:
#                    tmp[1] = sg #中文债券名称
#            elif sg.find('上市') != -1 :
#                tmp[0] = sg #带有上市的代码
#            else :
#                tmp[4] = sg #YTM
#        elif ( sg.find('D') != -1 or sg.find('M') != -1 or sg.find('Y') != -1 or sg.find('d') != -1 or sg.find('m') != -1 or sg.find('y') != -1) :
#            tmp[2] = sg #期限
#        elif sg.find('.') != -1 and (sg.find('I') == -1 and sg.find('i') == -1 and sg.find('S') == -1 and sg.find('s') == -1 ) :
#            tmp[4] = sg #YTM
#        elif sg.find('A') != -1 :
#            tmp[3] = sg #评级
#        else :
#            tmp[0] = sg #债券代码
#    if ytm_type != '0':
#        tmp[4] = tmp[4] + ytm_type
#   #tmp = [ yy.encode("utf-8") for yy in tmp]
#    return tmp

def name_detect(true_name,to_test):#此函数用来检测交易记录中的债券简称和万得中的债券简称匹配度
    mm = zhPattern.findall(true_name)
    hz_list = [y for x in mm for y in x]
    
    count = 0  
    for hz in hz_list :
        tmp_pattern = re.compile(hz)
        if tmp_pattern.search(to_test):
            count += 1            
    return count

def code_detect(raw_code,true_name,tr_date):#此函数用于寻找正确的债券代码（按与名字匹配度）
    global wind_count
    code_tails = ['.IB','.SH','.SZ']
    count_mark = 0
    true_code = raw_code
    true_wind = []
    for tail in code_tails:
        tmp_code = raw_code + tail
        wind_dat = w.wss(tmp_code, "sec_name,windl1type,windl2type,municipalbond,termifexercise,ptmyear","tradeDate=%s"%tr_date).Data
        wind_count += 1
        if wind_dat[0][0] != None:
            count_tmp = name_detect(true_name,wind_dat[0][0])
            if count_tmp >= count_mark :
                if wind_dat[1][0] != None and ( wind_dat[1][0].find('债') != -1 or wind_dat[1][0].find('期') != -1 or wind_dat[1][0].find('票据') != -1 or wind_dat[1][0].find('工具') != -1 or wind_dat[1][0].find('单') != -1 or wind_dat[1][0].find('券') != -1):
                    true_code = tmp_code
                    true_wind = wind_dat
                    count_mark = count_tmp
                    if true_name == '0': #找到一个代码符合债券代码条件之后如果之前没有名字（三条信息的国债金融债）
                        break
    return true_code,true_wind

def collect_line(f, tr_date) :#此函数用于逐行传入原始数据并生成最终交易列表
    global wind_count,line_count,error_lines 
    detect_flag = 0 #判断有没有进detect_name函数的符号
    w.start()
    print(datetime.now())
    lie=[]#初始化lie列表
    for line in f.readlines(): #此句逐行传入txt数据，但依然有问题
        line_count += 1
        if line == '\n' : #如果是空行跳过
            pass 
        else :  #不是空行则进行解析提取数据
            match_num = numPattern.search(line)
            for replace_mark in replace_lists:
                line = line.replace(replace_mark,'')
            x = line.split()
            if match_num and len(x) >= 2: #确定该行含有有用数据（有数字且数据量>2)
                raw_data = getdata_XYZ(x)
                if raw_data[0].find('(') != -1:#此段用于对含有（9.12上市）这一类的数据提取债券编码                    
                    end_num = raw_data[0].find('(')
                    tmp_true_code = raw_data[0]
                    raw_data[0] = tmp_true_code[0:end_num]
                true_code = raw_data[0]
                true_name = raw_data[1]
                wind_dat = []
                true_wind = []
                if raw_data[0].find('.') == -1:#此段用于找到没有.IB等后缀的债券的真实编码
                    detect_flag = 1
                    detect_result = code_detect(true_code,true_name,tr_date)
                    true_code = detect_result[0]                    
                    true_wind = detect_result[1]                   
                    if true_wind == []:#如果过了一圈还没有找到合适代码，可能是代码缺0情况
                        zero_true_code = '0' + raw_data[0]
                        detect_result = code_detect(zero_true_code,true_name,tr_date)
                        true_code = detect_result[0]                    
                        true_wind = detect_result[1]
                raw_data[0] = true_code
                if  detect_flag == 0:
                    wind_dat = w.wss(raw_data[0], "sec_name,windl1type,windl2type,municipalbond,termifexercise,ptmyear","tradeDate=%s"%tr_date).Data
                    wind_count += 1
                else :
                    wind_dat = true_wind
                    detect_flag = 0
                try:
                    raw_data[1] = wind_dat[0][0]
                    raw_data.append(wind_dat[1][0])
                    raw_data.append(wind_dat[2][0])
                    raw_data.append(wind_dat[3][0])
                    raw_data.append(wind_dat[4][0] if wind_dat[4][0] is None else '%.3f'%(float(wind_dat[4][0]))   )
                    raw_data.append(wind_dat[5][0] if wind_dat[5][0] is None else '%.3f'%(float(wind_dat[5][0]))  )   
                    raw_data.append(tr_date)
                    lie.append(raw_data)
                except IndexError as e:
                    raw_tuple = (line,raw_data)
                    error_lines[line_count] = raw_tuple
    w.stop()
    print(datetime.now())
    return lie

def getytm(s): #此程序用于将字符串形式、格式混乱的ytm统一成浮点数形式
    ytm_s = '' #ytm的数值
    type_s = '' #含权情况下ytm的类型：行权/到期
    paren_sign = 0
    for x in s:
        if x == '(' or x == '（':
            paren_sign = 1 #遇到括号，之后的数字就不处理了
        if (paren_sign == 0) and (x.isdigit() or x == '.') :
            ytm_s += x
        match = zhPattern.search(x)
        if match :
            if (x == '行' or x == '权'or x == '到' or x == '期') :
                type_s += x
        ytm_num = float(ytm_s)
    ytm_s = '%.4f%%'%(ytm_num) #用来将ytm转成百分形式输出
    return ytm_s,type_s  #多变量输出是元组形式


now = datetime.now()
yes = now - timedelta(hours = 24)
yesterday = yes.strftime("%y,%m,%d") # 昨天即交易日的时间

td = yes

wind_count = 0 #用于测试从万得借口中读取了多少次
line_count = 0 #用于记录当前处理到了第几行
error_lines = {} #用于记录出错的行号和对应信息

conn = pms.connect('localhost', 'root', '123456', 'mybond',charset='utf8')#链接数据库
cur = conn.cursor()#获取游标
        
while 1:
    try:        
        td_str = td.strftime('%Y%m%d')    
        path1 = 'C:/Users/chenchen/Desktop/BONDDAILY/%s.txt'%td_str
        file = open(path1)
        data_list = collect_line(file, td_str)
        bond_DF = pd.DataFrame(data_list,columns = ['code','name','term','rating','ytm','type1','type2','CT','option_year','maturity_year','trading_date'])
        
        t = bond_DF.ytm
        ytm_list = list(map(getytm,list(t))) #处理后得到的浮点数形式的ytm
        ytms = [x[0] for x in ytm_list]
        ytm_types = [x[1] for x in ytm_list]
        bond_DF.ytm = ytms #替换原来的YTM,以后要注意把‘行权’/‘到期’这些情况考虑进去
        bond_DF['ytm_type'] = ytm_types #储存着行权/到期这些信息
        
        real_terms = []
        bond_shape = bond_DF.shape
        bond_num = bond_shape[0]
        for ii in range(bond_num):
            if bond_DF.iloc[ii]['ytm_type'] == '行权':
                real_terms.append(bond_DF.iloc[ii]['option_year'])
            else : 
                real_terms.append(bond_DF.iloc[ii]['maturity_year'])
        bond_DF['real_term'] = real_terms # 考虑到行权问题之后的实际term
        
        cols = bond_DF.columns.tolist()
        cols.remove('trading_date')
        cols.append('trading_date')
        bond_DF = bond_DF[cols]
                                  
        bond_DF.to_sql('bonddaily_plus',conn,flavor='mysql',if_exists = 'append', index = False)
        td = td - timedelta(hours = 24)
        file.close()
    except IOError :
        print('hhh')
        break
    
conn.close()


            