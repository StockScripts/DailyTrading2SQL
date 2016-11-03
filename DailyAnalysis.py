#! /usr/bin/python
# -*- coding: utf-8 -*-

'''
This is a py3.4 version.

Created on ${date}
 
@author: ${user}
'''

import pandas as pd

import matplotlib.pyplot as plt

import pymysql as pms

from datetime import datetime, timedelta

from WindPy import *

#数据库读入
conn = pms.connect('localhost', 'root', '123456', 'bond01',charset='utf8')#链接数据库
cur = conn.cursor()#获取游标

cj_sql = 'select * from bond_data2 where trading_date = \'%s\''%'20160921';
cur.execute(cj_sql)
cj = cur.fetchall()
conn.close()

#存入DF
cj_list = [list(x) for x in cj]#
cj_list = [x[1:] for x in cj] #构建DataFrame只能用list来构建，tuple不行
records = pd.DataFrame(cj_list,columns = ['code','name','term','rating','ytm','type1','type2','trading_date'])#读取所有成交数据到DF

#预处理：选取要分析的数据
B = records[(records['rating'] == 'AAA') & (records['type1'] == '企业债')]#x选取其中企业债部分
A = records[records['type1'] == '企业债']

#Term 处理
term_years = []
w.start()
for code in A.code: #此段用于根据万得的到期日算出以年为单位的TERM,注意此处含权债问题未考虑
     mtd = w.wss(code,'maturitydate').Data
     cday = datetime.strptime('20160921', '%Y%m%d')
     delta1 = mtd[0][0] - cday
     term_years.append(delta1.days/365)
A['term_year'] = term_years
w.stop()

#YTM处理
def getytm(s): #此程序用于将字符串形式、格式混乱的ytm统一成浮点数形式
    ytm_s = ''
    for x in s:
        if x.isdigit():
        	ytm_s += x
    ytm = float(ytm_s)/100
    return ytm

t = A.ytm
ytms = list(map(getytm,list(t))) #处理后得到的浮点数形式的ytm
A.ytm = ytms #替换原来的YTM,以后要注意把‘行权’/‘到期’这些情况考虑进去

#分析1：有多笔成交的债的成交价差
cf = A[A['code'].duplicated() == True] # 找到重复的代码，即每天有超过一笔交易的
cf[cf['code'].duplicated() == False]

cod = cf['code']
cod = cod.drop_duplicates()

cf1 = A[A['code'].isin(cod)]
cf2= A[A['code'].isin(cod)].groupby('code')
cf2.ytm.max() - cf2.ytm.min() #找出同一支债当天不同成交之间的最大价差

#分析2：不同评级成交占比
k = A.groupby('rating') # 把当天的企业债按评级分类
k.boxplot() #直接调用画箱形图

mm = k.size()
sizes = list(mm.values)
lables = list(mm.index)

plt.pie(sizes,labels = lables,autopct='%1.1f%%') #用plt根据不同评级成交数量画饼图

# filter(str.isdigit,s)

# [x for x in s if x.isdigit()]

# gg =(x for x in s if x.isdigit()) next(gg)

#分析3：找出所有不含权债根据他们画出Term Structure
criterion = A['term'].map(lambda x: x.find('+') == -1) #这是一个筛选DF的标准，根据标准返回的True来筛出。相似的可以尝试DF.filteR和DF.apply
pures = A[criterion]#即纯债部分