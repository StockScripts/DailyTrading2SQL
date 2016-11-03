#! /usr/bin/python
# -*- coding: utf-8 -*-

'''
This is a py3.4 version.

Created on ${20160926}
 
@author: ${ChenCHEN}
'''

import pandas as pd

import matplotlib.pyplot as plt

import seaborn as sns

import pymysql as pms

import re

from datetime import datetime, timedelta

from WindPy import *


def get_data_from_sql(cur,bond_type):
	cj_sql = 'select * from bond_data2 where type2 like \'%' + bond_type + '''%\' and (rating = \'AA\' or rating = \'AA+\') 
	and (trading_date between \'20160926\' and \'20160926\' )'''
	cur.execute(cj_sql)
	cj = cur.fetchall()
	#存入DF
	cj_list = [list(x) for x in cj]#
	cj_list = [x[1:] for x in cj] #构建DataFrame只能用list来构建，tuple不行
	records = pd.DataFrame(cj_list,columns = ['code','name','term','rating','ytm','type1','type2','trading_date'])#读取所有成交数据到DF
	return records


zhPattern = re.compile(u'[\u4e00-\u9fa5]+') #用来判断一段文本中是否包含简体中文的pattern

#YTM处理
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

#数据库读入
conn = pms.connect('localhost', 'root', '123456', 'bond01',charset='utf8')#链接数据库
cur = conn.cursor()#获取游标

type_dict= {'企业债':(1,[6.5,7]),'中期票据':(0,[4.5,5]),'定向工具':(0,[0,10]),'私募债':(0,[0,10])}
#不同种类债的词典，元组里面前面一项是筛城投的判断标志，后面是筛期限的范围
 
public_bonds = pd.DataFrame(columns= ['name','rating','ytm','term','ytm_type'])
private_bonds = public_bonds #分别用来存储企业债+中票和定向工具+私募的最终数据 


for tp,attributes in type_dict.items():
	one_bond = 	get_data_from_sql(cur,tp)
	
	#先处理ytm和ytm类别
	t = one_bond.ytm
	ytm_list = list(map(getytm,list(t))) #处理后得到的浮点数形式的ytm
	ytms = [x[0] for x in ytm_list]
	ytm_types = [x[1] for x in ytm_list]
	one_bond.ytm = ytms #替换原来的YTM,以后要注意把‘行权’/‘到期’这些情况考虑进去
	one_bond['ytm_type'] = ytm_types #储存着行权/到期这些信息

	#Term 处理为剩余N年格式,CT为判断是否城投(凡是需要涉及WIND处理的全部在这一段里进行)
	ct_flag= [] #存储万得“是否城投债”数据
	option_years = [] #存储距离行权年份
	maturity_years = [] #存储距离到期年份
	real_terms = []
	w.start()
	bond_shape = one_bond.shape
	bond_num = bond_shape[0]
	for i in range(bond_num): #此段用于根据万得的到期日算出以年为单位的TERM,注意此处含权债问题未考虑
		code = one_bond.iloc[i]['code']
		tr_date = one_bond.iloc[i]['trading_date']
		mtd=w.wss(code,'maturitydate,municipalbond,termifexercise,ptmyear',"tradeDate=%s"%tr_date).Data

		# cday = datetime.strptime(tr_date, '%Y%m%d')
		# delta1 = mtd[0][0] - cday
		# maturity_years.append(delta1.days/365)
		maturity_years.append(mtd[3][0])
		option_years.append(mtd[2][0])
		if one_bond.iloc[i]['ytm_type'] == '行权':
			real_terms.append(mtd[2][0])
		else : 
			real_terms.append(mtd[3][0])
		ct_flag.append(mtd[1][0])
		
	one_bond['real_term'] = real_terms # 考虑到行权问题之后的实际term
	one_bond['maturity_year'] = maturity_years
	one_bond['option_year'] = option_years
	one_bond['CT'] = ct_flag
	w.stop()

	if attributes[0] == 1 :
		one_bond_CT= one_bond[one_bond['CT'] == '是']
	else :
		one_bond_CT= one_bond #one_bond_CT就是按照要求筛选城投（只有企业债要筛）之后的债

	del one_bond_CT['CT']

	#筛出剩余期限在要求范围内的
	one_bond_filtered = one_bond_CT[(one_bond_CT['real_term'] < attributes[1][1]) & (one_bond_CT['real_term'] > attributes[1][0])] 
	useful_info = one_bond_filtered.loc[:,['name','rating','ytm','term','ytm_type']]

	#下面要剔除同一笔债同样ytm的成交
	no_dupli = useful_info[useful_info[['name','ytm']].duplicated() == False] #也可直接用df.drop_duplicates()

	if tp == '企业债' or tp == '中期票据' :
		public_bonds = public_bonds.append(no_dupli)
	else :
		private_bonds = private_bonds.append(no_dupli)



conn.close()

public_bonds.sort(['rating','name'],inplace=True)#按照先rating后name的顺序进行排序，也可用df.sort_index(by = ['rating','name']) 
public_bonds.reset_index(drop=True, inplace=True)


private_bonds.sort(['rating','name'],inplace=True)
private_bonds.reset_index(drop=True, inplace=True)

#如何改变列的位置
#1 先加到某一列，再删了 one_bond.insert(0,'mean',one_bond.rating)

#2 先提取列名改变顺序再重新取值
cols = public_bonds.columns.tolist()
cols.remove('rating')
cols.append('rating')
public_bonds = public_bonds[cols]

cols = private_bonds.columns.tolist()
cols.remove('rating')
cols.append('rating')
private_bonds = private_bonds[cols]

#输出
filename1 = 'C:/Users/chenchen/Desktop/企业债+中票.csv'
public_bonds.to_csv(filename1)

filename2 = 'C:/Users/chenchen/Desktop/非公开+私募.csv'
private_bonds.to_csv(filename2)