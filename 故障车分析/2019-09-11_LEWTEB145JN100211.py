#!/usr/bin/env python
# coding: utf-8

get_ipython().magic('matplotlib notebook')

import json
import datetime
import pandas as pd
from numpy import array
import numpy as np
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib.figure as fig
from pyspark.sql import *


data=spark.read.json("hdfs://cdh1:8020/user/hbase/pro_dfjk4new/fault_data/2019-06-11/")
data.collect()

# 电压趋势图
data1=data.toPandas()
data2=pd.DataFrame(data1[['obd_time','voltage']])
data2['voltage']=data2['voltage'].astype('double')
data2['obd_time'] = pd.to_datetime(data2['obd_time'])
data2.set_index('obd_time',inplace=True)
_x = data2.index
_y = data2.values
plt.figure(figsize=(100,10),dpi=80)
plt.title('电压变化图')
plt.plot(range(len(_x)),_y,color='r')
plt.xticks(range(len(_x)),_x,rotation=90)
plt.show()

# 电流变化图
data3=pd.DataFrame(data1[['obd_time','current']])
data3['current']=data3['current'].astype('double')
data3['obd_time'] = pd.to_datetime(data3['obd_time'])
data3.set_index('obd_time',inplace=True)
_x = data3.index
_y = data3.values
plt.figure(figsize=(100,10),dpi=80)
plt.title('电流变化图')
plt.plot(range(len(_x)),_y,color='r')
plt.xticks(range(len(_x)),_x,rotation=90)
plt.show()

# 最高温度变化图
data4=pd.DataFrame(data1[['obd_time','maximum_temperature_value']])
data4['maximum_temperature_value']=data4['maximum_temperature_value'].astype('double')
data4['obd_time'] = pd.to_datetime(data4['obd_time'])
data4.set_index('obd_time',inplace=True)
_x = data4.index
_y = data4.values
plt.figure(figsize=(100,11),dpi=80)
plt.title('最高温度变化图')
plt.plot(range(len(_x)),_y,color='g')
plt.xticks(range(len(_x)),_x,rotation=90)
plt.show()

# 最低温度变化图
data5=pd.DataFrame(data1[['obd_time','minimum_temperature_value']])
data5['minimum_temperature_value']=data5['minimum_temperature_value'].astype('double')
data5['obd_time'] = pd.to_datetime(data5['obd_time'])
data5.set_index('obd_time',inplace=True)
_x = data5.index
_y = data5.values
plt.figure(figsize=(100,10),dpi=80)
plt.title('最低温度变化图')
plt.plot(range(len(_x)),_y,color='b')
plt.xticks(range(len(_x)),_x,rotation=45)
plt.show()

