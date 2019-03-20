#coding:utf-8
import matplotlib
matplotlib.use('Agg')  
import os
import subprocess
from PIL import Image
from PIL import ImageOps
import time,os,sys
import urllib
import urllib2
import pdb
import re
import thread
import random
import string
import sys
import urllib
import urllib2
import re
import codecs
import json
import hashlib
import lxml
import chardet
from scrapy.spiders import Spider  
from scrapy.selector import Selector  
from scrapy.http.request import Request 
import urllib2,re,os,datetime
from selenium import webdriver
import pymongo
from pymongo import MongoClient 
import redis
import time
from pyvirtualdisplay import Display 
from lxml import etree
from lxml import html
import matplotlib.pyplot as plt
import numpy as np
import datetime
import math
def plot_item(x,xx,x_length,item_id_nums,figsize,draw_type,source_name,key):
	plt.figure(figsize=figsize)

	plt.plot(x,item_id_nums[-x_length:] ,'bo-',label= source_name + ' : ' + key) # in 'bo-', b is blue, o is O marker, - is solid line and so on
	cnt = 0
 	if draw_type == 'hour':
 		plt.title('Data for the past 12 hours')
 		odd_flag = False
 	elif draw_type == 'day':
 		plt.title('Data for the past 15 days')
 		odd_flag = True
 	elif draw_type == 'month':
 		odd_flag = False
 		plt.title('Data for the past 12 month')

 	remainder = 0 if odd_flag else 1 # to assure the last point has a notation on it 
	for a,b in zip( x , item_id_nums[-x_length:] ) :  
		if cnt % 2 == remainder:
			plt.text(a, float(b), '%.0f' % float(b) , ha='center', va= 'bottom' , fontsize=11)
		cnt += 1
 	plt.xticks(np.arange(x_length),xx)
 	plt.ticklabel_format(axis='y', style='plain')
 	# plt.ticklabel_format(axis='y', style='sci', scilimits=(-2,2))

 	# pdb.set_trace()

	plt.legend()
	plt.savefig(source_name + '_' + draw_type+ '_' + key +'.png',dpi=100)

def draw_hour(source_name,redis_server):
	## past 12 hours
	item_redis = redis.Redis(host = redis_server.split(':')[0] ,
							 port = int(redis_server.split(':')[1]),
							 db   = int(redis_server.split(':')[2]) )		# pdb.set_trace()
	x_length = 12
	key_name = source_name + '_hour'

	index_list   = [ each*720 for each in range(x_length) ]
	# print index_list
	hour_records = [ item_redis.lindex(key_name,each) for each in index_list ]
	hour_records = [ json.loads(each) for each in hour_records ]

	keys 		 = list(hour_records[0].keys())
	x 			 = np.arange(x_length)
	figsize 	 = (10,7)
	draw_type 	 = 'hour'
	dates		 = [ each['datetime'].split('  ')[1] for each in hour_records ]
	dates 		 = [ each.split(':')[0]+':'+each.split(':')[1] for each in dates ]
	dates.reverse()
	for key in keys:
		if key == 'datetime' : continue
		y = [ each[key] for each in hour_records ]
		y.reverse()
		plot_item(x,dates,x_length,y,figsize,draw_type,source_name,key)


def draw_day( source_name ,redis_server):
	## past 30 days
	x_length = 15
	key_name = source_name + '_day'
	item_redis = redis.Redis(host = redis_server.split(':')[0] ,
							 port = int(redis_server.split(':')[1]),
							 db = int(redis_server.split(':')[2]) )	

	day_records = item_redis.lrange(key_name,0,x_length-1)
	day_records = [ json.loads(each) for each in day_records ]
	


	keys 		 = list(day_records[0].keys())	
	x 			 = np.arange(x_length)
	figsize 	 = (13,7)
	draw_type 	 = 'day'
	dates 	     = [ each['datetime'].split(' ')[0] for each in day_records ]
	dates        = [ each.split('-')[1]+'-'+each.split('-')[2] for each in dates ]
	dates.reverse()
	for key in keys:
		if key == 'datetime' : continue
		y = [ each[key] for each in day_records ]
		y.reverse()
		plot_item(x,dates,x_length,y,figsize,draw_type,source_name,key)



def draw_month(source_name ,redis_server):
	## past 12 month
	x_length = 12
	key_name = source_name + '_month'
	item_redis = redis.Redis(host = redis_server.split(':')[0] ,
							 port = int(redis_server.split(':')[1]),
							 db = int(redis_server.split(':')[2]) )	

	month_records = item_redis.lrange(key_name,0,x_length-1)
	month_records = [ json.loads(each) for each in month_records ]
	


	keys 		 = list(month_records[0].keys())	
	x 			 = np.arange(x_length)
	figsize 	 = (10,7)
	draw_type 	 = 'month'
	dates 	     = [ each['datetime'].split(' ')[0] for each in month_records ]
	dates        = [ each.split('-')[0]+'-'+each.split('-')[1] for each in dates ]
	dates.reverse()
	for key in keys:
		if key == 'datetime' : continue
		y = [ each[key] for each in month_records ]
		y.reverse()
		plot_item(x,dates,x_length,y,figsize,draw_type,source_name,key)	

def draw(source_name,draw_type, redis_server):
	if draw_type == 'hour':
		draw_hour(source_name , redis_server)
	elif draw_type == 'day':
		draw_day(source_name ,redis_server)
	elif draw_type == 'month':
		draw_month(source_name ,redis_server)


redis_server = '' # put in your RedisDB server 
source_name = ''  # put in your collection name in MongoDB 
draw_type = 'day' # choose the type of draw you want to see

draw(source_name ,draw_type,redis_server)



