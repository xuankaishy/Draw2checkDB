#!/usr/bin/python
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
# import thread
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
class sample(object):
	"""docstring for ClassName"""
	def __init__(self):
		
		self.arg = None
		
	def add_del_key(self,new_record,source_key,redis_server ):
		item_redis = redis.Redis(host = redis_server.split(':')[0] ,
								 port = int(redis_server.split(':')[1]),
								 db = int(redis_server.split(':')[2]) )	
		modify_num = item_redis.llen(source_key)
		new_record_dict = json.loads(new_record)
		new_keys = new_record_dict.keys()
		old_keys = json.loads(item_redis.lindex(source_key,modify_num-1)).keys()
		
		whole_set = set(new_keys).union(old_keys)
		add_key = whole_set.difference(set(old_keys))
		del_key = whole_set.difference(set(new_keys))
		# new_key_num = len(new_keys)
		# old_key_num = len(old_keys)
		

		for i in range(modify_num): # 检查之前的每条记录并且修改
			# print i
			if add_key :	
				record = json.loads(item_redis.lindex(source_key,i))
				# key_num = len( record.keys() )
				
				for key in add_key : record[key] = new_record_dict[key]
				record = json.dumps(record)
				# print record
				item_redis.lset(source_key,i,record)
				print '{} : {} --  key : {} added.'.format(source_key,i,str( list(add_key) ) )
			
			if del_key:
			
				record = json.loads( item_redis.lindex(source_key,i) )
				# key_num = len( record.keys() )
				# pdb.set_trace()
				for key in del_key : 
					if key in record.keys(): del record[key]
				record = json.dumps(record)
				item_redis.lset(source_key,i,record)
				print '{} : {} -- key : {} deleted.'.format(source_key,i,str( list(del_key) ) )	

	def store2redis(self,new_record,source_name,redis_server = '127.0.0.1:6379:0' ):
		print source_name
		hour_key  = source_name + '_hour'
		day_key   = source_name + '_day'
		month_key = source_name + '_month'
		item_redis = redis.Redis(host = redis_server.split(':')[0] ,
								 port = int(redis_server.split(':')[1]),
								 db = int(redis_server.split(':')[2]) )		   						


		while item_redis.llen(hour_key) < 17280:
			item_redis.lpush(hour_key,new_record)
			print 'Filling redis --- {} : {}'.format(hour_key,new_record)
		
		item_redis.lpush(hour_key,new_record)
		print 'New_record inserted to redis --- {} : {}'.format(hour_key,new_record)
		while item_redis.llen(hour_key) > 17281:
			print 'Pop overflow : {}'.format(item_redis.rpop(hour_key))

		self.add_del_key(new_record,hour_key,redis_server)
		self.add_del_key(new_record,day_key,redis_server)
		self.add_del_key(new_record,month_key,redis_server)
		
		if item_redis.llen(hour_key) == 17281 : ## hour 表满 17280，从头部lpush进新的，从尾部rpop出最旧的
			day_in_candidate = new_record
			item_redis.rpop('hour')

			# pdb.set_trace()

			while item_redis.llen(day_key) < 30:
				item_redis.lpush(day_key,day_in_candidate)
				print 'Filling redis --- {} : {}'.format(day_key,day_in_candidate)
			
			## 下面判断是否需要更新 day表 和 month表
			
			day_in_record_date = json.loads(day_in_candidate)['datetime']
			day0_date = json.loads(item_redis.lindex(day_key,0) )['datetime']
			day_delta = time.mktime(time.strptime(day_in_record_date , '%Y-%m-%d  %H:%M:%S'))\
					  - time.mktime(time.strptime(day0_date , '%Y-%m-%d  %H:%M:%S'))
			day_update_flag = True if day_delta >= 86400 else False
			item_redis.lpush(day_key,day_in_candidate) if day_update_flag else None
			
			while item_redis.llen(day_key) > 31:
				print 'Pop overflow : {}'.format(item_redis.rpop(day_key))

			month_in_candidate = item_redis.lindex(day_key,0)
			while item_redis.llen(month_key) < 12:
				item_redis.lpush(month_key,month_in_candidate)
				print 'Filling redis --- {} : {}'.format(month_key,month_in_candidate)			
			
			if item_redis.llen(day_key) == 31 : ## day 表满 30，从头部lpush进新的，从尾部rpop出最旧的
				item_redis.rpop(day_key) 
				month_in_candidate = item_redis.lindex(day_key,0)
				# 
				month_in_record_date = json.loads(month_in_candidate)['datetime'] ## 判断月份是否相同
				print '\nA record pop from redis --- {} : {}\n'.format( day_key ,month_in_candidate)
				

				
				month0_date = json.loads(item_redis.lindex(month_key,0) )['datetime']
				month_delta = time.mktime(time.strptime(month_in_record_date , '%Y-%m-%d  %H:%M:%S'))\
				 		    - time.mktime(time.strptime(month0_date , '%Y-%m-%d  %H:%M:%S'))



				month_update_flag = True if month_delta >= 2592000 else False
				item_redis.lpush(month_key,month_in_candidate) if month_update_flag else None
				
				while item_redis.llen(month_key) > 12:
					print 'Pop overflow : {}'.format(item_redis.rpop(month_key))				
					## month 表满 13，从尾部rpop出最旧的




	def get_data(self, mongo_uri , redis_keys = None, redis_server = '127.0.0.1:6379:0' ):

		client = pymongo.MongoClient(mongo_uri.split('/')[0].split(':')[0], int(mongo_uri.split('/')[0].split(':')[1]) )
		
		record = {}
		# print mongo_uri
		# pdb.set_trace()
		use_db = mongo_uri.split('/')[1].split(':')[0]
		
		for collection in mongo_uri.split('/')[1].split(':')[1].split(','):
			record[collection+'_num'] = str(client[use_db][collection].find().count())
		

		
		if redis_keys:
			item_redis = redis.Redis(host = redis_server.split(':')[0] ,
									 port = int(redis_server.split(':')[1]),
									 db = int(redis_server.split(':')[2]) )
			for key in redis_keys:
				record[key+'_num'] = str(item_redis.scard(key))

		record["datetime"] = str( datetime.datetime.now().strftime('%Y-%m-%d  %H:%M:%S') )
		record = json.dumps(record)
		return record

store_redis_server = '127.0.0.1:6379:0' #host:port:db put in your redis server
# =============================================================
#  1  sample douban data
# =============================================================
search_redis_server = '127.0.0.1:6379:0' #host:port:db
redis_keys = ['key1','key2','key3']
mongo_uri = '127.0.0.1:27017/db_name:collection_name' #host:port:db:collection
source_name = 'db_name'
douban_obj = sample()

douban_record = douban_obj.get_data(mongo_uri,redis_keys,search_redis_server) 
douban_obj.store2redis(douban_record,source_name,store_redis_server) 


