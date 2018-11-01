#!/usr/bin/env python
#  -*- coding:utf-8 -*-
############
# 
'''
输入最小超时阈值（小时）任务的队列资源占比危险阈值(0~1） 任务的集群资源占比危险阈值(0~1），然后超过任何一个阈值的已经超时还未完成的任务，杀掉，并且记录任务相关的信息便于后续审计。
'''
import os
import sys
import cmd
import string
import time
import json
import getopt
import re
import random
import urllib
import urllib2


def needToKill(app, qpt, cpt):
	needToKill = False
	queueUsagePercentage = float(app["queueUsagePercentage"])/100
	clusterUsagePercentage = float(app["clusterUsagePercentage"])/100
	if queueUsagePercentage>qpt or  clusterUsagePercentage >cpt:
		needToKill =  True
		print str(overtime_limit)+ " hour overtime task:"+ appid +" has occupied " +  str(queueUsagePercentage) + " queue resource and " + str(clusterUsagePercentage) + " cluser resource, need to be killed"

	return needToKill

#http://bxzj-test-swift0.bxzj.baixinlocal.com:8088/ws/v1/cluster/apps?limit=3&state=RUNNING&startedTimeBegin=1540361028371
#http://bxzj-test-swift0.bxzj.baixinlocal.com:8088/ws/v1/cluster/apps?state=RUNNING&startedTimeBegin=1540882344576&startedTimeEnd=1540889544576
if len(sys.argv)<4:
	print ' exec [overtime_limit(hour)]  [queue percentage threshold(0~1.0)] [cluster percentage threshold(0~1.0)] '
	exit(1)
t = time.time() 
current_time=(long(round(t * 1000))) 
print current_time
overtime_limit = float(sys.argv[1]) #hour
qpt = float(sys.argv[2])
cpt = float(sys.argv[3])


startedTimeEnd = "startedTimeEnd="+str(current_time - long(overtime_limit*60*60*1000))
startedTimeBegin = "startedTimeBegin="+str(current_time - long(24*60*60*1000))

rm_api= "http://bxzj-test-swift0.bxzj.baixinlocal.com:8088/ws/v1/cluster/apps?"

app_condition= "state=RUNNING" 
#app_condition = "finalStatus=UNDEFINED"
url = rm_api + app_condition + "&" + startedTimeBegin + "&" + startedTimeEnd
print url
req = urllib2.Request(url)
#print req

res_data = urllib2.urlopen(req)
res = res_data.read()
#print res

req_json = json.loads(res)

for app in req_json["apps"]["app"]:
	appid = app["id"]
	if needToKill(app, qpt, cpt):		
		shell_str = "yarn application -status " +  appid
		print shell_str
		print os.system(shell_str)
		print  appid + "` killed"


