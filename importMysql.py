#!/usr/bin/python
# -*- coding: UTF-8 -*-
# coding: utf-8
import MySQLdb
import json
import time
import datetime
import os
import io
import sys
import commands
import logging
import re
#------------------------
# 公共配置 MySQL-python
#------------------------
mysqlDump = "/usr/bin/mysqldump"
mysqlBin = "/usr/bin/mysql"
sqlSavePath = "/mnt/hgfs/workspace1/julive/"
logSaveFilePath = "/mnt/hgfs/workspace1/julive/log.log"
cleanTable = "yw_periodic_clean_table"
#-------------------------
#mysql 配置
#-------------------------
mysqlHost="127.0.0.1"
mysqlpPrt=3306
mysqlUserName="root"
mysqlPassword="123456"
mysqlDbname="table"
mysqlDbset="--default-character-set=utf8 -A"
#-------------------------
#mysqldump 配置
#-------------------------
mysqlDumpHost="127.0.0.1"
mysqlDumppPrt=3306
mysqlDumpUserName="root"
mysqlDumpPassword="123456"
mysqlDumpDbname="table"
mysqlDumpDbset="--default-character-set=utf8 -A"
#-------------------------
#mysqlLog 配置
#-------------------------
mysqlLogHost="127.0.0.1"
mysqlLogpPrt=3306
mysqlLogUserName="root"
mysqlLogPassword="123456"
mysqlLogDbname="tables"
mysqlLogDbset="--default-character-set=utf8 -A"
#-------------------------
#mysqldel 配置
#-------------------------
mysqlDelHost="127.0.0.1"
mysqlDelpPrt=3306
mysqlDelUserName="root"
mysqlDelPassword="123456"
mysqlDelDbname="table"
mysqlDelDbset="--default-character-set=utf8 -A"
#导入日志库配置
#------------------------
logCleanTable = ['yw_message','yw_timing_sms','yw_notify','yw_async_compute_timing','cj_sms_log','yw_operation_log','yw_dingding_msg_send','yw_timing_push_msg','yw_push_receive_user']
# 4个进程处理
worker_num = 4
#任务分配资源
job_source = []
#yw_periodic_clean_table的静态配置
type_arr = {1:"years",2:"months",3:"weeks",4:"days"}
#任务记录文件
taskHistoryRecord = "record.txt"
#分页查询
page_size = 500
#超过多少条分页
maxPageSize = 10000
#当前页，主要用于手动执行，从第几页开始
currentPage = -1
#是否开启导出后就删除
isImportDel = True
#存储id的list
idList = []

def old_file_yield(old_file_path):
    with open(old_file_path,'r') as  oldf:
      while True:
         line=oldf.readline()
         yield line
         if not line:
            oldf.close()
            break

def replace_str(file,oldfile,old_str,new_str):
	global idList
	with io.open(file,"w",encoding="utf-8") as f :
		for line in old_file_yield(oldfile):
			if old_str in line:
				line = line.replace(old_str,new_str)
	    		if line != None and line != False:
	    			if isImportDel:
	    				idTmp = re.findall(r'VALUES\s*\((\d+)+,*', unicode(line))
	    				if len(idTmp) :
	    					idList.append(idTmp[0])
	    			f.write(unicode(line))
	    		else :
	    			return False
    	f.close()
    	logging.info("把临时文件%s的INSERT INTO以及表替换REPLACE INTO以及表写入新的文件%s" % (oldfile,file) )
    	return True
 
def file_name(file_dir):   
	for root, dirs, files in os.walk(file_dir): 
		return (files)

def file_extension(file):
	return os.path.splitext(file)[1]

def recordTask(sid):
	path = os.getcwd()
	if os.path.exists(path):
		recordFile = path + '/' + taskHistoryRecord
		if os.path.exists(recordFile):
			modifiedTime = time.strftime('%Y%m%d',time.localtime(os.stat(recordFile).st_mtime))
			if modifiedTime == datetime.datetime.now().strftime('%Y%m%d'):
				with io.open(recordFile,"w+t",encoding="utf-8") as f:
					line = f.readline()
					if not line :
						f.write(unicode(str(sid)))
						f.close()
						return True
					elif int(line) >= int(sid) :
						return False
					else :
						f.write(unicode(str(sid)))
					return True
			else :
				return True
		else :
			with io.open(recordFile,"w",encoding="utf-8") as f:
				f.write(unicode("0"))
			logging.error("获取记录文件失败")
	else :
		logging.error("获取当前记录文件目录失败")
	return True
	


def getWhere(field_other):
	and_where_str = ''
	if field_other !=None and field_other :
		fieldOther = json.loads(field_other)
		for item in fieldOther:
			if len(fieldOther[item])==1 and fieldOther[item] != None:
		   		and_where_str += " and " + item + "=" + fieldOther[item][0]
		   		break
			elif fieldOther[item] != None and fieldOther[item]:
		   		and_where_str += " and " + item + " in (" + ','.join(str(i) for i in fieldOther[item]) + ")"

	return and_where_str

def getCount(table_name,where):
	sql = "select count(*) from " + table_name +" where " + where
	db = MySQLdb.connect(mysqlDumpHost,mysqlDumpUserName,re.escape(mysqlDumpPassword),mysqlDumpDbname, charset='utf8' )
	cursor = db.cursor()
	try:
		cursor.execute(sql)
		results = cursor.fetchone()
		cursor.close()
		db.close()
		return results[0]
	except Exception as e:
		print e
		logging.error("发生错误，error:{0}".format(e))
	 	cursor.close()
		db.close()
		sys.exit()

def exportSql(args_of_job):
	if args_of_job !=None :
		now = datetime.datetime.now()
		if type_arr[args_of_job['data']['type']] == "days":
			dateTimeTmp = now + datetime.timedelta(days = - args_of_job['data']['long'])
		if type_arr[args_of_job['data']['type']] == "years":
			dateTimeTmp = now + datetime.timedelta(days = - args_of_job['data']['long']*365)
		if type_arr[args_of_job['data']['type']] == "months":
			dateTimeTmp = now + datetime.timedelta(days = - args_of_job['data']['long']*365/12)
		if type_arr[args_of_job['data']['type']] == "weeks":
			dateTimeTmp = now + datetime.timedelta(weeks = - args_of_job['data']['long'])
		t = dateTimeTmp.timetuple()
		timeStamp = int(time.mktime(t))
		#timeStamp = float(str(timeStamp) + str("%d" % dateTimeTmp.microsecond))/1000000
		sql = args_of_job['data']['field_datetime'] + " < " + str(timeStamp)
		sql += args_of_job['where']
		count = getCount(args_of_job['data']['table_name'],sql)
		logging.info("当前获取到该%s的总共有%d条数据"%(args_of_job['data']['table_name'],count))
		if count > maxPageSize :
			page = count / page_size
			page1 = count % page_size
			cpage = 0
			if page1 != 0:
				page = page + 1
			if currentPage != -1 :
				cpage = currentPage
			countDel = count
			for p in range(cpage,page) :
				while True:
					try:
						sqlTmpStr = sql
						if isImportDel :
							logging.info("当前开启了导入后删除，该分页下获取到该%s的总共有%d条数据"%(args_of_job['data']['table_name'],countDel))
							if countDel <= 0 :
								return True
							sqlTmpStr += ' limit 0,' + str(page_size)
							countDel = countDel - page_size
							excuteResult = excutePageMysqldump(args_of_job,sqlTmpStr,now,p+1,count,True)
						else :
							logging.info("开始"+ args_of_job['data']['table_name'] +" 的第," + str(p + 1) +" 页查询")
							if p == 0:
								sqlTmpStr += ' limit ' + str(p * page_size) + ',' + str(page_size)
							else:
								sqlTmpStr += ' limit ' + str(p * page_size + 1) + ',' + str(page_size)
							excuteResult = excutePageMysqldump(args_of_job,sqlTmpStr,now,p+1,count)
						if  excuteResult == True:
							logging.info("当前执行完该%s的第 %d 页执行完成 "%(args_of_job['data']['table_name'],p+1))
							break
						else:
							return excuteResult
					except Exception as e:
						print e
						logging.error("发生错误，error:{0}".format(e))
						sys.exit()
		else:
			return excuteMysqldump(args_of_job,sql,now)

def excutePageMysqldump(args_of_job,sql,now,pages,count,delag = False):
	global idList
	page = count / page_size
	page1 = count % page_size
	if page1 != 0:
		page = page + 1

	saveTmpFile = sqlSavePath + args_of_job['data']['table_name'] + "-" + str(pages) + "-tmp.sql"
	cmd = mysqlDump + " --replace --compact  --skip-create-options --single-transaction --skip-extended-insert --skip-lock-tables --default-character-set=utf8 -h%s -u%s -P%d -p%s -t %s %s --where=\"%s\"  --triggers=false > %s" % (mysqlDumpHost,mysqlDumpUserName,mysqlDumppPrt,re.escape(mysqlDumpPassword),mysqlDumpDbname,args_of_job['data']['table_name'],sql,saveTmpFile)
	saveFile = sqlSavePath + args_of_job['data']['table_name'] + "-" + str(pages) + "-" + now.strftime('%Y%m%d') + ".sql"
	logging.info("mysqldump执行命令，cmd为：%s" % (mysqlDump + " --replace --compact  --skip-create-options --single-transaction --skip-extended-insert --skip-lock-tables --default-character-set=utf8 -t %s %s --where=\"%s\"  --triggers=false > %s" % (mysqlDumpDbname,args_of_job['data']['table_name'],sql,saveTmpFile)))
	# if recordTask(args_of_job['data']['sid']):
	status,output = commands.getstatusoutput(cmd)
	if isImportDel:
		idList = []
	if status != 0:
		logging.error("mysqldump导出 执行失败，原因:%s" % output)
		sys.exit()
	if os.access(saveFile,os.F_OK) :
		os.remove(saveFile)
	if saveTmpFile and replace_str(saveFile,saveTmpFile,"REPLACE INTO `" + args_of_job['data']['table_name'] + "`" ,"REPLACE INTO `" + args_of_job['data']['table_name'] + "_copy_clean`"):
		os.remove(saveTmpFile)
		logging.info("删除临时文件：%s" % saveTmpFile)
		# else :
			# os.rename(saveTmpFile,saveFile)
	if page > pages and count != 0 :
		if isImportDel:
			idListStr = ','.join(idList)
			limitStr = sql.find('limit')
			if limitStr != -1 :
				sql = sql[:limitStr]
				if idList and importData(saveFile,args_of_job['data']['table_name']) :
					if deletePageData(sql,args_of_job['data']['table_name'],idListStr):
						return True
		else :
			return importData(saveFile,args_of_job['data']['table_name'])
	elif delag and isImportDel:
		logging.info("导入后开启删除执行到最后一页数据")
		idListStr = ','.join(idList)
		limitStr = sql.find('limit')
		if limitStr != -1 :
			sql = sql[:limitStr]
			if idList and importData(saveFile,args_of_job['data']['table_name']) :
				if deletePageData(sql,args_of_job['data']['table_name'],idListStr):
					return True
	else :
		limitStr = sql.find('limit')
		if limitStr != -1 :
			sql = sql[:limitStr]
		logging.info("当前该执行该%s的第 %d 页的数据 "%(args_of_job['data']['table_name'],pages))
		return saveFile,sql,args_of_job['data']['table_name'],args_of_job['data']['sid']

def excuteMysqldump(args_of_job,sql,now):
	saveTmpFile = sqlSavePath + args_of_job['data']['table_name'] + "-tmp.sql"
	cmd = mysqlDump + " --replace --compact  --skip-create-options --single-transaction --skip-extended-insert --skip-lock-tables --default-character-set=utf8 -h%s -u%s -P%d -p%s -t %s %s --where=\"%s\"  --triggers=false > %s" % (mysqlDumpHost,mysqlDumpUserName,mysqlDumppPrt,re.escape(mysqlDumpPassword),mysqlDumpDbname,args_of_job['data']['table_name'],sql,saveTmpFile)
	saveFile = sqlSavePath + args_of_job['data']['table_name'] + "-" + now.strftime('%Y%m%d') + ".sql"
	logging.info("mysqldump执行命令，cmd为：%s" % (mysqlDump + " --replace --compact  --skip-create-options --single-transaction --skip-extended-insert --skip-lock-tables --default-character-set=utf8 -t %s %s --where=\"%s\"  --triggers=false > %s" % (mysqlDumpDbname,args_of_job['data']['table_name'],sql,saveTmpFile)))
	if recordTask(args_of_job['data']['sid']):
		status,output = commands.getstatusoutput(cmd)
		if status != 0:
			logging.error("mysqldump导出 执行失败，原因:%s" % output)
			sys.exit()
		if os.access(saveFile,os.F_OK) :
			os.remove(saveFile)
		if saveTmpFile and replace_str(saveFile,saveTmpFile,"REPLACE INTO `" + args_of_job['data']['table_name'] + "`" ,"REPLACE INTO `" + args_of_job['data']['table_name'] + "_copy_clean`"):
			os.remove(saveTmpFile)
			logging.info("删除临时文件：%s" % saveTmpFile)
		# else :
			# os.rename(saveTmpFile,saveFile)
	return saveFile,sql,args_of_job['data']['table_name'],args_of_job['data']['sid']


def importData(saveFile,table_name):
	if saveFile !=None :
		if table_name in logCleanTable:	
			cmd = mysqlBin + " --default-character-set=utf8 -h%s -u%s -P%d -p%s %s < %s " % (mysqlLogHost,mysqlLogUserName,mysqlLogpPrt,re.escape(mysqlLogPassword),mysqlLogDbname,saveFile)
		else:
			cmd = mysqlBin + " --default-character-set=utf8 -h%s -u%s -P%d -p%s %s < %s " % (mysqlDelHost,mysqlDelUserName,mysqlDelpPrt,re.escape(mysqlDelPassword),mysqlDelDbname,saveFile)
		logging.info("导入数据到表里，cmd为：%s" % (mysqlBin + " --default-character-set=utf8 %s < %s " % (mysqlDelDbname,saveFile)))
		status,output = commands.getstatusoutput(cmd)
		if status != 0:
			logging.error("mysql导入执行失败，原因:%s" % output)
			sys.exit()
	return True

def deletePageData(sql,table_name,idListStr):
	global idList
	if sql !=None and table_name !=None :
		sql = 'delete from `' + table_name + '` where ' + sql + " and id in(" + str(idListStr) +")"
		logging.info("分页删除源数据，sql为：%s" % sql)
		db = MySQLdb.connect(mysqlDelHost, mysqlDelUserName, mysqlDelPassword, mysqlDelDbname, charset='utf8' )
		cursor = db.cursor()
		cursor.execute(sql)
		cursor.close()
		db.commit()
		db.close()
	return True

def deleteData(sql,table_name):
	if sql !=None and table_name !=None :
		sql = 'delete from `' + table_name + '` where ' + sql
		logging.info("删除源数据，sql为：%s" % sql)
		db = MySQLdb.connect(mysqlDelHost, mysqlDelUserName, mysqlDelPassword, mysqlDelDbname, charset='utf8' )
		cursor = db.cursor()
		cursor.execute(sql)
		cursor.close()
		db.commit()
		db.close()
	return True

def addJob(data,and_where_str):
	tmpJob = {}
	tmpJob['data'] = data
	tmpJob['where'] = and_where_str
	job_source.append(tmpJob)
	return True

def deleteJob(table_name):
	for index,job in enumerate(job_source):
		if job['data']['table_name'] == table_name :
			job_source.remove(job)
	
	if len(job_source)==0 :
		sys.exit()
	return True

def createTable(table_name):
	if table_name !=None :
		sql = 'SHOW TABLES LIKE "' + table_name + '_copy_clean"'
		logging.info("查看%s_copy_clean 是否存在" % table_name)
		if table_name in logCleanTable:
			db = MySQLdb.connect(mysqlLogHost, mysqlLogUserName, mysqlLogPassword, mysqlLogDbname, charset='utf8' )
		else:
			db = MySQLdb.connect(mysqlDelHost, mysqlDelUserName, mysqlDelPassword, mysqlDelDbname, charset='utf8' )
		cursor = db.cursor()
		if cursor.execute(sql) == 1:
			return True
		else :	 
			sql = 'SHOW CREATE TABLE ' + table_name
			logging.info("%s_copy_clean 表不存在，查看createTable语句,sql为：%s" % (table_name,sql))
			dbOld = MySQLdb.connect(mysqlHost, mysqlUserName, mysqlPassword, mysqlDbname, charset='utf8' )
			cursorOld = dbOld.cursor()
			cursorOld.execute(sql)
			results = cursorOld.fetchall()
			for row in results:
				tableSql = row[1].replace(table_name,table_name + '_copy_clean')
				logging.info("%s_copy_clean 表不存在，获取createTable语句：%s" % (table_name,tableSql))
				cursor = db.cursor()
				cursor.execute(tableSql)
				tableSqlAuto = 'ALTER TABLE ' + table_name + '_copy_clean auto_increment = 1'
				logging.info("%s_copy_clean 表重置自增id，sql为：%s" % (table_name,tableSqlAuto))
				cursor = db.cursor()
				cursor.execute(tableSqlAuto)
				db.commit()
			cursorOld.close()
			dbOld.close()
		cursor.close()
		# db.commit()
		db.close()
	return True


def optimizeTable(table_name):
	now = datetime.datetime.now()
	if now.weekday() == 2:
		sql = 'optimize table `' + table_name + '` '
		logging.info("周三执行优化表，sql：%s：" % sql)
		db = MySQLdb.connect(mysqlDelHost, mysqlDelUserName, mysqlDelPassword, mysqlDelDbname, charset='utf8' )
		cursor = db.cursor()
		cursor.execute(sql)
		cursor.close()
		db.commit()
		db.close()
	return True

def foreman(worker_num):
	pipeline = create_pipeline(worker_num)
	args_of_ceate_jobs = None
	for i,job in enumerate(get_jobs(args_of_ceate_jobs),start=0):
		worker_id  = i % worker_num
		pipeline.send((job,worker_id))

def coroutine(func):
    def warper(*args):
        f = func(*args)
        f.next()
        return f
    return warper

@coroutine
def worker(pipeline,accepting,job,my_id):
    while True:
        args_of_job, worker_id = (yield )
       	if worker_id == my_id :
        	result = work(args_of_job)
        	accepting.send(result)
        else :
            pipeline.send((args_of_job,worker_id))
        	

@coroutine
def accept():
    while True:
        result = (yield )
        if result != None and result != True and importData(result[0],result[2]):
        	logging.info("当前执行完该%s的所有数据导入执行完成 "%result[2])
        	deleteData(result[1],result[2])
        	#回收工作
        	#deleteJob(result[2])
        	#优化表
        	optimizeTable(result[2])
        	#记录当然的任务
        	recordTask(result[3])
        	logging.info("执行完当前单个任务，任务数据为：{0}：".format(result))
        #do_some_accepting

def create_pipeline(worker_num):
    pipeline = None
    job = None
    args_of_ceate_jobs = None
    accepting = accept()
    for work_id in  range(worker_num):
        pipeline = worker(pipeline,accepting,job,work_id)
    return pipeline

def get_jobs(args_of_ceate_jobs):
	for job in job_source:
		yield job

def work(args_of_job):
	if args_of_job != None:
		if createTable(args_of_job['data']['table_name']) :
			return exportSql(args_of_job)
    #do_some_work

def getALLJobs(table_name = ''):
	nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	logging.info("===============" + nowTime + "begin Task==================")
	if table_name != '':
		sql="select * from " + cleanTable + " where status = 0 and table_name = '" + table_name + "';"
	else:
		sql="select * from " + cleanTable + " where status = 0 ;"
	db = MySQLdb.connect(mysqlHost, mysqlUserName, mysqlPassword, mysqlDbname, charset='utf8' )
	cursor = db.cursor()
	try:
		cursor.execute(sql)
		results = cursor.fetchall()
		for row in results:
			importData = {}
			importData['sid'] = int(row[0])
			importData['table_name'] = row[5]
			importData['type'] = row[7]
			importData['long'] = row[8]
			importData['field_datetime'] = row[9]
			importData['field_other'] = row[10]
			and_where_str = getWhere(importData['field_other'])
			if importData !=None :
				addJob(importData,and_where_str)
				logging.info("添加任务：addJob({0},{1})".format(importData,and_where_str))
		foreman(worker_num)
		cursor.close()
		db.close()
	except Exception as e:
		print e
		logging.error("发生错误，error:{0}".format(e))
	 	cursor.close()
		db.close()
		sys.exit()

def main():
	if sys.getdefaultencoding() != 'utf-8':
		reload(sys)
		sys.setdefaultencoding('utf-8')
	logging.basicConfig(
		filename=logSaveFilePath,
		format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
		datefmt = '%a, %Y-%m-%d %H:%M:%S',
		level=logging.INFO
	)
	if len(sys.argv) > 3:
		logging.error("参数错误，最多支持一个参数")
		sys.exit()
	elif len(sys.argv) == 2 and sys.argv[1] !='':
		getALLJobs(sys.argv[1])
		logging.info("今天当前的任务完成")
	elif len(sys.argv) == 3 and sys.argv[1] !='' and sys.argv[2] !='':
		global currentPage
		currentPage = int(sys.argv[2])
		getALLJobs(sys.argv[1])
		logging.info("今天当前的任务完成")
	else :
		getALLJobs()
	logging.info("今天的任务完成：完成时间为：%s" % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
)

if __name__ == '__main__':
	main()
