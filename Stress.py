#!/usr/bin/env python2.7
import os
import sys
import time
#import subprocess
from subprocess import Popen,PIPE,STDOUT,call
from random import randint
from multiprocessing import Process, Queue

query_filename="query.txt"
build_filename="build.txt"
build_query_filename="build_query.txt"
drop_filename="drop.txt"

server="127.0.0.1"
mysql_port="3306"
restful_port="9090"
mysql_server_down_str = "ERROR 2003 (HY000): Can't connect to MySQL server on"

def ReadFile(filename):
	array = []
	with open(filename, 'rt') as myFile:
		for line in myFile:
			line_data = line.split('\n')[0]
			if line_data[0:2] != "//":
				#array.append(line.split('\n')[0])
				array.append(line_data)
	return array

def runMysqlCmd(cmdStr):
	now = time.time()
	full_cmd = "mysql -h " + server + " -P " + mysql_port + " -e \"" + cmdStr + "\""
	#print full_cmd
	proc=Popen(full_cmd, shell=True, stdout=PIPE,stderr=PIPE)
	#result=proc.communicate()[0]
	result,err=proc.communicate()

	proc.wait()
	end = time.time()

	if mysql_server_down_str in err:
		print "server down";
		return -1 , (end - now)
	#print result
	#return round(end - now)
	return 0 , (end - now)


#def runRestCmd(cmdStr):

def runCmdWorker(query_num, queue):
	build_in = -1;	
	q_count = 0;
	bq_count = 0;
	
	q_arr = ReadFile(query_filename)
	b_arr = ReadFile(build_filename)
	bq_arr = ReadFile(build_query_filename)
	d_arr = ReadFile(drop_filename)

	q_time_avg = 0.0
	bq_time_avg = 0.0
#	print " :::: " + str(len(bq_arr))

	res = 0
	build_flag = 0

	for q in range(0, int(query_num)):
		cmd_type = randint(0,int(query_num) / 4)
		#print "cmd_type : " , cmd_type
		if cmd_type == 0 and build_flag == 0:
			build_flag = 1
			#build case
			#print "run build statement"
			for b_cmd in b_arr:
				#print b_cmd
				runMysqlCmd(b_cmd)
			build_in = q
		else:
			#query
			if build_flag == 1 and len(bq_arr) > 0:
				#run query + build query
				query_type = randint(1,2)
				#print "query_type : " , query_type
				if query_type == 1 :
					#print "query case"
					q_r_index = randint(0, len(q_arr) - 1)
					res, runtime = runMysqlCmd(q_arr[q_r_index])
					q_time_avg = q_time_avg + runtime
					q_count = q_count + 1
				else:
					#print "build query case"
					bq_r_index = randint(0, len(bq_arr) - 1)
					res, runtime = runMysqlCmd(bq_arr[bq_r_index])
					bq_time_avg = bq_time_avg + runtime
					bq_count = bq_count + 1			
			else:
				#run query
				#print "query case : before build"
				q_r_index = randint(0, len(q_arr) - 1)
				res, runtime = runMysqlCmd(q_arr[q_r_index])
				q_time_avg = q_time_avg + runtime
				q_count = q_count + 1

			if res == -1:
				break;
			
	for d_cmd in d_arr:
		#print "run drop statement"
		runMysqlCmd(d_cmd)

#	for cmd in q_arr:
#		print cmd
#		runMysqlCmd(cmd)
	total_cmd = q_count + bq_count
	total_avg = (q_time_avg + bq_time_avg) / total_cmd
	if q_count != 0:
		q_time_avg = q_time_avg / q_count
	if bq_count != 0:
		bq_time_avg = bq_time_avg / bq_count

	
	#print "build in the " + str(build_in) + "th statement"
	#print "basic query : " + str(q_count) + ", avg : " + str(q_time_avg)
	#print "build query : " + str(bq_count) + ", avg : " + str(bq_time_avg)
	#queue.put("build in the " + str(build_in) + "th statement\n" + "basic query : " + str(q_count) + ", avg : " + str(q_time_avg) + "\n" + "build query : " + str(bq_count) + ", avg : " + str(bq_time_avg))
	queue.put("proc avg time:" + str(total_avg))
	return res

def runTest(num_proc, query_num):
	jobs = []
	queue = Queue()
	for num in range(num_proc):      
		p = Process(target=runCmdWorker, args=(query_num, queue))
		jobs.append(p)
		p.start()

	for j in jobs:
        	j.join()
		if j.exitcode != 0:
	        	print '%s.exitcode = %s' % (j.name, j.exitcode)

	print "============================================="
	total_avg = 0.0
	for i in range(queue.qsize()):
		result = queue.get()
		proc_avg = float(result.split(':')[1])
		total_avg = total_avg + proc_avg
		#print result
	print "============================================="
	print "process num :" + str(num_proc) +" , total avg : " + str(total_avg / num_proc)

if __name__ == "__main__":
	#server = sys.argv[1]
	max_proc = int(sys.argv[1])
	query_num = sys.argv[2]

	#runCmdWorker(query_num)
	for i in range(1, max_proc + 1):
		runTest(i, query_num)
	
	

