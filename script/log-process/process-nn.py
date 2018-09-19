#!/usr/bin/env python3

import sys
import os
from pprint import pprint

def processline(line, dic):
	lis = line.split("@")
	dic[lis[1]] = (int(lis[2]),
			int(lis[3]),
			float(lis[4]),
			[e.split(":") for e in lis[5].split("|")])

def processfile(filename):
	dic = {}
	with open(filename, 'r') as reader:
		for line in reader:
			if "Metrics@" in line:
				processline(line, dic)
	return dic

def stat(dic, time):
	cnt = 0
	laten = 0.0
	rpc = {}
	for key in dic:
		for sim in dic[key]:
			cnt += dic[key][sim][0]
			laten += (dic[key][sim][0] * dic[key][sim][2])
			for e in dic[key][sim][3]:
				if e[0] not in rpc:
					#print(e)
					if e == ['']:
						continue
					rpc[e[0]] = [(int(e[1]),int(e[2]))]
				else:
					rpc[e[0]].append((int(e[1]),int(e[2])))

	print(cnt)     # RPC count
	print(float(time)/1000)   # run time in second
	print(laten / cnt)      # average latency 
	print(1.0 * cnt / (float(time)/1000))   # throughput

	latpercat = []
	for key in rpc:
		tot_lat = 0
		tot_cnt = 0
		for e in rpc[key]:
			tot_lat += e[0]
			tot_cnt += e[1]
			#print("{0} {1:.0f} {2:.2f}%".format(key,tot_lat/tot_cnt, 100.0*tot_cnt/cnt))
		latpercat.append((key, tot_lat/tot_cnt, 100.0*tot_cnt/cnt))
	latpercat = sorted(latpercat, key=lambda x:x[1],reverse=True)
	for el in latpercat:
		print("{0} {1:.0f} {2:.2f}%".format(el[0],el[1],el[2]))
		
def newtsStat(tsdic, allo_lat, reg_lat):

	newdic = {}
	newdic2 = {}
	newdic3 = {}
	for key in tsdic:
		for jobid in tsdic[key]:
			newdic2[jobid] = tsdic[key][jobid][0]
	for key in allo_lat:
		for jobid in allo_lat[key]:
			newdic[jobid] = allo_lat[key][jobid]
	for key in reg_lat:
		for jobid in reg_lat[key]:
			newdic3[jobid] = reg_lat[key][jobid]
	sortdic = sorted(newdic2.items(),key=lambda x:x[1])
	for el in sortdic:
		if el[0] in newdic:
			print("{0} {1} {2}".format(el[0], newdic[el[0]], newdic3[el[0]]))


def tsStat(dic, dic2):
	minStartTS = sys.maxsize
	maxEndTS = 0
	newdic = {}
	allo = {}
	for key in dic:
		for jobid in dic[key]:
			if jobid not in newdic:
				newdic[jobid] = [dic[key][jobid][0],dic[key][jobid][1]]
				#minStartTS = min(dic[key][jobid][0], minStartTS)
				#maxEndTS = max(dic[key][jobid][1], maxEndTS)
			else:
				start = min(dic[key][jobid][0], newdic[jobid][0])
				end = max(dic[key][jobid][1],newdic[jobid][1])
				newdic[jobid] = (start, end)
				#newdic[jobid] = (min(dic[key][jobid][0], newdic[jobid][0]), 
				#        max(dic[key][jobid][1],newdic[jobid][1]))

	for key in dic2:
		for jobid in dic2[key]:
			allo[jobid] = dic2[key][jobid]

	total = 0
	count = 0
	sortdic = sorted(newdic.items(), key = lambda x: x[1][0])
	for el in sortdic:
		print(el[0],end=' ')
		print("{0} {1}".format(el[1][1] - el[1][0], el[1][1] - allo[el[0]]))

	for key in newdic:
		if (newdic[key][1] < newdic[key][0]):
			print("alert: {0}, {1}".format(newdic[key][0], newdic[key][1]))
		count = count+1
		total = total + 1.0 * (newdic[key][1] - newdic[key][0])
		#print("{0}".format(newdic[key][1]- newdic[key][0]))
	print(total / count)

	#print(maxEndTS - minStartTS)
	#print(minStartTS)
	#print(maxEndTS)

if len(sys.argv) != 2:
	print("input format:\n\tpost-process.py <log folder>")
	sys.exit(1)

folder = sys.argv[1]
dic = {}
tsdic = {}
allo_lat ={}
reg_lat = {}
allodic = {}
time = 0

for filename in os.listdir(folder):
	if "sim" in filename:
		dic1 = processfile(os.path.join(folder, filename))
		dic[filename] = dic1
	elif "CC" in filename:
		with open(os.path.join(folder, filename), 'r') as reader:
			for line in reader:
				if "Total time" in line:
#                    print(line)
					time = int(line.split(":")[1])
					break
stat(dic, time)
#newtsStat(tsdic,allo_lat,reg_lat)
