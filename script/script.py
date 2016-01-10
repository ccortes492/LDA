#!/usr/bin/env python

from subprocess import check_output

a=check_output(["wc","-l","../../NYTimes/100db.txt" ])
n = int(a.split()[0])
num_partitions = 4
div=2
for i in range(num_partitions):
	div=div*2
	print n/div

