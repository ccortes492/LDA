#!/usr/bin/env python

from subprocess import check_output

dbpath ="../../NYTimes/100db.txt" 
a=check_output(["wc","-l", dbpath ])
n = int(a.split()[0])
#contar lineas con la funcion de python es mas lento.
#num_lines = sum(1 for line in open(dbpath))
#print "num_lines es", num_lines
num_partitions = 4
div=1
for i in range(num_partitions):
	div=div*2
	print n/div

