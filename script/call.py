#!/usr/bin/env python

from subprocess import call
import time

optimizers = ['em','online']
cluster = ['spark://reed:7077']
db  = ['100db.txt','050db.txt', '025db.txt', '012db.txt', '006db.txt']
cores = [1,2,4,8,16]

for opt in optimizers:
	for clus in cluster:
		for data in db:
			for num in cores: 
				print "Empiezo "+data+opt+str(num)
				command = '/usr/local/spark/bin/spark-submit --class "TestApp" --master '+ clus+' --total-executor-cores '+str(num)+' --conf spark.locality.wait=50000 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar '+data+' '+opt+' '+str(num).zfill(2)
				return_code = call(command, shell=True)
				print "Acabo "+data+opt+str(num)
				time.sleep(10)
"""
return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 100db.txt em', shell=True)

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 100db.txt online', shell=True)


print "Acabo 100db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://redd:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 050db.txt em',shell=True)


return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://redd:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 050db.txt online',shell=True)


print "Acabo 50db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 025db.txt em',shell=True)

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 025db.txt online',shell=True)

print "Acabo 025db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 012db.txt em',shell=True)

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 012db.txt online',shell=True)


print "Acabo 012db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 006db.txt em',shell=True)

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 006db.txt online',shell=True)

print "Acabo 006db"

"""
