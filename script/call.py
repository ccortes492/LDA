#!/usr/bin/env python

from subprocess import call

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 100db.txt em', shell=True)

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 100db.txt online', shell=True)


print "Acabo 100db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://redd:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 050db.txt em',shell=True)


return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://redd:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 050db.txt online',shell=True)


print "Acabo 50db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 025db.txt',shell=True)

print "Acabo 025db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 012db.txt',shell=True)


print "Acabo 012db"

return_code = call('/usr/local/spark/bin/spark-submit --class "TestApp" --master spark://reed:7077 ../TestApp/target/scala-2.10/testapp_2.10-1.0.jar 006db.txt',shell=True)

print "Acabo 006db"
