import json, httplib
from datetime import datetime

def load():
    #nombre de las apps
    optimizers = ['em','online']
    db  = ['100db.txt','050db.txt', '025db.txt', '012db.txt', '006db.txt']
    cores = [1,2,4,8,16]
    apps = []
    for data in db:
        for opt in optimizers:
            for c in cores:
                apps = apps + [data+opt+str(c)]
    #direccion y puerto del server
    connection = httplib.HTTPConnection('reed.pc.ac.upc.edu',8080)
    connection.connect()
    #llamada a la api
    connection.request('GET','/api/v1/applications')
    #leemos el resultado de la llamada
    result = json.loads(connection.getresponse().read())
    times = []
    for app in apps:
        for i in range(len(result)):
            if app in result[i].values():
                times =times + [parsedata(app, result[i])]
                break
    final = []
    for app in times:
        size = app[0][0:3]
        numCores = app[0][-2:]
        if app[0].find('em'):
            opt = 'em'
        else:
            opt = 'online'
        final = final + [[app[0],app[1], size, numCores,opt]]
    print final 
        

def parsedata(app, info):
    s = info['attempts'][0]['startTime']
    form = s[0:s.find('T')]+' '+s[11:s.find('G')]
    timeInicial = datetime.strptime(form, "%Y-%m-%d %H:%M:%S.%f")

    s = info['attempts'][0]['endTime']
    form = s[0:s.find('T')]+' '+s[11:s.find('G')]
    timeFinal = datetime.strptime(form, "%Y-%m-%d %H:%M:%S.%f")

    return [app,timeFinal - timeInicial]
    
"""
form = s[0:s.find('T')]+' '+s[11:s.find('G')]

time = datetime.strptime(form, "%Y-%m-%d %H:%M:%S.%f")
"""
