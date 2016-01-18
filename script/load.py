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
                apps = apps + [data+opt+str(c).zfill(2)]
                
    #direccion y puerto del server
    connection = httplib.HTTPConnection('reed.pc.ac.upc.edu',8080)
    connection.connect()
    #llamada a la api
    connection.request('GET','/api/v1/applications')
    #leemos el resultado de la llamada
    result = json.loads(connection.getresponse().read())
    times = []
    #para cada app
    for app in apps:
        #la buscamos en la respuesta de la API
        for i in range(len(result)):
            if app in result[i].values():
                #Cogemos los tiempos del JSON y lo guardamos en times.
                times =times + [parsedata(app, result[i])]
                #Solo nos interesa la ultima ejecucion de la app
                break
    #Damos formato a la info de cada app.
    final = []
    for app in times:
        size = app[0][0:3]
        numCores = app[0][-2:]
        if app[0].find('em')>0:
            opt = 'em'
        else:
            opt = 'online'
        final = final + [[app[0],app[1], size, numCores,opt]]

    #guardamos la info de la app en un txt 
    return final 
        

def parsedata(app, info):
    s = info['attempts'][0]['startTime']
    form = s[0:s.find('T')]+' '+s[11:s.find('G')]
    timeInicial = datetime.strptime(form, "%Y-%m-%d %H:%M:%S.%f")

    s = info['attempts'][0]['endTime']
    form = s[0:s.find('T')]+' '+s[11:s.find('G')]
    timeFinal = datetime.strptime(form, "%Y-%m-%d %H:%M:%S.%f")

    return [app,format((timeFinal - timeInicial).total_seconds(),'.3f')]


def save(matrix):
    out = ""

    for row in matrix:
        for cell in row:
            out = out + str(cell) + " ; "
        out += "\n"

    with open("out.txt","wt") as file:
        file.write(out)
    
    
"""
form = s[0:s.find('T')]+' '+s[11:s.find('G')]

time = datetime.strptime(form, "%Y-%m-%d %H:%M:%S.%f")
"""
