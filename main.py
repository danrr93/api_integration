import requests, json, time, sqlite3
#remember, now functions needs args

dbpath = '/home/ubuntu/api_integration/mobi.db'

def checkDB():
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()

    #cria todas as tabelas no banco
    sqlstr = '''CREATE TABLE IF NOT EXISTS employees ( 
        matricula  VARCHAR(100), 
        nome  TEXT, 
        afastado  INTEGER, 
        cracha VARCHAR(50), 
        client_id INTEGER, 
        planta INTEGER, 
        status VARCHAR(5),
        PRIMARY KEY("matricula", "planta", "client_id"),
        FOREIGN KEY(client_id) REFERENCES clients(id)
        )'''
    c.execute(sqlstr)

    sqlstr = '''CREATE TABLE IF NOT EXISTS clients (
        id TEXT PRIMARY KEY, 
        name TEXT, 
        login TEXT, 
        user TEXT, 
        password TEXT, 
        token TEXT
        )'''
    c.execute(sqlstr)

    sqlstr = '''CREATE TABLE IF NOT EXISTS devices (
        id TEXT PRIMARY KEY,
        client_id INTEGER,
        mac TEXT,
        plant INTEGER,
        conn_time VARCHAR(20),
        referencia_prod TEXT,
        codcontrole_prod TEXT,
        FOREIGN KEY(client_id) REFERENCES clients(id)
        )
        '''
    c.execute(sqlstr)

    sqlstr = '''CREATE TABLE IF NOT EXISTS device_employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id INTEGER,
        employee_id INTEGER,
        client_id INTEGER,
        cracha VARCHAR(20),
        status VARCHAR(10),
        FOREIGN KEY(client_id) REFERENCES clients(id),
        FOREIGN KEY(device_id) REFERENCES devices(id),
        FOREIGN KEY(employee_id) REFERENCES employees(id))'''
    c.execute(sqlstr)

    sqlstr = '''
        CREATE TABLE IF NOT EXISTS events (
        id VARCHAR(36) PRIMARY KEY,
        employee_id VARCHAR(36),
        codcontrole VARCHAR(36),
        referencia VARCHAR(36),
        usage INTEGER,
        datahora TEXT,
        client_id VARCHAR(36),
        device_id VARCHAR(36)
        )
    '''
    c.execute(sqlstr)

    c.close()
    conn.close()


def getClients():
    #pega todos os registros em clients
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    sqlstr = "select * from clients"
    c.execute(sqlstr)
    clients_rows = c.fetchall()
    print(clients_rows)
    c.close()
    conn.close()
    return clients_rows


def loginApi(userid, password, clientlogin):
    #Here we update tokens
    #to obtain all args, run getClients first
    print('starting login integration')
    print(userid, password, clientlogin)

    url = 'https://mobintegration.azurewebsites.net/api/Login'
    body = '{"UserID": "%s", "AccessKey": "%s", "IdCliente": "%s"}' % (userid, password, clientlogin)

    while True:
        res = requests.post(url, json=body)

        if res.status_code != 200:
            print("Request failed by status code:", str(res.status_code))
            time.sleep(1)
            continue
        else:
            print("200 OK")
            break

    jsondata = json.loads(res.content)

    if jsondata["authenticated"] == False:
        print("Error at authenticate client:", clientlogin)
        return "ERROR"

    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    sqlstr = "update clients set token = '{apitoken}' where login = '{clid}'".format(apitoken = jsondata["accessToken"], clid=clientlogin)
    c.execute(sqlstr)
    conn.commit()
    c.close()
    conn.close()

    print(sqlstr)
    time.sleep(5)

    return jsondata["accessToken"]


def updateEmployees(codplanta, apitoken, client):
    #url definition and read from apitoken file
    url = 'https://mobintegration.azurewebsites.net/api/default/obterfuncionarioplanta?codigoPlanta={codplanta}'.format(codplanta=codplanta)

    #authorization header definition to access api
    headers = {
        "Authorization": "Bearer {apitoken}".format(apitoken=apitoken)
    }

    #http request
    n = 0
    while True:
        res = requests.get(url, headers = headers)

        if res.status_code != 200:
            n += 1
            #attempts to http req, if failed return status code
            if n == 2:
                print("All atempts for http request failed:", str(res.status_code))
                return res.status_code
            
            print("Request failed by status code:", str(res.status_code))
            time.sleep(3)
            continue
        else:
            print("200 OK")
            break

    #if request is 200 ok, parse json
    jsondata = json.loads(res.content)

    if len(jsondata) == 0:
        return 1

    #prepare to write employees in db
    print('writing mobiemployees DB file...')

    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    #c.execute("CREATE TABLE IF NOT EXISTS employees (matricula  VARCHAR(100), nome  TEXT, planta TEXT, afastado  INTEGER, cracha VARCHAR(50), client_id INTEGER, planta INTEGER, PRIMARY KEY(matricula))")

    i = 0
    while True:
        try:
            nome = jsondata[i]['nome']
            matricula = jsondata[i]['matricula']
            #planta = jsondata[i]['planta']
            afastado = jsondata[i]['afastado']
            cracha = jsondata[i]['codigoCracha']
            dtdemissao = jsondata[i]['dt_Demissao']
        except IndexError:
            print('end of list')
            c.close()
            conn.commit()
            conn.close()
            break

        if afastado == 'True' or dtdemissao != None:
            status = 'DEL'
        else:
            status = 'ADD'

        sqlstr = '''INSERT INTO employees (matricula, nome, afastado, cracha, client_id, planta, status) 
        VALUES ('{matricula}', '{nome}', '{afastado}', '{cracha}', '{client}', '{planta}', '{status}')
        '''.format(nome=nome, matricula=matricula, planta=codplanta, afastado=afastado, cracha=cracha, client=client, status=status)
        print("Inserindo colaborador matricula:", matricula, "e nome:", nome)

        try:
            c.execute(sqlstr)
        except Exception as e:
            print("Falha na inserção deste colaborador matricula:", matricula, "Erro:", str(e))
            print("Verificando consistencia dos dados deste colaborador")
            #if failed in add employee by unique pk, select this employee and verifiy if edited by mobi

            sqlstr = "SELECT * FROM employees WHERE matricula = '{matricula}' and planta = '{codplanta}'".format(matricula=matricula, codplanta=codplanta)
            #print(sqlstr)
            c.execute(sqlstr)
            rows = c.fetchall()
            #print(rows)
            count = 0
            for row in rows:
                if count != 0:
                    print("Parece que existem dois colaboradores com a mesma matricula")
                    time.sleep(10)
                    break
                count += 1
                mat = row[0]
                nam = row[1]
                afa = row[2]
                cra = row[3]
                pla = row[5]

                map = {"True": True, "False": False} 
                afa = map[afa]


                #verify if employee is edited by mobi software
                if mat == matricula and nam == nome and pla == codplanta and afa == afastado and cra == str(cracha):
                    print("Colaborador não editado, tudo ok")

                elif mat != matricula and nam == nome and pla == codplanta and afa == afastado and cra == str(cracha):
                    print("Colaborador somente com a matricula diferente")
                    print("Atualizando matricula na tabela employees")
                    sqlstr = "UPDATE employees set id = {nova_matricula} WHERE id = {velha_matricula}".format(nova_matricula = matricula, velha_matricula = mat)
                    try:
                        c.execute(sqlstr)
                    except Exception:
                        continue
                    #update in device_employees too...
                    print("Atualizando matricula na tabela device_employees")
                    sqlstr = "UPDATE device_employees set employee_id = '{empid}', status = 'UPD' WHERE employee_id = '{oldempid}' and client_id = '{clid}'".format(oldempid = mat, clid = client)
                    try:
                        c.execute(sqlstr)
                    except Exception:
                        continue

                    #agora atualiza employee_id dos eventos
                    print("Atualizando matrícula nos eventos")
                    sqlstr = "UPDATE events set employee_id = '{empid}' WHERE employee_id = '{oldempid}', and client_id = '{clid}'".format(oldempid = mat, clid = client)
                    try:
                        c.execute(sqlstr)
                    except Exception:
                        continue

                else:
                    sqlstr = "UPDATE employees SET nome = '{nam}', planta = '{pla}', afastado = '{afa}', cracha = '{cra}', status = 'UPD' WHERE matricula = '{mat}'".format(mat=matricula, nam=nome, pla=codplanta, afa=afastado, cra=cracha)
                    print(sqlstr)
                    try:
                        c.execute(sqlstr)
                    except Exception:
                        #time.sleep(.1)
                        continue


        conn.commit()
        print(i)
        i += 1

    print('file writted')
    return 0


def sendEmployeesToDevice(client, plant):
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()

    sqlstr = "select * from devices where client_id = '{client}' and plant = '{plant}'".format(client=client, plant=plant)
    c.execute(sqlstr)
    devices = c.fetchall()
    #print('devices is:', devices)
    #time.sleep(3)

    sqlstr = "select * from employees where client_id = '{client}' and planta = '{plant}' and status is not 'OK'".format(client=client, plant=plant)
    c.execute(sqlstr)
    employees = c.fetchall()

    for employee in employees:
        #print(employee)
        emp_id = employee[0]
        status = employee[6]
        cracha = employee[3]

        for device in devices:
            print(device)
            id = device[0]

            sqlstr = '''insert into device_employees 
            (device_id, employee_id, client_id, cracha, status) VALUES 
            ('{device_id}', '{employee_id}', '{client_id}', '{cracha}', '{status}')
            '''.format(device_id = id, employee_id = emp_id, client_id = client, cracha=cracha, status = status)
            print(sqlstr)
            c.execute(sqlstr)

            sqlstr = '''update employees set status = 'OK' WHERE
            matricula = '{emp_id}' and planta = '{plant}' '''.format(emp_id=emp_id, plant=plant)
            print(sqlstr)
            c.execute(sqlstr)

            conn.commit()

    c.close()
    conn.close()


def sendEvents(token, client):
    print('Enviado eventos...')
    time.sleep(3)
    url = 'https://mobintegration.azurewebsites.net/api/VendingMachine/receberConsumoEpi'
    headers = {'Authorization': 'Bearer '+token}

    conn = sqlite3.connect(dbpath)
    c = conn.cursor()   
    sqlstr = '''select * from events where client_id = '{client}' '''.format(client=client)
    c.execute(sqlstr)
    res = c.fetchall()

    for event in res:
        evid = event[0]
        employee_id = event[1]
        codcontrole = event[2]
        ref = event[3]
        usage = event[4]
        datahora = event[5]
        bjson = {"Matricula": employee_id, "DataRetirada": datahora, "EpisConsumidos":[{"CodigoControle": codcontrole, "Referencia": ref, "QuantidadeConsumida": usage, "DataHora": datahora}]}
        r = requests.post(url, json=bjson, headers=headers)
        print(r.content)
        time.sleep(.3)

        if r.status_code != 200:
            print('erro at send event, status code is:', str(r.status_code))
            continue

        try:
            json_data = json.loads(r.content)
            print(json_data)
        except Exception:
            print('error at json loads in send events')
            continue
        else:
            if json_data['ok'] == True:
                print('event sended!')
                sqlstr = '''delete from events where id = '{evid}' '''.format(evid = evid)
                print(sqlstr)
                c.execute(sqlstr)

            else:
                print('error at response from API in send events')

    print("Sem eventos por aqui")
    conn.commit()
    c.close()
    conn.close()


def mainloop():
    checkDB()

    clients = getClients()
    for client in clients:
        client_id = client[0]
        clientlogin = client[2]
        user = client[3]
        password = client[4]
        token = client[5]
        planta = 0
        while True:
            res = updateEmployees(planta, token, client_id)
            sendEvents(token, client_id)

            if res == 401:
                #time to update token
                print("status 401, running token")
                token = loginApi(user, password, clientlogin)
                if token != "ERROR":
                    continue
                else:
                    break

            if res == 0:
                print("status 200, updateEmployees run ok for client:", clientlogin, "and plant:", planta)
                print("Start to populate device_employees at clid:", client_id, "and plant:", planta)
                time.sleep(3)
                sendEmployeesToDevice(client_id, planta)
                #ok, continue running
                planta += 1
                continue

            if res == 1:
                print("this plant does not exists, ending update for this client")
                break

    print("no more clients")

    
mainloop()
