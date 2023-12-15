import requests, json, time, sqlite3, api
#remember, now functions needs args

def checkDB():
    conn = sqlite3.connect('mobi.db')
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
        id INTEGER, 
        name TEXT, 
        login TEXT, 
        user TEXT, 
        password TEXT, 
        token TEXT,
        PRIMARY KEY("id" AUTOINCREMENT)
        )'''
    c.execute(sqlstr)

    sqlstr = '''CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        name TEXT,
        plant INTEGER,
        FOREIGN KEY(client_id) REFERENCES clients(id)
        )
        '''
    c.execute(sqlstr)

    sqlstr = '''CREATE TABLE IF NOT EXISTS device_employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id INTEGER,
        employee_id INTEGER,
        client_id INTEGER,
        status VARCHAR(10),
        FOREIGN KEY(client_id) REFERENCES clients(id),
        FOREIGN KEY(device_id) REFERENCES devices(id),
        FOREIGN KEY(employee_id) REFERENCES employees(id))'''
    c.execute(sqlstr)

    c.close()
    conn.close()


def getClients():
    #pega todos os registros em clients
    conn = sqlite3.connect('mobi.db')
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

    conn = sqlite3.connect('mobi.db')
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

    conn = sqlite3.connect('mobi.db')
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
        print(sqlstr)

        try:
            c.execute(sqlstr)
        except Exception as e:
            #if failed in add employee by unique pk, select this employee and verifiy if edited by mobi
            print(str(e))
            sqlstr = "SELECT * FROM employees WHERE matricula = '{matricula}' and planta = '{codplanta}'".format(matricula=matricula, codplanta=codplanta)
            print(sqlstr)
            c.execute(sqlstr)
            rows = c.fetchall()
            print(rows)

            for row in rows:
                mat = row[0]
                nam = row[1]
                afa = row[2]
                cra = row[3]
                pla = row[5]

                map = {"True": True, "False": False} 
                afa = map[afa]

                if mat == matricula:
                    print('matricula igual')
                if nam == nome:
                    print('nome igual')
                if pla == codplanta:
                    print('plant igual')
                if afa == afastado:
                    print('afastado igual')
                if cra == str(cracha):
                    print('cracha igual')
                #time.sleep(3)

                #verify if employee is edited by mobi software
                if mat == matricula and nam == nome and pla == codplanta and afa == afastado and cra == str(cracha):
                    print("Everithing is fine with this employee, nothing to edit")

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
    conn = sqlite3.connect('mobi.db')
    c = conn.cursor()

    sqlstr = "select * from devices where client_id = '{client}' and plant = '{plant}'".format(client=client, plant=plant)
    c.execute(sqlstr)
    devices = c.fetchall()

    sqlstr = "select * from employees where client_id = '{client}' and planta = '{plant}' and status is not 'OK'".format(client=client, plant=plant)
    c.execute(sqlstr)
    employees = c.fetchall()

    for employee in employees:
        print(employee)
        emp_id = employee[0]
        status = employee[6]

        for device in devices:
            print(device)
            id = device[0]

            sqlstr = '''insert into device_employees 
            (device_id, employee_id, client_id, status) VALUES 
            ('{device_id}', '{employee_id}', '{client_id}', '{status}')
            '''.format(device_id = id, employee_id = emp_id, client_id = client, status = status)
            c.execute(sqlstr)

            sqlstr = '''update employees set status = 'OK' WHERE
            matricula = '{emp_id}' and planta = '{plant}' '''.format(emp_id=emp_id, plant=plant)
            c.execute(sqlstr)

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

            if res == 401:
                #time to update token
                print("status 401, running token")
                token = loginApi(user, password, clientlogin)
                continue

            if res == 0:
                print("Start to populate device_employees")
                time.sleep(3)
                sendEmployeesToDevice(client_id, planta)
                #ok, continue running
                print("status 200, updateEmployees run ok for client:", clientlogin, "and plant:", planta)
                planta += 1
                continue

            if res == 1:
                print("this plant does not exists, ending update for this client")
                break

    print("no more clients")

        

    
mainloop()
#sendEmployeesToDevice('1', '1')
