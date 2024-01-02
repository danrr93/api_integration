from flask import Flask, jsonify, request, send_file
from datetime import datetime
import sqlite3, uuid
from werkzeug.utils import secure_filename
import os

dbpath = '/home/ubuntu/api_integration/mobi.db'
firmwaredir = '/home/ubuntu/api_integration/firmwares/'

app = Flask(__name__)
#app.config['UPLOAD_FOLDER'] = './'

@app.route('/api/data/<clid>/<dvid>', methods=['GET'])
def get_employees(clid, dvid):
    conn = sqlite3.connect(dbpath)
    cursor = conn.cursor()

    sqlstr = "SELECT * FROM device_employees WHERE device_id = '{dvid}' and client_id = '{clid}' and status is not 'OK'".format(dvid = dvid, clid = clid)
    cursor.execute(sqlstr)
    rows = cursor.fetchall()

    data = []
    count = 0
    for row in rows:
        data.append({description[0]: column for description, column in zip(cursor.description, row)})
        count += 1
        if count < 30:
            continue
        else:
            break

    cursor.close()
    conn.close()

    return jsonify(data)


@app.route('/api/empok/<dvid>/<device_emp_id>', methods=['POST'])
def set_employee_ok(dvid, device_emp_id):
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    
    if data["data"] == 'OK':
        conn = sqlite3.connect(dbpath)
        c = conn.cursor()
        sqlstr = "update device_employees set status = 'OK' where id = '{device_emp_id}'".format(device_emp_id=device_emp_id)
        print(sqlstr)
        c.execute(sqlstr)
        conn.commit()
        c.close()
        conn.close()
    # process your data
    return jsonify({'message': 'Successfully processed the data'}), 200


@app.route('/api/newdevice', methods=['POST'])
def new_device():
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400

    if data:

        client = data["client_id"]
        plant = data["plant"]
        mac = data["mac"]
        codcontrole_prod = data["codcontrole_prod"]
        referencia_prod = data["referencia_prod"]
        password = data["password"]
        if password == "@#$SS1a@@":

            device_id = str(uuid.uuid4())

            #verifying if this client exists
            conn = sqlite3.connect(dbpath)
            c = conn.cursor()
            sqlstr = '''select * from clients where id = '{clientid}' '''.format(clientid=client)
            c.execute(sqlstr)
            clients = c.fetchall()
            count = 0
            for cl in clients:
                count += 1
            if count != 1:
                return jsonify({'message': 'Something wrong with this client_id!'}), 400

            sqlstr = '''insert into devices 
            values ('{id}', '{clid}', '{mac}', '{plant}', '0', '{codcontrole}', '{referencia}')
            '''.format(id = device_id, clid=client, mac=mac, plant=plant, codcontrole=codcontrole_prod, referencia=referencia_prod)
            print(sqlstr)
            try:
                c.execute(sqlstr)
            except Exception as e:
                print(str(e))
                c.close()
                conn.close()
                return jsonify({'message': 'Name already exists'}), 400

            sqlstr = '''select * from employees where client_id = 
            '{client_id}' and planta = '{plant}' '''.format(client_id = client, plant = plant)
            print(sqlstr)
            c.execute(sqlstr)
            employee = c.fetchall()

            for e in employee:
                employee_id = e[0]
                sqlstr = '''insert into device_employees (device_id, employee_id, client_id, status)
                VALUES ('{device_id}', '{employee_id}', '{client_id}', 'ADD')
                '''.format(device_id = device_id, employee_id = employee_id, client_id = client)
                c.execute(sqlstr)

            conn.commit()
            c.close()
            conn.close()
            return jsonify({'message': 'Device saved ok!'}), 200
        return jsonify({'message': 'Wrong password!'}), 400


@app.route('/api/syncdevice', methods=['POST'])
def sync_device():
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400

    if data:
        devmac = data["mac"]
        conntime = data["conn_time"]

        conn = sqlite3.connect(dbpath)
        c = conn.cursor()
        sqlstr = "select * from devices where mac = '{devmac}'".format(devmac=devmac)
        c.execute(sqlstr)
        device = c.fetchall()

        count = 0
        for dev in device:
            iddev = dev[0]
            clientid = dev[1]
            codcontrole = dev[6]
            referencia = dev[5]
            count += 1
            if count != 1:
                c.close()
                conn.close()
                return jsonify({"message": "Error, find more than one device with this mac!"}), 400
            
        if count == 0:
            c.close()
            conn.close()
            return jsonify({"message": "Error, find 0 devices with this mac!"}), 400
        
        sqlstr = "update devices set conn_time = '{conntime}' where id = '{iddev}'".format(conntime=conntime, iddev=iddev)
        try:
            c.execute(sqlstr)
        except Exception:
            c.close()
            conn.close()
            return jsonify({"message": "Error, maybe db is locked!"}), 400
        
        conn.commit()
        c.close()
        conn.close()

        #pega o arquivo firmware
        full_path = '{firmwaredir}{clid}'.format(firmwaredir=firmwaredir, clid=clientid)
        try:
            file_list = os.listdir(full_path)
        except Exception:
            file_name = 'none'
            
        count = 0
        for file in file_list:
            count += 1
        if count != 1:
            file_name = 'none'
        else:
            file_name=file

        return jsonify({"firm_version": file_name, "device_id": iddev, "codcontrole": codcontrole, "referencia": referencia}), 200


@app.route('/api/getdevices', methods=['GET'])
def get_devices():
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    sqlstr = '''select * from devices'''
    c.execute(sqlstr)
    rows = c.fetchall()

    data = []
    for row in rows:
        data.append({description[0]: column for description, column in zip(c.description, row)})

    c.close()
    conn.close()

    return jsonify(data)


@app.route('/api/newclient', methods=['POST'])
def new_client():
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400

    if data:
        name = data['name']
        login = data['login']
        user = data['user']
        passwordapi = data['passwordapi']
        password = data['password']

        if password == "@#$SS1a@@":

            client_id = str(uuid.uuid4())

            conn = sqlite3.connect(dbpath)
            c = conn.cursor()
            sqlstr = '''insert into clients VALUES 
            ('{client_id}', '{name}', '{login}', '{user}', '{password}', '0')
            '''.format(client_id=client_id, name=name, login=login, user=user, password=passwordapi)
            c.execute(sqlstr)
            conn.commit()
            c.close()
            conn.close()

            return jsonify({'message': 'Client saved ok!'}), 200
        else:
            return jsonify({'message': 'Wrong password'}), 400
        

@app.route('/api/getclients', methods=['GET'])
def get_clients():
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    sqlstr = '''select * from clients'''
    c.execute(sqlstr)
    rows = c.fetchall()

    data = []
    for row in rows:
        data.append({description[0]: column for description, column in zip(c.description, row)})

    c.close()
    conn.close()

    return jsonify(data)


@app.route('/api/delclient', methods=['POST'])
def del_client():
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400

    if data:
        client_id = data['client_id']
        password = data['password']
        if password == '@#$SS1a@@':
            conn = sqlite3.connect(dbpath)
            c = conn.cursor()

            sqlstr = '''delete from device_employees where
            client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            sqlstr = '''delete from employees where
            client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            sqlstr = '''delete from devices where
            client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            sqlstr = '''delete from clients where
            id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            conn.commit()
            c.close()
            conn.close()

            return jsonify({'message': 'Client deleted from database'}), 200

        else:
            return jsonify({'message': 'Wrong password!'}), 400


@app.route('/api/delclient', methods=['POST'])
def del_device():
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400

    if data:
        client_id = data['client_id']
        password = data['password']
        if password == '@#$SS1a@@':
            conn = sqlite3.connect(dbpath)
            c = conn.cursor()

            sqlstr = '''delete from device_employees where client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            sqlstr = '''delete from devices where client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            return jsonify({'message': 'Device deleted'}), 200

                
@app.route('/api/datetime', methods=['GET'])
def get_datetime():
    current_datetime = datetime.now()

    dia = current_datetime.day
    mes = current_datetime.month
    ano = current_datetime.year

    hora = current_datetime.hour
    minuto = current_datetime.minute
    segundo = current_datetime.second

    return jsonify({'dia': dia, 'mes': mes, 'ano': ano, 'hora': hora, 'minuto': minuto, 'segundo': segundo})


@app.route('/api/getclient/<id>', methods=['GET'])
def get_client(id):
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    sqlstr = '''select * from clients where id = '{id}' '''.format(id=id)
    c.execute(sqlstr)
    client = c.fetchall()
    data = []
    for row in client:
        data.append({description[0]: column for description, column in zip(c.description, row)})

    c.close()
    conn.close()

    return jsonify(data)


@app.route('/api/firmwareupdate/<clid>', methods=['POST'])
def upload_file(clid):
    #verifica se client existe
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    sqlstr = '''select * from clients where id = '{clid}' '''.format(clid=clid)
    c.execute(sqlstr)
    rows = c.fetchall()
    c.close()
    conn.close()

    count = 0
    for client in rows:
        idclient = client[0]
        count += 1
    
    if count != 1:
        return "Error with this client_id"
    

    if 'file' not in request.files:
        return 'No file part'
    
    password = request.args.get('password')
    if password != 'xy998U1$4':
        return 'Generic Error'
    
    #se client existir, cria pasta
    full_path = '{firmwaredir}{clid}'.format(firmwaredir=firmwaredir, clid=idclient)
    os.makedirs(full_path, exist_ok=True)

    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    
    filename = secure_filename(file.filename)
    file_path = os.path.join(full_path, filename)

    if os.path.isfile(file_path):
       return f"The file {file_path} exists."
    
    #remover qualquer arquivo antigo
    file_list = os.listdir(full_path)
    for file_name in file_list:
        file_path = os.path.join(full_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)


    file.save(os.path.join(full_path, filename))
    return 'File uploaded successfully'


@app.route('/api/firmwareupdate/getfile/<clid>/<filename>', methods=['GET'])
def get_file(clid, filename):
    full_path = f'{firmwaredir}/{clid}/{filename}'
    return send_file(full_path, as_attachment=True)


if __name__ == '__main__':
    app.run()