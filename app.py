from flask import Flask, jsonify, request
import sqlite3, uuid

app = Flask(__name__)

@app.route('/api/data/<clid>/<dvid>', methods=['GET'])
def get_employees(clid, dvid):
    conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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
        conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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
        name = data["name"]
        password = data["password"]
        if password == "@#$SS1a@@":

            device_id = str(uuid.uuid4())

            #verifying if this client exists
            conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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
            values ('{id}', '{clid}', '{name}', '{plant}') '''.format(id = device_id, clid=client, name=name, plant=plant)
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


@app.route('/api/getdevices', methods=['GET'])
def get_devices():
    conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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

            conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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
    conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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
            conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
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
            conn = sqlite3.connect('/home/ubuntu/api_integration/mobi.db')
            c = conn.cursor()

            sqlstr = '''delete from device_employees where client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            sqlstr = '''delete from devices where client_id = '{client_id}' '''.format(client_id=client_id)
            c.execute(sqlstr)

            return jsonify({'message': 'Device deleted'}), 200

                


if __name__ == '__main__':
    app.run()