from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

@app.route('/api/data/<clid>/<dvid>', methods=['GET'])
def get_data(clid, dvid):
    conn = sqlite3.connect('mobi.db')
    cursor = conn.cursor()

    sqlstr = "SELECT * FROM device_employees WHERE device_id = '{dvid}' and client_id = '{clid}' and status is not 'OK'".format(dvid = dvid, clid = clid)
    cursor.execute(sqlstr)
    rows = cursor.fetchall()

    data = []
    for row in rows:
        data.append({description[0]: column for description, column in zip(cursor.description, row)})

    cursor.close()
    conn.close()

    return jsonify(data)


@app.route('/api/empok/<dvid>/<device_emp_id>', methods=['POST'])
def set_data(dvid, device_emp_id):
    data = request.get_json()  # get data from POST request
    if not data:
        return jsonify({'message': 'No input data provided'}), 400
    
    if data["data"] == 'OK':
        conn = sqlite3.connect('mobi.db')
        c = conn.cursor()
        sqlstr = "update device_employees set status = 'OK' where id = '{device_emp_id}'".format(device_emp_id=device_emp_id)
        print(sqlstr)
        c.execute(sqlstr)
        conn.commit()
        c.close()
        conn.close()
    # process your data
    return jsonify({'message': 'Successfully processed the data'}), 200


if __name__ == '__main__':
    app.run(host="0.0.0.0")