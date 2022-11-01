from flask import Flask, render_template
import util

app = Flask(__name__)

username='lex'
password=''
host='127.0.0.1'
port='5432'
database='dvdrental'

@app.route('/')
def index():
    cursor, connection = util.connect_to_db(username,password,host,port,database)
    record = util.run_and_fetch_sql(cursor, "SELECT * from customer;")
    if record == -1:
        print('Something is wrong with the SQL command')
    else:
        col_names = [desc[0] for desc in cursor.description]
        log = record[:5]
    util.disconnect_from_db(connection,cursor)
    return render_template('index.html', sql_table = log, table_title=col_names)

@app.route('/api/update_basket_a')
def update_basket_a():
    cursor, connection = util.connect_to_db(username,password,host,port,database)
    record, error = util.run_sql(cursor, "INSERT INTO basket_a (a, fruit_a) VALUES (5, 'Cherry');")
    connection.commit()
    
    if record == -1:
        print('Something is wrong with the SQL command')
        util.disconnect_from_db(connection, cursor)
        return "<html><body> ERROR: \n" + str(error) + "</body></html>"
    else:
        print("Success!")
        util.disconnect_from_db(connection,cursor)
        return "<html><body> Success! </body></html>"
    
@app.route('/api/unique')
def unique():
    cursor, connection = util.connect_to_db(username,password,host,port,database)
    record, error = util.run_and_fetch_sql(cursor, "SELECT fruit_a, fruit_b FROM basket_a FULL JOIN basket_b ON fruit_a = fruit_b WHERE a IS NULL OR b IS NULL;")
    
    if record == -1:
        print('Something is wrong with the SQL command')
        return "<html><body> ERROR: \n" + str(error) + "</body></html>"
    else:
        record = parse_values(record)
        col_names = [desc[0] for desc in cursor.description]
        log = record[:5]
        
    util.disconnect_from_db(connection,cursor)
    return render_template('index.html', sql_table = log, table_title=col_names)

def parse_values(record):
    results = []
    for i in range(len(record)):
        if record[i][1] == None:
            results.append((record[i][0],record[-i-1][1]))
    return results
    
    

if __name__ == '__main__':
    app.debug = True
    ip = '127.0.0.1'
    app.run(host=ip)

