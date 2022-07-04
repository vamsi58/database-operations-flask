from flask import Flask, request, jsonify
from common.mysql_database import MysqlDatabse
from common.mongo_database import MongoDatabse
from common.cassandra_database import CassandraDatabse
import json

app = Flask(__name__)
mydb = MysqlDatabse()
mongodb = MongoDatabse()
cassandradb = CassandraDatabse()


@app.route('/health-check')
def health_check():
    return "Welcome to database operations"


@app.route('/mysql/create_schema', methods=['POST'])
def mysql_create_schema():
    host = request.values.get('host')
    username = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = mydb.create_schema(host, username, password, schema_name)
    print(result)
    return jsonify(result)


@app.route('/mysql/create_table', methods=['POST'])
def mysql_create_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns"))
    result = mydb.create_table(host, username, password, schema_name, table_name, **columns)
    print(result)
    return jsonify(result)


@app.route('/mysql/insert_record', methods=['POST'])
def mysql_insert_record():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns"))
    result = mydb.insert_record(host, username, password, schema_name, table_name, **columns)
    print(result)
    return jsonify(result)


@app.route('/mysql/insert_multiple_records', methods=['POST'])
def mysql_insert_multiple_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns").replace("'", '"'))
    input_file_name = request.values.get("input_file_name")
    result = mydb.insert_multiple_records(host, username, password, schema_name, table_name, input_file_name, *columns)
    print(result)
    return jsonify(result)


@app.route('/mysql/update_records', methods=['POST'])
def mysql_update_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    updated_values = json.loads(request.values.get("updated_values"))
    result = mydb.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
    print(result)
    return jsonify(result)


@app.route('/mysql/retrieve_records', methods=['POST'])
def mysql_retrieve_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mydb.retrieve_records(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mysql/retrieve_records_with_filter', methods=['POST'])
def mysql_retrieve_records_with_filter():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    result = mydb.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
    print(result)
    return jsonify(result)


@app.route('/mysql/delete_records_with_filter', methods=['POST'])
def mysql_delete_records_with_filter():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    result = mydb.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
    print(result)
    return jsonify(result)


@app.route('/mysql/delete_records', methods=['POST'])
def mysql_delete_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mydb.delete_records(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mysql/truncate_table', methods=['POST'])
def mysql_truncate_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mydb.truncate_table(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mysql/drop_table', methods=['POST'])
def mysql_drop_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mydb.drop_table(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mysql/drop_schema', methods=['POST'])
def mysql_drop_schema():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    result = mydb.drop_schema(host, username, password, schema_name)
    print(result)
    return jsonify(result)

#-----------------------------------------------------------
# Mongo DB
#-----------------------------------------------------------
@app.route('/mongo/create_schema', methods=['POST'])
def mongo_create_schema():
    host = request.values.get('host')
    username = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = mongodb.create_schema(host, username, password, schema_name)
    print(result)
    return jsonify(result)


@app.route('/mongo/create_table', methods=['POST'])
def mongo_create_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns"))
    result = mongodb.create_table(host, username, password, schema_name, table_name, **columns)
    print(result)
    return jsonify(result)

@app.route('/mongo/insert_record', methods=['POST'])
def mongo_insert_record():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns"))
    result = mongodb.insert_record(host, username, password, schema_name, table_name, **columns)
    print(result)
    return jsonify(result)


@app.route('/mongo/insert_multiple_records', methods=['POST'])
def mongo_insert_multiple_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns").replace("'", '"'))
    input_file_name = request.values.get("input_file_name")
    result = mongodb.insert_multiple_records(host, username, password, schema_name, table_name, input_file_name, *columns)
    print(result)
    return jsonify(result)


@app.route('/mongo/update_records', methods=['POST'])
def mongo_update_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    updated_values = json.loads(request.values.get("updated_values"))
    result = mongodb.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
    print(result)
    return jsonify(result)


@app.route('/mongo/retrieve_records', methods=['POST'])
def mongo_retrieve_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mongodb.retrieve_records(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mongo/retrieve_records_with_filter', methods=['POST'])
def mongo_retrieve_records_with_filter():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    result = mongodb.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
    print(result)
    return jsonify(result)


@app.route('/mongo/delete_records_with_filter', methods=['POST'])
def mongo_delete_records_with_filter():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    result = mongodb.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
    print(result)
    return jsonify(result)


@app.route('/mongo/delete_records', methods=['POST'])
def mongo_delete_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mongodb.delete_records(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mongo/truncate_table', methods=['POST'])
def mongo_truncate_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mongodb.truncate_table(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mongo/drop_table', methods=['POST'])
def mongo_drop_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = mongodb.drop_table(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/mongo/drop_schema', methods=['POST'])
def mongo_drop_schema():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    result = mongodb.drop_schema(host, username, password, schema_name)
    print(result)
    return jsonify(result)

# Cassandra
#===========
@app.route('/cassandra/create_schema', methods=['POST'])
def cassandra_create_schema():
    host = request.values.get('host')
    username = request.values.get('username')
    password = request.values.get('password')
    schema_name = request.values.get('schema_name')
    result = cassandradb.create_schema(host, username, password, schema_name)
    print(result)
    return jsonify(result)


@app.route('/cassandra/create_table', methods=['POST'])
def cassandra_create_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns"))
    result = cassandradb.create_table(host, username, password, schema_name, table_name, **columns)
    print(result)
    return jsonify(result)


@app.route('/cassandra/insert_record', methods=['POST'])
def cassandra_insert_record():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns"))
    result = cassandradb.insert_record(host, username, password, schema_name, table_name, **columns)
    print(result)
    return jsonify(result)


@app.route('/cassandra/insert_multiple_records', methods=['POST'])
def cassandra_insert_multiple_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    columns = json.loads(request.values.get("columns").replace("'", '"'))
    input_file_name = request.values.get("input_file_name")
    result = cassandradb.insert_multiple_records(host, username, password, schema_name, table_name, input_file_name, *columns)
    print(result)
    return jsonify(result)


@app.route('/cassandra/update_records', methods=['POST'])
def cassandra_update_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    updated_values = json.loads(request.values.get("updated_values"))
    result = cassandradb.update_records(host, username, password, schema_name, table_name, filters, **updated_values)
    print(result)
    return jsonify(result)


@app.route('/cassandra/retrieve_records', methods=['POST'])
def cassandra_retrieve_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = cassandradb.retrieve_records(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/cassandra/retrieve_records_with_filter', methods=['POST'])
def cassandra_retrieve_records_with_filter():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    result = cassandradb.retrieve_records_with_filter(host, username, password, schema_name, table_name, **filters)
    print(result)
    return jsonify(result)


@app.route('/cassandra/delete_records_with_filter', methods=['POST'])
def cassandra_delete_records_with_filter():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    filters = json.loads(request.values.get("filters"))
    result = cassandradb.delete_records_with_filter(host, username, password, schema_name, table_name, **filters)
    print(result)
    return jsonify(result)


@app.route('/cassandra/delete_records', methods=['POST'])
def cassandra_delete_records():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = cassandradb.delete_records(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/cassandra/truncate_table', methods=['POST'])
def cassandra_truncate_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = cassandradb.truncate_table(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/cassandra/drop_table', methods=['POST'])
def cassandra_drop_table():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    table_name = request.values.get("table_name")
    result = cassandradb.drop_table(host, username, password, schema_name, table_name)
    print(result)
    return jsonify(result)


@app.route('/cassandra/drop_schema', methods=['POST'])
def cassandra_drop_schema():
    host = request.values.get("host")
    username = request.values.get("username")
    password = request.values.get("password")
    schema_name = request.values.get("schema_name")
    result = cassandradb.drop_schema(host, username, password, schema_name)
    print(result)
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
