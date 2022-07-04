import csv
import pymongo
import sys
import cassandra
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from .database import Database


def connect_database():
    cloud_config = {
        'secure_connect_bundle': 'E:\\PythonTraining\\Datastax\\secure-connect-db1.zip'
    }
    auth_provider = PlainTextAuthProvider('JCQsdZKAaPYDalZjBcXbIdQt',
                                          '1bpGqn6G1rot8P+vq7,Y_JyOmd,qnRU8ima-sHlmw--Hip_dMFMiFbzNmWejTh68vFPgs.be2oBKSAdQbNW_ts3KJT6.B20ELtlqmceaHc6eSeyqcwt9BYd.N1Nd3X_9')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    return session


class CassandraDatabse(Database):
    def create_schema(self, host, username, password, schema_name):
        try:
            session = connect_database()
            replica = " with replication={ 'class': 'SimpleStrategy', 'replication_factor' : 3};"
            query = f"CREATE KEYSPACE IF NOT EXISTS {schema_name} " + replica
            session.execute(query)
            return f"Schema {schema_name} has been created"
        except Exception as exc:
            return str(exc)

    def drop_schema(self, host, username, password, schema_name):
        try:
            session = connect_database()
            query = f"DROP KEYSPACE IF EXISTS {schema_name}"
            print(query)
            session.execute(query)
            return f"Schema {schema_name} has been dropped"
        except Exception as exc:
            return str(exc)

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            session = connect_database()
            query = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}({','.join([x + ' ' + y for x, y in columns.items()])});"
            print(query)
            session.execute(query)
            return f"Table {table_name} has been created"
        except Exception as exc:
            return str(exc)

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            session = connect_database()
            query = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"
            print(query)
            session.execute(query)
            return f"Table {table_name} has been dropped"
        except Exception as exc:
            return str(exc)

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            values = ','.join("'{0}'".format(v) for v in columns.values())
            session = connect_database()
            query = f"INSERT INTO {schema_name}.{table_name}({','.join(columns.keys())}) VALUES({values} ); "
            session.execute(query)
            return f"Row inserted into table {table_name.title()}"
        except Exception as exc:
            return str(exc)

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            columns_headings = ','.join(columns)
            session = connect_database()
            with open(input_file_name, encoding="utf-8-sig") as csv_f:
                csv_r = csv.reader(csv_f)
                for row in csv_r:
                    values = ','.join("'{0}'".format(v) for v in row)
                    query = f"INSERT INTO {schema_name}.{table_name}({columns_headings}) VALUES({values}); "
                    session.execute(query)
            return f"Rows inserted into table {table_name.title()}"
        except Exception as exc:
            return str(exc)

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            values = ','.join(f"{k} = '{v}'" for k, v in updated_values.items())
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            session = connect_database()
            query = f"UPDATE {schema_name}.{table_name} SET {values} WHERE {where} ALLOW FILTERING;"
            session.execute(query)
            return f"Row updated in table {table_name.title()}"
        except Exception as exc:
            return str(exc)

    def retrieve_records(self, host, username, password, schema_name, table_name):
        one_row = {}
        all_rows = []
        try:
            session = connect_database()
            query = f"SELECT * FROM {schema_name}.{table_name};"
            rows = session.execute(query)
            print("Rows,", rows)
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            print("here", rows)
            return all_rows
        except Exception as exc:
            return str(exc)

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        one_row = {}
        all_rows = []
        try:
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            session = connect_database()
            query = f"SELECT * FROM {schema_name}.{table_name} WHERE {where} ALLOW FILTERING;"
            rows = session.execute(query)
            print("Rows,", rows)
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            print("here", rows)
            return all_rows
        except Exception as exc:
            return str(exc)

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            session = connect_database()
            query = f"DELETE FROM {schema_name}.{table_name} WHERE {where};"
            print(query)
            session.execute(query)
            return f"Records deleted from table {table_name.title()}"
        except Exception as exc:
            return str(exc)

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            session = connect_database()
            query = f"DELETE FROM {schema_name}.{table_name} WHERE id > '2';"
            session.execute(query)
            return f"Records deleted from table {table_name.title()}"
        except Exception as exc:
            return str(exc)

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            session = connect_database()
            query = f"TRUNCATE TABLE {schema_name}.{table_name};"
            session.execute(query)
            return f"Table {table_name.title()} has been truncated"
        except Exception as exc:
            return str(exc)
