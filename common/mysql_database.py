from .database import Database
import mysql.connector as conn
import sys, csv


class MysqlDatabse(Database):
    def create_schema(self, host, username, password, schema_name):
        try:
            mysql_db = conn.connect(host = host, user = username, passwd = password, use_pure = True)
            query = f"CREATE DATABASE {schema_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return "Database has been created successfully"
        except Exception as exc:
            return exc

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"CREATE TABLE IF NOT EXISTS {table_name}({','.join([x + ' ' + y for x, y in columns.items()])});"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return f"Table {table_name.title()} has been created successfully"
        except Exception as exc:
            return exc

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            values = ','.join('"{0}"'.format(v) for v in columns.values())
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"INSERT INTO {table_name}({','.join(columns.keys())}) VALUES({values});"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            return f"Row inserted into table {table_name.title()}"
        except conn.errors.IntegrityError as exc:
            return f"Error {exc.msg} while inserting a record into {table_name.title()} "
        except Exception as exc:
            print("Unexpected database error:", sys.exc_info()[0])

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            columns_headings = ','.join(columns)
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            with open(input_file_name, encoding="utf-8-sig") as csv_f:
                csv_r = csv.reader(csv_f)
                for row in csv_r:
                    values = ','.join('"{0}"'.format(v) for v in row)
                    query = f"INSERT INTO {table_name}({columns_headings}) VALUES({values});"
                    cursor = mysql_db.cursor()
                    cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            return f"Rows inserted into table {table_name.title()}"
        except conn.errors.IntegrityError as exc:
            return f"Error {exc.msg} while inserting a record into {table_name.title()} "
        except Exception as exc:
            print("Unexpected database error:", sys.exc_info()[0])

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            values = ','.join(f"{k} = '{v}'" for k,v in updated_values.items())
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"UPDATE {table_name} SET {values} WHERE {where};"
            print(query)
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            return f"Row updated in table {table_name.title()}"
        except conn.errors.IntegrityError as exc:
            return f"Error {exc.msg} while updating a record into {table_name.title()} "
        except Exception as exc:
            print("Unexpected database error:", sys.exc_info()[0])

    def retrieve_records(self, host, username, password, schema_name, table_name):
        one_row = {}
        all_rows = []
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"SELECT * FROM {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            print("here", rows)
            mysql_db.close()
            return all_rows
        except Exception as exc:
            return exc

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        one_row = {}
        all_rows = []
        try:
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"SELECT * FROM {table_name} WHERE {where};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for r in rows:
                one_row["id"] = r[0]
                one_row["name"] = r[1]
                one_row["age"] = r[2]
                all_rows.append(one_row.copy())
            mysql_db.close()
            return all_rows
        except Exception as exc:
            return exc

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            where = 'and '.join(f"{k} = '{v}'" for k, v in filters.items())
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"DELETE FROM {table_name} WHERE {where};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            return f"Records deleted from table {table_name.title()}"
        except Exception as exc:
            return exc

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"DELETE FROM {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.commit()
            mysql_db.close()
            return f"All records deleted from table {table_name.title()}"
        except Exception as exc:
            return exc

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"TRUNCATE TABLE {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return f"Table {table_name.title()} was truncated"
        except Exception as exc:
            return exc

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True, database=schema_name)
            query = f"DROP TABLE {table_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return f"Table {table_name.title()} was dropped from database"
        except Exception as exc:
            return exc

    def drop_schema(self, host, username, password, schema_name):
        try:
            mysql_db = conn.connect(host=host, user=username, passwd=password, use_pure=True)
            query = f"DROP DATABASE {schema_name};"
            cursor = mysql_db.cursor()
            cursor.execute(query)
            mysql_db.close()
            return "Database has been dropped successfully"
        except Exception as exc:
            return exc
