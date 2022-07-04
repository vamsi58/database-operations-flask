from .database import Database
import pymongo, csv, sys


class MongoDatabse(Database):
    def create_schema(self, host, username, password, schema_name):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db["db_log"]
            record = {"event": "database created"}
            collection.insert_one(record)
            return "Database has been created successfully"
        except Exception as exc:
            return exc

    def drop_schema(self, host, username, password, schema_name):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            client.drop_database(schema_name)
            return f"Database {schema_name.title()} has been dropped successfully"
        except Exception as exc:
            return exc

    def create_table(self, host, username, password, schema_name, table_name, **columns):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            columns["__table structure__"] = "This is to show the column constraints"
            collection.insert_one(columns)
            return f"Table {table_name.title()} has been created successfully"
        except Exception as exc:
            return exc

    def drop_table(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            collection.drop()
            return f"Table {table_name.title()} has been dropped successfully"
        except Exception as exc:
            return exc

    def insert_record(self, host, username, password, schema_name, table_name, **columns):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            collection.insert_one(columns)
            return f"Row inserted into table {table_name.title()}"
        except Exception as exc:
            return exc

    def insert_multiple_records(self, host, username, password, schema_name, table_name, input_file_name, *columns):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            with open(input_file_name, encoding="utf-8-sig") as csv_f:
                csv_r = csv.reader(csv_f)
                for row in csv_r:
                    key_values = {columns[i]: row[i] for i in range(len(columns))}
                    collection.insert_one(key_values)
            return f"Rows inserted into table {table_name.title()}"
        except Exception as exc:
            print("Unexpected database error:", sys.exc_info()[0])

    def update_records(self, host, username, password, schema_name, table_name, filters, **updated_values):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            new_values = {"$set":updated_values}
            print("before update", new_values)
            collection.update_many(filters, new_values)
            return "Documents updated successfully!"
        except Exception as exc:
            return str(exc)

    def retrieve_records(self, host, username, password, schema_name, table_name):
        try:
            result = []
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            rows = collection.find({},{ "_id": 0, "id": 1, "name": 1, "age":1 })
            for r in rows:
                result.append(r)
            return result
        except Exception as exc:
            return exc

    def retrieve_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            result = []
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            rows = collection.find(filters, {"_id": 0, "id": 1, "name": 1, "age": 1})
            for r in rows:
                result.append(r)
            return result
        except Exception as exc:
            return exc

    def delete_records_with_filter(self, host, username, password, schema_name, table_name, **filters):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many(filters)
            return "Documents deleted successfully!"
        except Exception as exc:
            return str(exc)

    def delete_records(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            collection.delete_many({})
            return "Documents deleted successfully!"
        except Exception as exc:
            return str(exc)

    def truncate_table(self, host, username, password, schema_name, table_name):
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = client[schema_name]
            collection = db[table_name]
            collection.remove({ })
            return "Documents deleted successfully!"
        except Exception as exc:
            return str(exc)
