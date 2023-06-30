from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
from pymongo import MongoClient

class DatabaseConnection:
    def __init__(self, db_type, db_name):
        self.db_type = db_type
        self.db_name = db_name
        self.db_params = {
            'mysql': {
                'driver': 'mysql+pymysql',
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'password'
            },
            'postgresql': {
                'driver': 'postgresql+psycopg2',
                'host': 'localhost',
                'port': 5432,
                'user': 'postgres',
                'password': 'password'
            },
            'sqlite': {
                'driver': 'sqlite',
                'database': 'data.db'
            },
            'h2': {
                'driver': 'h2',
                'url': 'jdbc:h2:mem:test;DB_CLOSE_DELAY=-1'
            },
            'mongodb': {
                'driver': 'mongodb',
                'host': 'localhost',
                'port': 27017
            },
            'redis': {
                'driver': 'redis',
                'host': 'localhost',
                'port': 6379
            }
        }

    def connect(self):
        db_params = self.db_params[self.db_type]

        if self.db_type == 'sqlite':
            connection_string = f"{db_params['driver']}:///{db_params['database']}"
        elif self.db_type == 'mongodb':
            client = MongoClient(db_params['host'], db_params['port'])
            self.db = client[self.db_name]
            return
        elif self.db_type == 'h2':
            connection_string = f"{db_params['driver']}:{db_params['url']}"
        else:
            connection_string = f"{db_params['driver']}://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{self.db_name}"

        self.engine = create_engine(connection_string)
        self.metadata = MetaData()

        if self.db_type in ['mysql', 'postgresql']:
            # Проверка существования базы данных
            with self.engine.connect() as connection:
                exists = connection.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{self.db_name}'" if self.db_type == 'postgresql' else f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{self.db_name}'")).fetchone()

                if not exists:
                    connection.execute(text(f"CREATE DATABASE {self.db_name}" if self.db_type == 'postgresql' else f"CREATE DATABASE IF NOT EXISTS {self.db_name}"))

    def create_import_collection(self):
        if self.db_type == 'mongodb':
            self.collection = self.db['import']
            return

        import_table = Table('import', self.metadata,
                             Column('id', Integer, primary_key=True),
                             Column('data', String(255)))
        import_table.create(self.engine)

    def insert_document(self, data):
        if self.db_type == 'mongodb':
            self.collection.insert_one(data)
        else:
            raise NotImplementedError("Insert operation is not supported for this database type.")

    def close(self):
        if self.db_type == 'mongodb':
            self.db.client.close()
        else:
            self.engine.dispose()

# Пример использования класса
db_type = 'h2'
db_name = 'mydatabase'

db_connection = DatabaseConnection(db_type, db_name)
db_connection.connect()
db_connection.create_import_collection()

# Вставка документа в MongoDB
data = {'name': 'John Doe', 'age': 30}
db_connection.insert_document(data)

db_connection.close()
