import re
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
from pymongo import MongoClient
import redis


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
                'password': '12345678'
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

    def create_import_table(self):
        import_table = Table('import', self.metadata,
                             Column('id', Integer, primary_key=True),
                             Column('ip_address', String(255)),
                             Column('forwarded_for', String(255)),
                             Column('timestamp', String(255)),
                             Column('request', String(255)),
                             Column('status_code', Integer),
                             Column('response_size', Integer),
                             Column('time_taken', Integer),
                             Column('referer', String(255)),
                             Column('user_agent', String(255)),
                             Column('balancer_worker_name', String(255)))

        import_table_exists = self.engine.has_table('import')

        if not import_table_exists:
            import_table.create(self.engine)
        else:
            # Очистка таблицы перед импортом данных
            self.engine.execute(import_table.delete())

    def import_log_data(self, log_file):
        regex = r'^(?P<ip_address>\S+) \((?P<forwarded_for>\S+)\) - - \[(?P<timestamp>[\w:/]+\s[+\-]\d{4})\] "(?P<request>[A-Z]+ \S+ \S+)" (?P<status_code>\d+) (?P<response_size>\d+) (?P<time_taken>\d+) (?P<balancer_worker_name>\d+) "(?P<Referer>[^"]*)" "(?P<user_agent>[^"]*)"'
        pattern = re.compile(regex)

        if self.db_type == 'mongodb':
            self.collection = self.db['import']
            with open(log_file, 'r') as file:
                for line in file:
                    match = pattern.match(line)
                    if match:
                        data = match.groupdict()
                        self.collection.insert_one(data)
        elif self.db_type == 'redis':
            r = redis.Redis(host=self.db_params['redis']['host'], port=self.db_params['redis']['port'])
            with open(log_file, 'r') as file:
                for line in file:
                    match = pattern.match(line)
                    if match:
                        data = match.groupdict()
                        r.hmset('import', data)
        else:
            self.create_import_table()

            with open(log_file, 'r') as file:
                for line in file:
                    match = pattern.match(line)
                    if match:
                        data = match.groupdict()
                        self.engine.execute(self.metadata.tables['import'].insert().values(data))

    def close(self):
        if self.db_type == 'mongodb':
            self.db.client.close()
        else:
            self.engine.dispose()


# Пример использования класса
db_type = 'mysql'
db_name = 'mydatabase'

db_connection = DatabaseConnection(db_type, db_name)
db_connection.connect()
db_connection.import_log_data('access_log')

db_connection.close()
