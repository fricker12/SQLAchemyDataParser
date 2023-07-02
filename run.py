# ...
from sqlalchemy import Boolean, create_engine, MetaData, Table, Column, Integer, String, Text, text
from sqlalchemy.inspection import inspect
from sqlalchemy import quoted_name
from pymongo import MongoClient
import redis
import re
import logging
import time

# Определение логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

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
            connection_string = f"{db_params['driver']}://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}"

        self.engine = create_engine(connection_string)
        self.metadata = MetaData()

        if self.db_type in ['mysql', 'postgresql']:
            # Проверить, существует ли база данных
            with self.engine.connect() as connection:
                if self.db_type == 'postgresql':
                    exists = connection.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{self.db_name}'")).fetchone()
                else:  # Для MySQL
                    exists = connection.execute(text(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{self.db_name}'")).fetchone()

                if not exists:
                    # Создать базу данных
                    connection.execute(text(f"CREATE DATABASE {self.db_name}" if self.db_type == 'postgresql' else f"CREATE DATABASE IF NOT EXISTS {self.db_name}"))
                    # Повторно подключиться к созданной базе данных
                    connection.close()
                    self.engine = create_engine(connection_string)

    def create_import_table(self):
        import_table = Table('import', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('ip_address', Text(length=50), nullable=True),
                            Column('forwarded_for', Text(length=3000), nullable=True),
                            Column('timestamp', Text(length=1000), nullable=True),
                            Column('request', Text(length=5000), nullable=True),
                            Column('status_code', Integer),
                            Column('response_size', Integer),
                            Column('time_taken', Integer),
                            Column(quoted_name('avb', quote=True), Text(length=5000),nullable=True),
                            Column('user_agent', Text(length=5000), nullable=True),
                            Column('balancer_worker_name', Text(length=5000), nullable=True))

        with self.engine.connect() as connection:
            # Выбор базы данных
            connection.execute(text(f"USE {self.db_name}"))
            inspector = inspect(connection)
            import_table_exists = inspector.has_table('import', schema=self.db_name)
            if not import_table_exists:
                import_table.create(connection)
            else:
                #Очистите таблицу импорта перед импортом данных
                connection.execute(import_table.delete())
                connection.commit()

    def import_log_data(self, log_file):
        regex = r'^(?P<ip_address>\S+) \((?P<forwarded_for>\S+)\) - - \[(?P<timestamp>[\w:/]+\s[+\-]\d{4})\] "(?P<request>[A-Z]+ \S+ \S+)" (?P<status_code>\d+) (?P<response_size>\d+) (?P<time_taken>\d+) (?P<balancer_worker_name>\d+) "(?P<avb>[^"]*)" "(?P<user_agent>[^"]*)"'
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
            with self.engine.connect() as connection:
                import_table = self.metadata.tables['import']  # Получаем объект таблицы Import
                with open(log_file, 'r') as file:
                    values_list = []  # Store the values to be inserted
                    for line in file:
                        match = pattern.match(line)
                        if match:
                            data = match.groupdict()
                            try:
                                values = {}
                                for column, value in data.items():
                                    if column in import_table.columns:
                                        column_obj = import_table.columns[column]
                                        # Выполнить преобразование типа на основе типа столбца
                                        if isinstance(column_obj.type, Integer):
                                            value = int(value)
                                        elif isinstance(column_obj.type, String):
                                            value = str(value)
                                        # Добавить преобразованное значение в словарь
                                        values[column_obj] = value
                                values_list.append(values)
                            except Exception as e:
                                print(f"An error occurred: {e}")

                    connection.execute(import_table.insert().values(values_list))
                    connection.commit()
    def close(self):
        if self.db_type == 'mongodb':
            self.db.client.close()
        else:
            self.engine.dispose()


db_type = 'mysql'
db_name = 'mydatabase'

db_connection = DatabaseConnection(db_type, db_name)
db_connection.connect()
start_time = time.time()  # Запоминаем время начала выполнения
db_connection.import_log_data('access_log')
end_time = time.time()  # Запоминаем время окончания выполнения
execution_time = end_time - start_time  # Вычисляем время выполнения
# Выводим информацию о времени выполнения
logger.info(f"Время выполнения: {execution_time} сек")
db_connection.close()
