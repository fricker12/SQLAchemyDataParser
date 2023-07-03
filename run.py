import logging
import time
from DataBasesParser import Connector,DataAnalyzer

# Определение логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Остальной код программы...

db_type = 'mysql'
db_name = 'mydatabase'

db_connection = Connector.DatabaseConnection(db_type, db_name)
db_connection.connect()

start_time = time.time()
db_connection.import_log_data('access_log')
end_time = time.time()
execution_time_import = end_time - start_time

analyzer = DataAnalyzer.Analyzer(db_connection, db_type)

start_time = time.time()
statistics = analyzer.get_ip_user_agent_statistics(5)
end_time = time.time()
execution_time_statistics = end_time - start_time

for ip_address, referer, balancer_worker_name, user_agent, count in statistics:
    print(f"IP Address: {ip_address}")
    print(f"User-Agent: {user_agent}")
    print(f"referer:    {referer}")
    print(f"balancer_worker_name:    {balancer_worker_name}")
    print(f"Count: {count}")
    print("-------------------")

# Выводим информацию о времени выполнения
logger.info(f"Время выполнения import_log_data: {execution_time_import:.2f} сек")
logger.info(f"Время выполнения get_ip_user_agent_statistics: {execution_time_statistics:.2f} сек")

db_connection.close()