import argparse
import logging
import time
from DataBasesParser import Connector, DataAnalyzer

# Определение логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Создание парсера аргументов командной строки
parser = argparse.ArgumentParser(
    description='Analysis of the log file using databases')
parser.add_argument('--db_type', type=str, choices=[
                    'mysql', 'postgresql', 'sqlite', 'h2', 'mongodb', 'redis'], help='Database type')
parser.add_argument('--db_name', type=str, help='Database name')
parser.add_argument('--import_data', action='store_true',
                    help='Import log data')
parser.add_argument('--ip_user_agent_statistics',
                    action='store_true', help='Get IP and User-Agent statistics')
parser.add_argument('--request_frequency',
                    action='store_true', help='Get request frequency')
parser.add_argument('--top_user_agents', action='store_true',
                    help='Get top user agents')
parser.add_argument('--errors_50x', action='store_true', help='Get 50x errors')
parser.add_argument('--longest_or_shortest_queries',
                    action='store_true', help='Get longest or shortest queries')
parser.add_argument('--top_requests_to_kth_slash',
                    action='store_true', help='Get top requests to Kth slash')
parser.add_argument('--count_by_upstream',
                    action='store_true', help='Get count by upstream')
parser.add_argument('--conversion_statistics',
                    action='store_true', help='Get conversion statistics')
parser.add_argument('--outgoing_requests_30s', action='store_true',
                    help='Get outgoing requests in the last 30 seconds')
parser.add_argument('--outgoing_requests_1m', action='store_true',
                    help='Get outgoing requests in the last 1 minute')
parser.add_argument('--outgoing_requests_5m', action='store_true',
                    help='Get outgoing requests in the last 5 minutes')
parser.add_argument('--largest_request_periods',
                    action='store_true', help='Get largest request periods')

# Парсинг аргументов командной строки
args = parser.parse_args()

# Проверка наличия обязательных аргументов
if not args.db_type or not args.db_name:
    parser.error('Database type and name are required')

# Подключение к базе данных
db_connection = Connector.DatabaseConnection(args.db_type, args.db_name)
db_connection.connect()

# Выполнение операции импорта данных, если указан аргумент --import_data
if args.import_data:
    start_time = time.time()
    db_connection.import_log_data('access_log')
    end_time = time.time()
    execution_time_import = end_time - start_time
    logger.info(
        f"Время выполнения import_log_data: {execution_time_import:.2f} сек")

# Создание экземпляра класса Analyzer
analyzer = DataAnalyzer.Analyzer(db_connection, args.db_type)

# Выполнение выбранных операций анализа данных
if args.ip_user_agent_statistics:
    logger.info("Executing get_ip_user_agent_statistics method...")
    start_time = time.time()
    statistics = analyzer.get_ip_user_agent_statistics(5)
    end_time = time.time()
    execution_time = end_time - start_time
    for ip_address, referer, balancer_worker_name, user_agent, count in statistics:
        print(f"IP Address: {ip_address}")
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print(f"Count: {count}")
        print("-------------------")

    logger.info(
        f"get_ip_user_agent_statistics executed in {execution_time:.2f} seconds")

if args.request_frequency:
    logger.info("Executing get_request_frequency method...")
    start_time = time.time()
    request_frequency = analyzer.get_request_frequency(2)
    end_time = time.time()
    execution_time = end_time - start_time
    for ip_address, referer, balancer_worker_name, user_agent, frequency in request_frequency:
        print(f"IP Address: {ip_address}")
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print(f"frequency: {frequency}")
        print("-------------------")
    logger.info(
        f"get_request_frequency executed in {execution_time:.2f} seconds")

if args.top_user_agents:
    logger.info("Executing get_top_user_agents method...")
    start_time = time.time()
    top_user_agents = analyzer.get_top_user_agents(10)
    end_time = time.time()
    execution_time = end_time - start_time
    for referer, balancer_worker_name, user_agent, frequency, Count in top_user_agents:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print(f"frequency: {frequency}")
        print(f"Count: {Count}")
        print("-------------------")
    logger.info(
        f"get_top_user_agents executed in {execution_time:.2f} seconds")

if args.errors_50x:
    logger.info("Executing get_50x_errors method...")
    start_time = time.time()
    errors = analyzer.get_50x_errors('500', 30)
    end_time = time.time()
    execution_time = end_time - start_time
    for error in errors:
        print(error)  # Вывод ошибок
    logger.info(
        f"get_50x_errors executed in {execution_time:.2f} seconds")

if args.longest_or_shortest_queries:
    logger.info("Executing get_longest_or_shortest_queries method...")
    start_time = time.time()
    longest_queries = analyzer.get_longest_or_shortest_queries(
        10, longest=True)
    end_time = time.time()
    execution_time = end_time - start_time
    for query in longest_queries:
        print(f"Query: {query}")
        print("-------------------")
    logger.info(
        f"get_longest_or_shortest_queries executed in {execution_time:.2f} seconds")

    # Вывод результатов

if args.top_requests_to_kth_slash:
    logger.info("Executing get_top_requests_to_kth_slash method...")
    start_time = time.time()
    top_requests = analyzer.get_top_requests_to_kth_slash(5, 2)
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent in top_requests:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print("-------------------")
    logger.info(
        f"get_top_requests_to_kth_slash executed in {execution_time:.2f} seconds")

if args.count_by_upstream:
    logger.info("Executing get_upstream_requests method...")
    start_time = time.time()
    count_by_upstream = analyzer.get_upstream_requests()
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent, Count in count_by_upstream:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print(f"Count: {Count}")
        print("-------------------")
    logger.info(
        f"get_upstream_requests executed in {execution_time:.2f} seconds")

if args.conversion_statistics:
    logger.info("Executing get_conversion_statistics method...")
    start_time = time.time()
    conversion_statistics = analyzer.get_conversion_statistics('domain')
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent, domain, Count in conversion_statistics:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print(f"domain: {domain}")
        print(f"Count: {Count}")
        print("-------------------")
    logger.info(
        f"get_conversion_statistics executed in {execution_time:.2f} seconds")

if args.outgoing_requests_30s:
    logger.info("Executing get_outgoing_requests_30s method...")
    start_time = time.time()
    result_30s = analyzer.get_outgoing_requests_30s()
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent in result_30s:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print("-------------------")
    logger.info(
        f"get_outgoing_requests_30s executed in {execution_time:.2f} seconds")

if args.outgoing_requests_1m:
    logger.info("Executing get_outgoing_requests_1m method...")
    start_time = time.time()
    result_1m = analyzer.get_outgoing_requests_1m()
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent in result_1m:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print("-------------------")
    logger.info(
        f"get_outgoing_requests_1m executed in {execution_time:.2f} seconds")

if args.outgoing_requests_5m:
    logger.info("Executing get_outgoing_requests_5m method...")
    start_time = time.time()
    result_5m = analyzer.get_outgoing_requests_5m()
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent in result_5m:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print("-------------------")
    logger.info(
        f"get_outgoing_requests_5m executed in {execution_time:.2f} seconds")

if args.largest_request_periods:
    logger.info("Executing get_largest_request_periods method...")
    start_time = time.time()
    largest_request_periods = analyzer.get_largest_request_periods(5)
    end_time = time.time()
    execution_time = end_time - start_time
    for forwarded_for, referer, balancer_worker_name, user_agent in largest_request_periods:
        print(f"User-Agent: {user_agent}")
        print(f"referer:    {referer}")
        print(f"balancer_worker_name:    {balancer_worker_name}")
        print("-------------------")
    logger.info(
        f"get_largest_request_periods executed in {execution_time:.2f} seconds")

# Закрытие соединения с базой данных
db_connection.close()
