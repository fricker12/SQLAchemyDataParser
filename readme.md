# Parsing Access log with databases

## Requirements

You need the followin to be able to run this code:

## Usage

First install the script and it's requirements:

```
git clone [https://github.com/fricker12/DataBasesPytonParsingLog](https://github.com/fricker12/SQLAchemyDataParser)
cd SQLAchemyDataParser

```
Then run the script as follows:
```
Примеры запуска команд

python run.py --db_type mysql --db_name mydatabase --import_data --ip_user_agent_statistics --request_frequency

В этом примере мы выбираем MySQL в качестве типа базы данных, указываем имя базы данных mydatabase и запускаем операцию импорта данных, а затем выполняем методы get_ip_user_agent_statistics и get_request_frequency.


# Получение частоты запросов и получение топ-10 User-Agent
python run.py --db_type mysql --db_name mydatabase --request_frequency --top_user_agents

# Получение ошибок 50x и получение статистики конверсии по домену
python run.py --db_type mysql --db_name mydatabase --50x_errors --conversion_statistics

# Получение самых длинных запросов и получение количества запросов по upstream
python run.py --db_type mysql --db_name mydatabase --longest_or_shortest_queries --count_by_upstream

# Получение топ-5 запросов к K-му слэшу и получение самых больших периодов запросов
python run.py --db_type mysql --db_name mydatabase --top_requests_to_kth_slash --largest_request_periods

# Получение исходящих запросов за последние 30 секунд и за последнюю минуту
python run.py --db_type mysql --db_name mydatabase --outgoing_requests_30s --outgoing_requests_1m
