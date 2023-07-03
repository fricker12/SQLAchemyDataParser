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
