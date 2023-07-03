from sqlalchemy import Text, text
import redis
class Analyzer:
    def __init__(self, db_connection, db_type):
        self.db_connection = db_connection
        self.db_type = db_type

    def execute_query(self, query):
        if self.db_type == 'mongodb':
            return self.db_connection.collection.find(query)
        elif self.db_type == 'redis':
            r = redis.Redis(host=self.db_connection.db_params['redis']['host'], port=self.db_connection.db_params['redis']['port'])
            return r.hgetall('import')
        else:
            with self.db_connection.engine.connect() as connection:
                result = connection.execute(query)
                return result.fetchall()
            
    def get_ip_user_agent_statistics(self, n): 
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        user_agent = import_table.c.user_agent
        referer = import_table.c.referer
        balancer_worker_name = import_table.c.balancer_worker_name
        
        query = text(f"SELECT {forwarded_for}, {user_agent}, {referer}, {balancer_worker_name}, COUNT(*) AS count "
                 "FROM import "
                 "GROUP BY forwarded_for, user_agent, referer, balancer_worker_name "
                 "ORDER BY count DESC "
                 f"LIMIT {n}")

        return self.execute_query(query)