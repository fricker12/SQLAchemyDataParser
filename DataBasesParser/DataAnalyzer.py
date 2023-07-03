from sqlalchemy import Text, text
import redis
import time
import logging
class Analyzer:
    def __init__(self, db_connection, db_type):
        self.db_connection = db_connection
        self.db_type = db_type
        
    @staticmethod
    def log_execution_time(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger = logging.getLogger(__name__)
            logger.info(f"Method {func.__name__} executed in {execution_time:.2f} seconds")
            return result
        return wrapper
    
    @log_execution_time
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
            
    @log_execution_time        
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
    
    def get_request_frequency(self, dT): 
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name}, COUNT(*) AS frequency "
                    "FROM import "
                    "WHERE timestamp >= NOW() - INTERVAL :interval MINUTE "
                    "GROUP BY forwarded_for, referer, user_agent, balancer_worker_name "
                    "ORDER BY frequency DESC")

        return self.execute_query(query.bindparams(interval=dT))
    
    def get_top_user_agents(self, N):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name}, COUNT(*) AS frequency "
                    "FROM import "
                    "GROUP BY forwarded_for, referer, user_agent, balancer_worker_name "
                    "ORDER BY frequency DESC "
                    f"LIMIT {N}")

        return self.execute_query(query)
    
    def get_50x_errors(self, S, dT):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        timestamp = import_table.c.timestamp

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"WHERE status_code LIKE '5%' AND timestamp >= NOW() - INTERVAL {dT} MINUTE "
                    f"GROUP BY forwarded_for, referer, user_agent, balancer_worker_name")

        return self.execute_query(query)
    
    def get_longest_or_shortest_queries(self, N, longest=True):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        time_taken = import_table.c.time_taken

        sort_order = "DESC" if longest else "ASC"

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"ORDER BY time_taken {sort_order} "
                    f"LIMIT {N}")

        return self.execute_query(query)
    
    def get_top_requests_to_kth_slash(self, N, K):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        request = import_table.c.request

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(request, '/', {K+1}), '/', -1) = 'merlin-service-search' "
                    f"GROUP BY forwarded_for, referer, user_agent, balancer_worker_name "
                    f"ORDER BY COUNT(*) DESC "
                    f"LIMIT {N}")

        return self.execute_query(query)
    
    def get_upstream_requests(self):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name}, COUNT(*) as request_count "
                    f"FROM import "
                    f"GROUP BY forwarded_for, referer, user_agent, balancer_worker_name")

        return self.execute_query(query)
    
    def get_conversion_statistics(self, sort_by):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name}, "
                    f"SUBSTRING_INDEX(referer, '/', 3) AS domain, COUNT(*) AS transitions "
                    f"FROM import "
                    f"WHERE referer IS NOT NULL "
                    f"GROUP BY forwarded_for, referer, user_agent, balancer_worker_name, domain "
                    f"ORDER BY {sort_by}")

        return self.execute_query(query)
    
    def get_outgoing_requests_30s(self):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        timestamp = import_table.c.timestamp

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"WHERE timestamp >= NOW() - INTERVAL 30 SECOND")

        with self.db_connection.engine.connect() as connection:
            result = connection.execute(query)
            return result.fetchall()
        
    def get_outgoing_requests_1m(self):
        
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        timestamp = import_table.c.timestamp

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"WHERE timestamp >= NOW() - INTERVAL 1 MINUTE")

        with self.db_connection.engine.connect() as connection:
            result = connection.execute(query)
            return result.fetchall()
        
    def get_outgoing_requests_5m(self):
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        timestamp = import_table.c.timestamp

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"WHERE timestamp >= NOW() - INTERVAL 5 MINUTE")

        with self.db_connection.engine.connect() as connection:
            result = connection.execute(query)
            return result.fetchall()
        
    def get_largest_request_periods(self, N):
        import_table = self.db_connection.metadata.tables['import']
        forwarded_for = import_table.c.forwarded_for
        referer = import_table.c.referer
        user_agent = import_table.c.user_agent
        balancer_worker_name = import_table.c.balancer_worker_name
        timestamp = import_table.c.timestamp

        query = text(f"SELECT {forwarded_for}, {referer}, {user_agent}, {balancer_worker_name} "
                    f"FROM import "
                    f"GROUP BY DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i'), "
                    f"TIME_FORMAT(timestamp, '%H:%i') "
                    f"ORDER BY COUNT(*) DESC "
                    f"LIMIT {N}")

        return self.execute_query(query)
    