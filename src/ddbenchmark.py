import time
import sqlalchemy

class Benchmark:

    def __init__(self, method_name, dir_name, version, pg_url):
        self._start_time = time.time()
        self._method_name = method_name
        self._dir_name = dir_name
        self._version = version
        self._engine = sqlalchemy.create_engine(pg_url)

        # Make sure the tables exist
        for table_name in ['benchmarks', 'methods', 'directories', 'versions']:
            if not self._table_exists(table_name):
                self._create_table(table_name)

    def _table_exists(self, table_name):
        '''
        Check if the table exists
        Returns True or False
        '''
        res = self._engine.execute("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = '{}');".format(table_name))

        return res.fetchone()[0]

    def _create_table(self, table_name):
        '''
        Create table
        '''
        if table_name == 'benchmarks':
            self._engine.execute('''CREATE TABLE benchmarks(
                                      benchmark_id SERIAL PRIMARY KEY,
                                      method_id INT NOT NULL,
                                      directory_id INT NOT NULL,
                                      version_id INT NOT NULL,
                                      num_images BIGINT NOT NULL,
                                      bytes BIGINT NOT NULL,
                                      seconds FLOAT NOT NULL,
                                      timestamp TIMESTAMP NOT NULL);''')
        elif table_name == 'methods':
            self._engine.execute('''CREATE TABLE methods(
                                      method_id SERIAL PRIMARY KEY,
                                      method VARCHAR (50) NOT NULL);''')
        elif table_name == 'directories':
            self._engine.execute('''CREATE TABLE directories(
                                      directory_id SERIAL PRIMARY KEY,
                                      directory VARCHAR (15) NOT NULL);''')
        elif table_name == 'versions':
            self._engine.execute('''CREATE TABLE versions(
                                      version_id SERIAL PRIMARY KEY,
                                      version VARCHAR (10) NOT NULL);''')

    def _value_exists(self, table_name, column_name, value):
        '''
        Check if the value exists in a table
        '''
        res = self._engine.execute("SELECT COUNT(*) FROM {} WHERE {} = '{}';".format(table_name, column_name, value))

        return (res.fetchone()[0])

    def _get_single_value(self, query):
        '''
        Return a single value
        '''
        res = self._engine.execute(query)
        
        return res.fetchone()[0]

    def log(self):
        '''
        Log benchmark to the database
        '''
        elapsed_time = time.time() - self._start_time

        if not self._value_exists('methods', 'method', self._method_name):
            self._engine.execute("INSERT INTO methods (method) VALUES ('{}');".format(self._method_name))
        method_id = self._get_single_value("SELECT method_id FROM methods WHERE method = '{}';".format(self._method_name))

        if not self._value_exists('directories', 'directory', self._dir_name):
            self._engine.execute("INSERT INTO directories (directory) VALUES ('{}');".format(self._dir_name))
        directory_id = self._get_single_value("SELECT directory_id FROM directories WHERE directory = '{}';".format(self._dir_name))

        if not self._value_exists('versions', 'version', self._version):
            self._engine.execute("INSERT INTO versions (version) VALUES ('{}')".format(self._version))
        version_id = self._get_single_value("SELECT version_id FROM versions WHERE version = '{}';".format(self._version))

        table_name_images = "images_{}_{}".format(self._method_name, self._dir_name)
        table_name_contents = "contents_{}_{}".format(self._method_name, self._dir_name)

        num_images = self._get_single_value("SELECT COUNT(*) FROM {}".format(table_name_images))

        table_size_images = self._get_single_value("SELECT pg_total_relation_size('{}')".format(table_name_images))
        table_size_contents = self._get_single_value("SELECT pg_total_relation_size('{}');".format(table_name_contents))

        total_size = table_size_images + table_size_contents
        print("Elapsed time: {} s".format(elapsed_time))
        print("Total size: {} bytes".format(total_size))
        print("Version: {}".format(self._version))
        print("Num Images: {}".format(num_images))

        self._engine.execute("INSERT INTO benchmarks (method_id, directory_id, version_id, num_images, bytes, seconds, timestamp) VALUES ({}, {}, {}, {}, {}, {}, NOW() AT TIME ZONE 'US/Pacific');".format(method_id, directory_id, version_id, num_images, total_size, elapsed_time))
