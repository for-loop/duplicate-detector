import os

class PostgresConfigurator:

    def __init__(self):
        self._url = "jdbc:postgresql://{}:{}/{}".format(os.environ['POSTGRES_HOST_PRIVATE'], os.environ['POSTGRES_PORT'], os.environ['POSTGRES_DATABASE'])
        self._url_w_password = 'postgresql://{}:{}@{}:{}/{}'.format(os.environ['POSTGRES_USER'], os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_HOST_PRIVATE'], os.environ['POSTGRES_PORT'], os.environ['POSTGRES_DATABASE'])
        self._properties = {}
        self._properties['username'] = os.environ['POSTGRES_USER']
        self._properties['password'] = os.environ['POSTGRES_PASSWORD']
        self._properties['driver'] = "org.postgresql.Driver"

    def get_url(self):
        return self._url

    def get_url_w_password(self):
        return self._url_w_password

    def get_properties(self):
        return self._properties
