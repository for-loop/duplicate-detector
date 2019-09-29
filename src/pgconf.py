import json

class PostgresConfigurator:

    def __init__(self, path='../postgres_credentials.json'):
        with open(path, "r") as read_file:
            self._auth = json.load(read_file)

        self._url = "jdbc:postgresql://{}:{}/{}".format(self._auth['host'], self._auth['port'], self._auth['database'])
        self._properties = {}
        self._properties['username'] = self._auth['user']
        self._properties['password'] = self._auth['password']
        self._properties['driver'] = "org.postgresql.Driver"

    def get_url(self):
        return self._url

    def get_properties(self):
        return self._properties
