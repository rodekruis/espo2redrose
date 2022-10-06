import requests
import urllib
import json


class RedRoseAPIError(Exception):
    """An exception class for the client"""


def http_build_query(data):
    parents = list()
    pairs = dict()

    def renderKey(parents):
        depth, outStr = 0, ''
        for x in parents:
            s = "[%s]" if depth > 0 or isinstance(x, int) else "%s"
            outStr += s % str(x)
            depth += 1
        return outStr

    def r_urlencode(data):
        if isinstance(data, list) or isinstance(data, tuple):
            for i in range(len(data)):
                parents.append(i)
                r_urlencode(data[i])
                parents.pop()
        elif isinstance(data, dict):
            for key, value in data.items():
                parents.append(key)
                r_urlencode(value)
                parents.pop()
        else:
            pairs[renderKey(parents)] = str(data)

        return pairs
    return urllib.parse.urlencode(r_urlencode(data))


class RedRoseAPI:

    url_path = '/externalapi/'

    def __init__(self, url, api_user, api_key, module):
        self.url = url
        self.api_user = api_user
        self.api_key = api_key
        self.module = module
        self.status_code = None

    def request(self, method, action, params=None, files=None):

        kwargs = {
            'url': self.normalize_url(action),
            'auth': (self.api_user, self.api_key),
        }

        if files is not None:
            kwargs['files'] = [('keyValuePair', ('keyValuePair', json.dumps(files), 'application/json'))]
        if params is not None:
            kwargs['url'] = kwargs['url'] + '?' + http_build_query(params)

        response = requests.request(method, **kwargs)

        self.status_code = response.status_code

        if self.status_code != 200:
            reason = self.parse_reason(response.headers)
            raise RedRoseAPIError(f'Wrong request, status code is {response.status_code}, reason is {reason}')

        data = response.content
        if not data:
            raise RedRoseAPIError('Wrong request, content response is empty')

        return response.json()

    def normalize_url(self, action):
        return self.url + self.url_path + 'modules/' + self.module + '/' + action

    @staticmethod
    def parse_reason(headers):
        if 'X-Status-Reason' not in headers:
            return 'Unknown Error'

        return headers['X-Status-Reason']