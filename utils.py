from functools import reduce
import requests


def join(string, pattern=","):
    return reduce(lambda x, y: "{}{}{}".format(x, pattern, y), string)


class RequestHandler(object):
    def __init__(self, host, port, company):
        self.url = 'http://{}:{}/?/{}'.format(host, port, company).replace('?', '{}')

    def get(self, service, param):
        return requests.get(self.url.format(service), param)
