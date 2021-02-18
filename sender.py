import redis
from datetime import datetime
from time import strftime, gmtime
import json
import requests
import threading
import pickle

url = "[your api endpoint here]"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime):
            return str(z)
        else:
            return super().default(z)


def post_data(_record):
    try:

        # send data to api
        headers = {'Content-type': 'application/json',
                   'Accept': 'application/json',
                   "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5"}
        json_data = json.dumps(_record, cls=DateTimeEncoder, indent=4)
        response = requests.post(url, data=json_data, headers=headers)
        print(f"Status Code: {response.status_code}")
    except requests.ConnectionError as e:
        print(f"Error Occurred: {e}")


class Sender(threading.Thread):
    def __init__(self, channels, identifier=0):
        threading.Thread.__init__(self)
        self.r_cli = redis.Redis(host='localhost', port=6379, db=0)
        self.identifier = identifier
        self.channels = channels

    def work(self, serialized_data):
        try:
            payload = pickle.loads(serialized_data)
            print(payload)
            post_data(payload)
        except pickle.UnpicklingError:
            self.log('unpack_error_test', serialized_data)

    def run(self):
        print(f"Started listener {self.identifier} at channel: {self.channels}")
        while True:
            item = self.r_cli.blpop(self.channels, 0)[1]
            if item == "KILL":
                break
            else:
                self.work(item)
        print(f"{self} {self.identifier} unsubscribed and finished")

    def log(self, key, item):
        print(key, item)
        self.r_cli.hset(key, str(datetime.now()), pickle.dumps(item))


if __name__ == '__main__':
    print(f'Redis Sensors Data Server {strftime("%d %b %H:%M:%S", gmtime())}')

    for i in range(1, 11):
        client = Sender(channels="GPSSensorsData", identifier=i)
        client.start()
