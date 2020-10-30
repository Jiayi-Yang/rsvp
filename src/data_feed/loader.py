import json
import requests

url = 'https://stream.meetup.com/2/rsvps'


def load_real_time(url):
    response = requests.get(url, stream=True)
    for line in response.iter_lines():
        if line:
            rspv_data = json.loads(line)
            print(rspv_data)


if __name__ == '__main__':
    load_real_time(url)
