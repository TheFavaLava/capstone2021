import time
from json import dumps, loads
import requests


class Client:

    def __init__(self):
        self.url = 'http://localhost:8080'
        self.client_id = -1

    def get_client_id(self):
        client_id = read_client_id()
        if client_id == -1:
            client_id = self.request_client_id()
        self.client_id = client_id

    def request_client_id(self):
        endpoint = self.url + '/get_client_id'
        response = assure_request(requests.get, endpoint)
        client_id = int(response.json()['client_id'])
        write_client_id(client_id)
        return client_id

    def get_job(self):
        endpoint = self.url + '/get_job'
        client_id = self.client_id
        data = {'client_id': client_id}
        job_id, work = request_job(endpoint, data)
        return job_id, work

    def send_job_results(self, job_id, job_result):
        endpoint = self.url + '/put_results'
        client_id = self.client_id
        data = {'results': {job_id: int(job_result)}, 'client_id': client_id}
        assure_request(requests.put, endpoint, data=dumps(data))


def execute_client_task(_client):
    print('Requesting new job from server...')
    job_id, work = _client.get_job()
    print('Received job!')
    perform_job(work)
    print('Sending result back to server...')
    _client.send_job_results(job_id, work)
    print('Job complete!\n')


def perform_job(work):
    print(f'I am working for {work} seconds...')
    time.sleep(int(work))
    print('Done with current job!')


def request_job(endpoint, data):
    response = assure_request(requests.get, endpoint,
                              data=dumps(data))
    job = loads(response.text)['job']
    work_id = list(job.keys())[0]
    work = job[work_id]
    return work_id, work


def assure_request(request, url, **kwargs):
    while True:
        try:
            response = request(url, **kwargs)
            check_status_code(response.status_code)
            response.raise_for_status()
        except requests.exceptions.ConnectionError:
            print('Unable to connect to the server. '
                  'Trying again in a minute...')
            time.sleep(60)
        except requests.exceptions.RequestException:
            time.sleep(60)
        else:
            return response


def check_status_code(status_code):
    if status_code == 400:
        print('No jobs available. Trying again in a minute...')
    elif status_code > 400:
        print('Server error. Trying again in a minute...')


def read_client_id():
    try:
        with open('client.cfg', 'r') as file:
            return int(file.readline())
    except FileNotFoundError:
        return -1


def write_client_id(client_id):
    with open('client.cfg', 'w') as file:
        file.write(str(client_id))


if __name__ == '__main__':
    client = Client()
    client.get_client_id()
    print('Your ID is: ', client.client_id)
    while True:
        execute_client_task(client)
