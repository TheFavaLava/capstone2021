import time
from json import dumps, loads
import requests


class Client:
    def __init__(self):
        self.url = "http://capstone.cs.moravian.edu:8080"

    def handle_error(self, j_id, work, data):
        while j_id == "error":
            print(work)
            print("I'm sleeping a minute.")
            time.sleep(60)
            j_id, work = request_job(self.url, data)
        return j_id, work

    def get_job(self):
        full_url = f"{self.url}/get_job"
        c_id = self.get_client_id()
        data = {"client_id": c_id}
        j_id, work = request_job(full_url, data)
        if j_id == "error":
            j_id, work = self.handle_error(j_id, work, data)
        return j_id, work

    def send_job_results(self, j_id, job_result):
        end_point = "/put_results"
        c_id = self.get_client_id()
        data = {j_id: int(job_result), "client_id": c_id}
        request = requests.put(self.url + end_point, data=dumps(data))
        request.raise_for_status()

    def request_client_id(self):
        end_point = "/get_client_id"
        request = requests.get(self.url + end_point)

        # Check if status code is 200 type code: successful GET
        if request.status_code // 100 == 2:
            c_id = int(request.json()['client_id'])
            write_client_id(c_id)
            return c_id
        return -1

    # Reads from file, or requests if nothing there
    def get_client_id(self):
        c_id = read_client_id()
        if c_id == -1:
            c_id = self.request_client_id()
        if c_id == -1:
            print("Could not get client ID!")
        return c_id

    def complete_client_request(self):
        job_id, job = self.get_job()
        perform_job(job)
        self.send_job_results(job_id, job)


# Helper functions (don't need to be a part of the class)
def request_job(full_url, data):
    request = requests.get(full_url, data=dumps(data))
    if request.status_code // 100 == 4:
        request.raise_for_status()
    work_id, work = list(loads(request.text).items())[0]
    return work_id, work


def perform_job(j):
    print(f"I am working for {j} seconds.")
    time.sleep(int(j))


def read_client_id():
    try:
        with open("client.cfg", "r") as file:
            return int(file.readline())
    except FileNotFoundError:
        return -1


def write_client_id(c_id):
    with open("client.cfg", "w") as file:
        file.write(str(c_id))


if __name__ == "__main__":
    client = Client()
    client_id = client.get_client_id()
    print("ID of this client: ", client_id)

    while True:
        client.complete_client_request()
