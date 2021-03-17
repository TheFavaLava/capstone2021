import requests
from c21client.client import Client, execute_client_task

BASE_URL = "http://localhost:8080"


def test_client_url():
    client = Client()
    assert client.url == BASE_URL


def test_client_gets_existing_client_id(mocker):
    client = Client()
    mock_client_id = 99
    write_mock_client_id(mocker)
    read_mock_client_id(mocker, mock_client_id)
    assert mock_client_id == client.get_client_id()


def test_client_gets_client_id_from_server(requests_mock, mocker):
    client = Client()
    mock_client_id = 9
    read_mock_client_id(mocker, -1)
    write_mock_client_id(mocker)
    requests_mock.get(
        '{}/get_client_id'.format(BASE_URL),
        json={'client_id': mock_client_id}
    )
    assert mock_client_id == client.get_client_id()


def test_client_get_id_handles_error(requests_mock, mocker):
    client = Client()
    read_mock_client_id(mocker, -1)
    write_mock_client_id(mocker)
    requests_mock.get(
        '{}/get_client_id'.format(BASE_URL),
        json={'error': 'Some error'},
        status_code=400
    )
    assert -1 == client.get_client_id()


def test_client_gets_job(requests_mock, mocker):
    client = Client()
    mock_client_id = 9
    read_mock_client_id(mocker, mock_client_id)
    requests_mock.get(
        '{}/get_client_id'.format(BASE_URL),
        json={'client_id': mock_client_id}
    )
    requests_mock.get('{}/get_job'.format(BASE_URL), json={1: "1"})
    assert ("1", "1") == client.get_job()


def test_client_handles_error_getting_jobs(requests_mock, mocker):
    client = Client()
    mock_client_id = 99
    read_mock_client_id(mocker, mock_client_id)
    requests_mock.get(
        '{}/get_client_id'.format(BASE_URL),
        json={'client_id': mock_client_id}
    )
    requests_mock.get(
        '{}/get_job'.format(BASE_URL),
        json={"Error": "Error message"},
        status_code=400
    )
    try:
        client.get_job()
    except requests.exceptions.HTTPError as exception:
        assert True, 'raised an exception: {}'.format(exception)


def test_client_sleeps_when_no_jobs_available(requests_mock, mocker):
    client = Client()
    read_mock_client_id(mocker, 1)
    requests_mock.get(
        '{}/get_job'.format(BASE_URL),
        json={"error": "No jobs available"}
    )
    mocker.patch(
        'c21client.client.Client.handle_error',
        return_value=("2", "22")
    )
    assert ("2", "22") == client.get_job()


def test_client_sends_job_results(requests_mock, mocker):
    client = Client()
    mock_id = 1
    mock_job_result = "1"
    mock_client_id = 999
    read_mock_client_id(mocker, mock_client_id)

    requests_mock.get(
        '{}/get_client_id'.format(BASE_URL),
        json={'client_id': 999}
    )
    requests_mock.put('{}/put_results'.format(BASE_URL), text='')
    try:
        client.send_job_results(mock_id, mock_job_result)
    except requests.exceptions.HTTPError as exception:
        assert False, 'raised an exception: {}'.format(exception)


def test_client_completes_job_requested(requests_mock, mocker):
    client = Client()
    mock_client_id = 9
    read_mock_client_id(mocker, mock_client_id)
    requests_mock.get(
        '{}/get_client_id'.format(BASE_URL),
        json={'client_id': mock_client_id}
    )
    requests_mock.get('{}/get_job'.format(BASE_URL), json={1: "1"})
    requests_mock.put('{}/put_results'.format(BASE_URL), text='')

    try:
        execute_client_task(client)
    except requests.exceptions.HTTPError as exception:
        assert False, 'Raised an exception: {}'.format(exception)


def read_mock_client_id(mocker, value):
    mocker.patch(
        'c21client.client.read_client_id',
        return_value=value
    )


def write_mock_client_id(mocker):
    mocker.patch(
        'c21client.client.write_client_id',
        return_value=''
    )
