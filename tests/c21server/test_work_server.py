from json import dumps
from fakeredis import FakeRedis
from pytest import fixture
from c21server.work_server.work_server import create_server


@fixture(name='mock_server')
def fixture_mock_server():
    mock_db = FakeRedis()
    server = create_server(mock_db)
    server.app.config['TESTING'] = True
    server.client = server.app.test_client()
    return server


def test_create_mock_database(mock_server):
    assert mock_server.redis is not None


def test_create_mock_server(mock_server):
    assert mock_server.app is not None


def test_new_mock_database_is_empty(mock_server):
    assert mock_server.redis.keys() == []


def test_data_entry_in_mock_database_is_correct(mock_server):
    mock_server.redis.set('val1', 1)
    assert mock_server.redis.get('val1').decode() == '1'


def test_data_entry_changes_in_mock_database(mock_server):
    mock_server.redis.set('val1', 1)
    mock_server.redis.set('val1', 2)
    assert mock_server.redis.get('val1').decode() != '1'
    assert mock_server.redis.get('val1').decode() == '2'


def test_data_deleted_from_mock_database_is_gone(mock_server):
    mock_server.redis.set('val1', 1)
    assert mock_server.redis.get('val1').decode() == '1'
    mock_server.redis.delete('val1')
    assert mock_server.redis.get('val1') is None


def test_get_job_has_no_available_job(mock_server):
    response = mock_server.client.get('/get_job')
    assert response.status_code == 400
    expected = {'error': 'There are no jobs available'}
    assert response.get_json() == expected


def test_get_job_returns_single_job(mock_server):
    mock_server.redis.hset('jobs_waiting', 1, 2)
    response = mock_server.client.get('/get_job')
    assert response.status_code == 200
    expected = {'1': '2'}
    assert response.get_json() == expected


def test_get_waiting_job_is_now_in_progress_and_not_waiting(mock_server):
    mock_server.redis.hset('jobs_waiting', 2, 3)
    mock_server.client.get('/get_job')
    keys = mock_server.redis.hkeys('jobs_waiting')
    assert keys == []
    keys = mock_server.redis.hkeys('jobs_in_progress')
    assert mock_server.redis.hget('jobs_in_progress', keys[0]).decode() == '3'


def test_put_results_with_zero_jobs_in_progress(mock_server):
    mock_server.redis.hset('jobs_in_progress', 2, '')
    response = mock_server.client.put("/put_results", data=dumps({'': ''}))
    assert mock_server.redis.hget('jobs_in_progress', 2).decode() == ''
    assert response.status_code == 400


def test_put_results_returns_correct_job(mock_server):
    mock_server.redis.hset('jobs_in_progress', 2, 3)
    response = mock_server.client.put("/put_results", data=dumps({2: 3}))
    assert mock_server.redis.hget('jobs_done', 2).decode() == '3'
    assert response.status_code == 200
    assert response.get_json() is None
