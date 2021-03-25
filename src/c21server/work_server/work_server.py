from flask import Flask, json, jsonify, request
import redis

app = Flask(__name__)
# class WorkServer:
#     def __init__(self, redis_server):
#         self.redis = redis_server


def get_first_key(data):
    '''
    Checks to make sure JSON has at least one entry and that its
        key-value pair are both integers. Returns the first key value
        if the data is valid, otherwise returns -1.
    '''
    if data is not None:
        keys = list(data.keys())
        is_key_digit = len(keys) > 0 and keys[0].isdigit()
        if is_key_digit and isinstance(data[keys[0]], int):
            return keys[0]
    return -1


def check_for_database(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.exceptions.ConnectionError:
            body = {'error': 'Cannot connect to the database'}
            return False, jsonify(body), 500
    return wrapper


@check_for_database
def get_job(workserver):
    keys = workserver.hkeys('jobs_waiting')
    if len(keys) == 0:
        return False, jsonify({'error': 'There are no jobs available'}), 400
    value = workserver.hget('jobs_waiting', keys[0])
    workserver.hset('jobs_in_progress', keys[0], value)
    workserver.hdel('jobs_waiting', keys[0])
    return True, keys[0].decode(), value.decode()


@check_for_database
def put_results(workserver, data):
    key = get_first_key(data)
    value = workserver.hget('jobs_in_progress', key)
    if value is None:
        body = {'error': 'The job being completed was not in progress'}
        return False, jsonify(body), 400
    workserver.hdel('jobs_in_progress', key)
    workserver.hset('jobs_done', key, value)
    return (True,)


@check_for_database
def get_client_id(workserver):
    # workserver.incr('total_num_client_ids')
    return True, int(workserver.redis.get('total_num_client_ids'))


def create_server():
    '''Create server, add endpoints, and return the server'''
    workserver = redis.Redis()
    try:
        workserver.keys('*')
    except redis.exceptions.ConnectionError:
        return None
    return workserver

workserver = create_server()

@app.route('/get_job', methods=['GET'])
def _get_job():
    success, *values = get_job(workserver)
    if not success:
        return tuple(values)
    return jsonify({'job': {values[0]: values[1]}}), 200

@app.route('/put_results', methods=['PUT'])
def _put_results():
    data = json.loads(request.data).get('results', None)
    if data is None:
        body = {'error': 'The body does not contain the results'}
        return jsonify(body), 400
    error = put_results(workserver, data)
    if not error[0]:
        return tuple(error[1:])
    return jsonify({'success': 'The job was successfully completed'}), 200

@app.route('/get_client_id', methods=['GET'])
def _get_client_id():
    success, *values = get_client_id(workserver)
    if not success:
        return tuple(values)
    return jsonify({'client_id': values[0]}), 200

    return workserver
