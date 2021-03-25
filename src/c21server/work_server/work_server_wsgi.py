from work_server import WorkServer
import redis

if __name__ == "__main__":
    server = create_server(redis.Redis())
    if server is None:
        print('There is no Redis database to connect to.')
    else:
        server.app.run(host='0.0.0.0', port=8080, debug=False)
