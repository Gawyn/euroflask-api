import os

import redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

redisConn = redis.from_url(os.environ['REDISCLOUD_URL'])

if __name__ == '__main__':
    with Connection(redisConn):
        worker = Worker(map(Queue, listen))
        worker.work()
