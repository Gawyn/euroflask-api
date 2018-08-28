import eurostat_data
from rq import Queue
from worker import conn

q = Queue(connection=conn)

q.enqueue(eurostat_data.fetchUnemployment, False)
