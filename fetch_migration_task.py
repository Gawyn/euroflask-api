import eurostat_data
from rq import Queue
from worker import conn

q = Queue(connection=conn)

for country in eurostat_data.europeanUnionCountries():
    q.enqueue(eurostat_data.getMigrationTo, country, False)
