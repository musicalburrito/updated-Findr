from cassandra.cluster import Cluster

cluster = Cluster(port=9043)
session = cluster.connect()
session.set_keyspace('multi')
statement = 'SELECT * FROM mean5'
rows = session.execute(statement)
locations = []
# with open('mean5.csv', 'w') as f:
#     f.write('mean,regionname,state\n')
for i in rows:
    print i[3]
    locations.append(i[3])
