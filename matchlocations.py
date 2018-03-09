from cassandra.cluster import Cluster

cluster = Cluster(port=9043)
session = cluster.connect()
session.set_keyspace('multi')
statement = 'SELECT * FROM mean5'
rows = session.execute(statement)
locations = dict()
states = []
means = []

# dictionary locations
# key: region name
# value : tuple(states, means)
for i in rows:
    locations[i[3]] = (i[2], i[5])

# dictionary areas
# key: name of area
# value: areacocde
# stops at first match to prevent duplicate area to areacode mappings
session.set_keyspace('occuwage')
statement = 'SELECT * FROM area'
rows = session.execute(statement)
areacodes = []
areas = dict()
for i in rows:
    for j in locations:
        if j in i[1]:
            if i[0] not in areacodes:
                areacodes.append(i[0])
                areas[i[1]] = i[0]
                continue

# Passes in area_code from the areas dictionary
# For every area, assigns list of tuples with series_id and occupation_code
# Deletes areas where there are no values

values = dict()
toDelete = []

for key in areas:
    statement = ('SELECT series_id, occupation_code FROM filteredseries WHERE area_code = \'' + areas[key] + '\' limit 1000000')
    rows = session.execute(statement)
    list1 = []
    for i in rows:
        list1.append((i[0], i[1]))
    a = len(list1)
    if a == 0:
        toDelete.append(key)
    else:
        areas[key] = list1

for x in toDelete:
    del areas[x]

sal = dict()
#Given the current series, get the corresponding numerical salary and store it into
for key in areas:
    for i in areas[key]:
        statement = ('SELECT value FROM current WHERE series_id = \'' + i[0] + '\'')
        rows = session.execute(statement)
        for j in rows:
            sal[(key, i[1])] = j[0].strip()

for x in sal:
    for lockey in locations:
        if lockey in x[0]:
            try:
                value = float(sal[x]) / float(locations[lockey][0])
                sal[x] = value
            except:
                print 'error here: ' + sal[x]


with open('110000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '110000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('130000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '130000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('150000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '150000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('170000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '170000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('210000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '210000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('230000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '230000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('250000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '250000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('270000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '270000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('290000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '290000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('310000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '310000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('330000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '330000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('350000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '350000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('370000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '370000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('390000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '390000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('410000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '410000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('430000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '430000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('470000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '470000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('490000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '490000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('510000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '510000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')

with open('530000.csv', 'w') as f:
    f.write('mean,regionname\n')
    for x in sal:
        print x[0]
        if x[1] == '530000':
            f.write(str(sal[x]) + ',' + str(x[0]) + '\n')
