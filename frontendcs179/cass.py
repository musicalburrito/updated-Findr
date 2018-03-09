from cassandra.cluster import Cluster
from flask import Flask
from flask import render_template
from flask import request


app = Flask(__name__)

@app.route("/sd")
def getLocations():
    cluster = Cluster(port=9043)
    session = cluster.connect()
    session.set_keyspace('occuwage')
    statement = 'SELECT * FROM industry'
    rows = session.execute(statement)
    listofstuff = []
    for i in rows:
        print i[2]
        listofstuff.append(i[2])
    return "hello"

@app.route("/")
def asdf():
    cluster = Cluster(port=9043)
    session = cluster.connect()
    session.set_keyspace('occuwage')
    statement = 'SELECT * FROM industry'
    rows = session.execute(statement)
    listofstuff = []
    for i in rows:
        print i[2]
        listofstuff.append(i[2])
    return render_template('asdf.html', listofstuff=listofstuff)

@app.route("/retID", methods=['POST'])
def retID():
    projectpath = request.form.projectFilePath
    print projectpath

if __name__ == "__main__":
    app.run()
