#!/usr/bin/python3.6 -u 
from flask import Flask, request, jsonify, json, abort
from flask_cors import CORS, cross_origin
from datetime import datetime
import griddb_python
import logging

griddb = griddb_python
factory = griddb.StoreFactory.get_instance()
app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

methods = ('GET', 'POST')

@app.route('/', methods=methods)
@cross_origin()
def hello_world():
    return("GridDB Data source")

@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    # return a list of containers that can be queried via /query
    return jsonify([])

def query_column(container, column, freq, start, end):
    results = []
    ts = gridstore.get_container(container)
    tql = "select "+ freq 
    if freq == "*":
        tql = tql + " where timestamp > TO_TIMESTAMP_MS("+str(start)+") and timestamp < TO_TIMESTAMP_MS("+str(end)+")"

    query = ts.query(tql) 
    rs = query.fetch(False) 
    columns = rs.get_column_names()

    a=0
    datapoints=[] 
    while rs.has_next():
        data = rs.next()
        datapoints.append((data[int(columns.index(column))], int(data[0].timestamp()*1000)))
        a=a+1
    return {'target': '%s' % (container+" "+column), 'datapoints': datapoints}

@app.route('/query', methods=methods)
@cross_origin(max_age=600)
def query_metrics():
    req = request.get_json()


    results = []

    start =  int(datetime.strptime(req['range']['from'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()*1000)
    end = int(datetime.strptime(req['range']['to'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()*1000)

    if 'intervalMs' in req:
        freq = "TIME_SAMPLING(*, TO_TIMESTAMP_MS("+ str(start) +"), TO_TIMESTAMP_MS("+ str(end) +"), "+ str(req.get('intervalMs'))+", MILLISECOND)"
    else:
        freq = "*"

    for target in req['targets']:
        if ':' not in target.get('target', ''):
            abort(404, Exception('Target must be of type: <container>:<column no> got instead: ' + target['target']))

        req_type = target.get('type', 'timeserie')
        container, column = target['target'].split(':', 1)

        results.append(query_column(container, column, freq, start, end))
    return jsonify(results)

@app.route('/annotations', methods=methods)
@cross_origin(max_age=600)
def query_annotations():
    req = request.get_json()

    response = []
    start =  int(datetime.strptime(req['range']['from'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()*1000)
    end = int(datetime.strptime(req['range']['to'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()*1000)


    container, reqquery = req['annotation']['query'].split(':', 1)

    ts = gridstore.get_container(container)

    tql = "select * where timestamp > TO_TIMESTAMP_MS("+str(start)+") and timestamp < TO_TIMESTAMP_MS("+str(end)+") and ( "+reqquery+" )"
    query = ts.query(tql) 
    rs = query.fetch(False) 
    last = None

    while rs.has_next():
        data = rs.next()
        if not last or data[0].timestamp() - last[0].timestamp() > 60:
            response.append({
                "annotation": reqquery, # The original annotation sent from Grafana.
                "time": int(data[0].timestamp()*1000), # Time since UNIX Epoch in milliseconds. (required)
                "title": reqquery, # The title for the annotation tooltip. (required)
            })
        last = data

    return jsonify(response)


@app.route('/panels', methods=methods)
@cross_origin()
def get_panel():
    req = request.args 
    return

if __name__ == "__main__":
    gridstore = factory.get_store(
        host="239.0.0.1",
        port=31999,
        cluster_name="defaultCluster",
        username="admin",
        password="admin"
    )

    logging.basicConfig(filename='/tmp/griddb_grafanajson_connector.log', level=logging.DEBUG)
    app.run(host='0.0.0.0', port=3003 )

