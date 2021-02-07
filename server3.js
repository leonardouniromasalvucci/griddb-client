const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;
var griddb = require('griddb_node');
var fs = require('fs');
const express = require('express')
const app = express()

// our Cluster's credentials
var factory = griddb.StoreFactory.getInstance();
var store = factory.getStore({
    "notificationMember":"10.0.0.17:10001,10.0.0.53:10001",
    "clusterName": "defaultCluster",
    "username": "admin",
    "password": "admin"
});

// Timeseries container. TIMESTAMP is the rowKey (JavaScript OBJ)
var timeConInfo = new griddb.ContainerInfo({
    'name': "SensorRateLast",
    'columnInfoList': [
        ["timestamp", griddb.Type.TIMESTAMP],
        ["sensorValue", griddb.Type.STRING],
        ["topic", griddb.Type.STRING]
    ],
    'type': griddb.ContainerType.TIME_SERIES, 'rowKey': true
});

module.exports = app.get('/myEndpoint', async (req, res) => {
    res.status(200).send('successfully tested');
});
  
module.exports = app.post('/myEndpoint/search', async (req, res) => {      
    let data = [ { "text": "upper_25", "value": 1}, { "text": "upper_75", "value": 2} ];
    res.status(200).send(data);
});

var time_series;
store.getContainer("SensorRateLast")
    .then(ts => {
        time_series = ts;
        query = time_series.query("select * where timestamp > TIMESTAMPADD(HOUR, NOW(), -6)");
        return query.fetch();
    })
    .then(rowset => {
        var row;
        while (rowset.hasNext()) {
            var row = rowset.next();
            console.log("Time =", row[0], "Sensor Value =", row[1].toString(), "Topic =", row[2]);
        }
    })
    .catch(err => {
        if (err.constructor.name == "GSException") {
            for (var i = 0; i < err.getErrorStackSize(); i++) {
                console.log("[", i, "]");
                console.log(err.getErrorCode(i));
                console.log(err.getMessage(i));
            }
        } else {
            console.log(err);
        }
    });