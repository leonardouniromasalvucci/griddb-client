const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;
var griddb = require('griddb_node');
var fs = require('fs');
const express = require('express')
var app = require('express')()

var factory = griddb.StoreFactory.getInstance();
var store = factory.getStore({
    "notificationMember":"10.0.0.28:10001,10.0.0.37:10001,10.0.0.172:10001",
    "clusterName": "defaultCluster",
    "username": "admin",
    "password": "admin"
});

var timeConInfo = new griddb.ContainerInfo({
    'name': "SensorVlues",
    'columnInfoList': [
      ["timestamp", griddb.Type.TIMESTAMP],
      ["sensorId", griddb.Type.STRING],
      ["sensorValue", griddb.Type.STRING],
      ["topic", griddb.Type.STRING]
    ],
    'type': griddb.ContainerType.TIME_SERIES, 'rowKey': true
});


module.exports = app.get('/myEndpoint', async (req, res) => {
    res.status(200).send('successfully tested');
});

module.exports = app.get('/myEndpoint/get_values', async (req, res) => {
    var time_series;
    var all_data = [];
    store.getContainer("SensorValues")
        .then(ts => {
            time_series = ts;
            query = time_series.query("select * where timestamp > TIMESTAMPADD(HOUR, NOW(), -1)");
            return query.fetch();
        })
        .then(rowset => {
            var row;
            while (rowset.hasNext()) {
                var row = rowset.next();
                all_data.push(row)
                console.log("Time =", row[0], "Device ID = ", row[1].toString() , "Sensor Value =", row[2].toString(), "Topic =", row[3]);
            }
            res.status(200).send(all_data);
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
});
  
module.exports = app.post('/myEndpoint/search', async (req, res) => {      
        let data = [];
        res.status(200).send(data);
});

function append_value(x, id, value){
  if(x[id] == undefined){
    x.push({"target":id, "datapoints":[value]});
  }else{
    for (var i in x) {
      if (x[i].target == id) {
        x[i].datapoints.push(value);
        break;
      }
    }
  }
  return x
}

function toTimestamp(strDate){
  var datum = Date.parse(strDate);
  return datum/1000;
}

module.exports = app.post('/myEndpoint/query', async (req, res) => {      
    var time_series;
    var data = []
    store.getContainer("SensorValues")
        .then(ts => {
            time_series = ts;
            query = time_series.query("select * where timestamp > TIMESTAMPADD(MINUTE, NOW(), -5)"); //get last 5 minutes
            return query.fetch();
        })
        .then(rowset => {
            var row;
            while (rowset.hasNext()) {
                var row = rowset.next();
                data = append_value(data, row[1].toString(), [row[2], toTimestamp(row[0])])                
            }
            res.status(200).send(data);
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
});

app.listen(8080)