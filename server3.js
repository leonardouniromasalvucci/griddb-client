const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;
var griddb = require('griddb_node');
var fs = require('fs');
const express = require('express')
var app = require('express')()

var factory = griddb.StoreFactory.getInstance();
var store = factory.getStore({
    "notificationMember":"10.0.0.17:10001,10.0.0.53:10001",
    "clusterName": "defaultCluster",
    "username": "admin",
    "password": "admin"
});

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

module.exports = app.get('/myEndpoint/get_values', async (req, res) => {
    var time_series;
    var all_data = [];
    store.getContainer("SensorRateLast")
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
                console.log("Time =", row[0], "Sensor Value =", row[1].toString(), "Topic =", row[2]);
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

/*const getQueryData = () => {
    return [
      {
        "target":"pps in",
        "datapoints":[
          [622,1450754160000],
          [365,1450754220000]
        ]
      },
      {
        "target":"pps out",
        "datapoints":[
          [861,1450754160000],
          [767,1450754220000]
        ]
      },
      {
        "target":"errors out",
        "datapoints":[
          [861,1450754160000],
          [767,1450754220000]
        ]
      },
      {
        "target":"errors in",
        "datapoints":[
          [861,1450754160000],
          [767,1450754220000]
        ]
      }
    ]
  };*/
  

/*const getQueryData = () => {
    return [
      {
        "target":"value",
        "datapoints":[
          [622,1450754160000],
          [365,1450754220000]
        ]
      }
    ]
  };*/

module.exports = app.post('/myEndpoint/query', async (req, res) => {      
    var time_series;
    let data = [];
    store.getContainer("SensorRateLast")
        .then(ts => {
            time_series = ts;
            query = time_series.query("select * where timestamp > TIMESTAMPADD(MINUTE, NOW(), -20)"); //get last 10 minutes
            return query.fetch();
        })
        .then(rowset => {
            var row;
            while (rowset.hasNext()) {
                var row = rowset.next();
                var vv = JSON.parse(row[1].toString())
                data.push('{"target":"'+vv.id+'","datapoints":['+vv.value+','+row[0]+']]}')
                //var v = row[1].toString();
                console.log("Time =", row[0], "Sensor Value =", row[1].toString(), "Topic =", row[2]);
                
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