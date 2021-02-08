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


/*

var data = []
data.push({"target":"1", "datapoints":[[[12,132566]]]});
//console.log(data)

function append_value(x, id, value){
  if(x[id] == undefined){
    data.push({"target":id, "datapoints":[value]});
  }else{
    for (var i in x) {
      if (x[i].target == id) {
        x[i].datapoints.push(value);
        break;
      }
    }
  }
}

r = append_value(data, "2", [16, 3466])
r = append_value(data, "1", [78, 3546])
r = append_value(data, "1", [56, 1234])
console.log(data)

*/

function append_value(x, id, value){
  if(x[id] == undefined){
    data.push({"target":id, "datapoints":[value]});
  }else{
    for (var i in x) {
      if (x[i].target == id) {
        x[i].datapoints.push(value);
        break;
      }
    }
  }
}

function toTimestamp(strDate){
  var datum = Date.parse(strDate);
  return datum/1000;
}

module.exports = app.post('/myEndpoint/query', async (req, res) => {      
    var time_series;
    var data = []
    ///let dd = getQueryData()
    //console.log(dd)
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
                //append_value(data, "2", [16, 3466])
                var v = JSON.parse(row[1].toString())
                append_value(data, v.id, [v.value, toTimestamp(row[0])])
                console.log(data)
                //console.log("[{target:'"+vv.id+"', datapoints:[[" + vv.value + ", " + toTimestamp(row[0]) + "]]}]")
                //data.push("[{target:'"+vv.id+"', datapoints:[[" + vv.value + ", " + toTimestamp(row[0]) + "]]}]")
                //console.log("Time =", row[0], "Sensor Value =", row[1].toString(), "Topic =", row[2]);
                
            }
            //var data = "[{target:'1', datapoints:[[1.804, 1612726246]]}]"
            //console.log(data)
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