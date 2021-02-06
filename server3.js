const cluster = require('cluster');
const http = require('http');
const numCPUs = require('os').cpus().length;
var griddb = require('griddb_node');
var fs = require('fs');

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

// Define schema. This is the COLLECTION Container
/*var colConInfo = new griddb.ContainerInfo({
    'name': "Person",
    'columnInfoList': [
        ["name", griddb.Type.STRING],
        ["age", griddb.Type.INTEGER],
    ],
    'type': griddb.ContainerType.COLLECTION, 'rowKey': true
});*/

/*
let colContainer;
store.putContainer(colConInfo, false)
    .then(cont => {
        colContainer = cont;
        return colContainer.createIndex({ 'columnName': 'age', 'indexType': griddb.IndexType.DEFAULT });
    })
    .then(() => {
        colContainer.setAutoCommit(false);
	return colContainer.put(["John", 30]);
    })
    .then(() => {
        return colContainer.commit();
    })
    .then(() => {
        query = colContainer.query("SELECT * WHERE name = 'John'")
        return query.fetch();
    })
    .then(rs => {
        while (rs.hasNext()) {
            console.log(rs.next());
        }
        return colContainer.commit();
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
    });*/

/*let time_series;
store.putContainer(timeConInfo, false)
    .then(ts => {
        time_series = ts;
        return ts.put([new Date(), 60, 'resting']);
    })
    .then(() => {
        query = time_series.query("select * where timestamp > TIMESTAMPADD(HOUR, NOW(), -6)");
        return query.fetch();
    })
    .then(rowset => {
        while (rowset.hasNext()) {
            var row = rowset.next();
            console.log("Time =", row[0], "Heart Rate =", row[1].toString(), "Activity =", row[2]);
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
    });*/

var timeseries;
store.getContainer("SensorRateLast")
    .then(ts => {
        timeseries = ts;
        query = ts.query("select * from point01 where not active and voltage > 50");
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
    