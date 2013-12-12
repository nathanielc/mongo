
sh.addShard('localhost:27001');
sh.addShard('localhost:27002');


function test_linked_large() {
    print("test_linked_large");
    var dbName = 'test_linked_large';
    db = db.getSiblingDB(dbName);
    db.dropDatabase();

    sh.enableSharding(dbName);

    cols = [];

    for ( var i = 0; i < 4; i++) {
        var colName = 'col' + i;
        var col = db.getCollection(colName);
        col.ensureIndex({time:1});
        var ns = dbName + '.' + colName;
        sh.shardCollection(ns, {time:1});
        cols.push(ns);
        if ( i != 0 ) {
            assert(sh.linkCollections(cols[0], cols[i]).ok);
        }
    }

    var d = new Date(2013, 11, 11);
    for ( var j = 0; j < 100000; j++) {

        for ( i = 0; i < 4; i++) {
            var colName = 'col' + i;
            var col = db.getCollection(colName);
            col.insert({time: d, value: Math.sin(j*0.001)});
        }
        if (j % 1000 == 0) {
            print(d);
        }

        //Go back 10s
        d.setSeconds(d.getSeconds() - 10);
    }
}


test_linked_large();
