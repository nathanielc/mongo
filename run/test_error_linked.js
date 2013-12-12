
sh.addShard('localhost:27001');
sh.addShard('localhost:27002');


function test_linked_01() {
    print("test_linked_01");
    var dbName = 'test_linked_01';
    db = db.getSiblingDB(dbName);
    db.dropDatabase();

    sh.enableSharding(dbName);

    cols = [];

    for ( var i = 0; i < 4; i++) {
        var colName = 'col' + i;
        var col = db.getCollection(colName);
        col.insert({a:i});
        col.ensureIndex({a:1});
        var ns = dbName + '.' + colName;
        sh.shardCollection(ns, {a:1});
        cols.push(ns);
    }

    assert(sh.linkCollections(cols[0], cols[1]).ok);
    assert(sh.linkCollections(cols[2], cols[3]).ok);
    //Should fail to link
    assert(!sh.linkCollections(cols[0], cols[3]).ok);
    assert(!sh.linkCollections(cols[1], cols[3]).ok);
    assert(!sh.linkCollections(cols[2], cols[0]).ok);
    assert(!sh.linkCollections(cols[2], cols[1]).ok);


}

function test_linked_02() {
    print("test_linked_02");
    var dbName = 'test_linked_02';
    db = db.getSiblingDB(dbName);
    db.dropDatabase();

    sh.enableSharding(dbName);

    cols = [];

    for ( var i = 0; i < 4; i++) {
        var colName = 'col' + i;
        var col = db.getCollection(colName);
        col.insert({a:i});
        col.ensureIndex({a:1});
        var ns = dbName + '.' + colName;
        sh.shardCollection(ns, {a:1});
        cols.push(ns);
        if ( i != 0 ) {
            assert(sh.linkCollections(cols[0], cols[i]).ok);
        }
    }
}

function test_linked_03() {
    print("test_linked_03");
    var dbName = 'test_linked_03';
    db = db.getSiblingDB(dbName);
    db.dropDatabase();

    sh.enableSharding(dbName);

    var colName1 = 'col1';
    var colName2 = 'col2';

    var ns1 = dbName + '.' + colName1;
    var ns2 = dbName + '.' + colName2;


    var col1 = db.getCollection(colName1);
    var col2 = db.getCollection(colName2);

    col1.insert({a:1});
    col2.insert({a:2});

    col1.insert({a:6});
    col2.insert({a:7});

    col1.ensureIndex({a:1});
    col2.ensureIndex({a:1});



    sh.shardCollection(ns1, {a:1});
    sh.shardCollection(ns2, {a:1});

    assert.eq(1, col1.findOne().a);
    assert.eq(2, col2.findOne().a);
    assert.eq(6, col1.findOne({a : { $gt : 4}}).a);
    assert.eq(7, col2.findOne({a : { $gt : 4}}).a);


    print("\tlinking col1 and col2");

    sh.linkCollections(ns1, ns2);


    db = db.getSiblingDB('config')

    var chunk = db.chunks.findOne({ns : ns1});
    var toShard = 'shard0000';
    var fromShard = chunk.shard;
    if (toShard == chunk.shard) {
        toShard = 'shard0001';
    }

    var res = undefined;

    assert.eq(1, col1.findOne().a);
    assert.eq(2, col2.findOne().a);
    assert.eq(6, col1.findOne({a : { $gt : 4}}).a);
    assert.eq(7, col2.findOne({a : { $gt : 4}}).a);

    print("\tmoving chunk 1");
    res = sh.moveChunk(ns1, {a:1}, toShard);
    printjson(res);
    assert(res.ok);

    assert.eq(1, col1.findOne().a);
    assert.eq(2, col2.findOne().a);
    assert.eq(6, col1.findOne({a : { $gt : 4}}).a);
    assert.eq(7, col2.findOne({a : { $gt : 4}}).a);

    print("\tsplitting chunk");
    res = db.adminCommand( { split : ns1, middle : {a:4}});
    printjson(res);
    assert(res.ok);

    assert.eq(1, col1.findOne().a);
    assert.eq(2, col2.findOne().a);
    assert.eq(6, col1.findOne({a : { $gt : 4}}).a);
    assert.eq(7, col2.findOne({a : { $gt : 4}}).a);


    //  print("\tmoving chunk 2");
    //  res = sh.moveChunk(ns1, {a:6}, fromShard);
    //  printjson(res);
    //  assert(res.ok);

    //  assert.eq(1, col1.findOne().a);
    //  assert.eq(2, col2.findOne().a);
    //  assert.eq(6, col1.findOne({a : { $gt : 4}}).a);
    //  assert.eq(7, col2.findOne({a : { $gt : 4}}).a);

}

test_linked_01();
test_linked_02();
test_linked_03();

print("************Linked tests passed**************")
