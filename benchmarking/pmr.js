

data = {}

function pemit(key, value) {
    if (!(key in data)) {
        data[key] = []
    }
    data[key].push(value)
}

DBCollection.prototype.pmr = function(other_coll, map, reduce, options) {
    data = {}
    reduced_data = {}
    var self = this.find()
    var other = other_coll.find()
    while(self.hasNext()) {
        map(self.next(), other.next())
    }
    printjson(data)
    for (key in data) {
        reduced_data[key] = reduce(key, data[key])
    }

    printjson(reduced_data)

    if ('finalize' in options) {
        finalize = options['finalize']
        for (key in reduced_data) {
            reduced_data[key] = finalize(key, reduced_data[key])
        }
    }
    return reduced_data
}



