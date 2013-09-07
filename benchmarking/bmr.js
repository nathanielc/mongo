

data = {}

function bemit(key, value) {
    if (!(key in data)) {
        data[key] = []
    }
    data[key].push(value)
}



/**
 * Oversimplified implementation of a benchmarking map reduce job.
 * The idea here is just to provide a working, although not practical,
 * implementation of bmr.
 */
DBCollection.prototype.bmr = function(other_coll, map, reduce, options) {
    data = {}
    reduced_data = {}
    var self = this.find()
    var other = other_coll.find()
    while(self.hasNext()) {
        map(self.next(), other.next())
    }

    for (key in data) {
        reduced_data[key] = reduce(key, data[key])
    }

    if ('finalize' in options) {
        finalize = options['finalize']
        for (key in reduced_data) {
            reduced_data[key] = finalize(key, reduced_data[key])
        }
    }
    return reduced_data
}



