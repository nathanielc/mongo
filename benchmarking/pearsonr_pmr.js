load('pmr.js')

db = db.getSiblingDB('benchmarks')


function map(self, other) {
    pemit(1, {
        sum_self    : self.value,
        sum_other   : other.value,
        sum_self_2  : self.value * self.value,
        sum_other_2 : other.value * other.value,
        sum_product : self.value * other.value,
        count       : 1,
    })
}

function reduce(key, values) {
    var a = values[0];
    for ( var i = 1; i < values.length; i++) {
        var b = values[i]
        a.sum_self    += b.sum_self
        a.sum_other   += b.sum_other
        a.sum_self_2  += b.sum_self_2
        a.sum_other_2 += b.sum_other_2
        a.sum_product += b.sum_product
        a.count += b.count
    }
    return a
}

function finalize(key, value) {
    value.p = (value.count * value.sum_product - (value.sum_self * value.sum_other) )
     / (Math.sqrt(value.count * value.sum_self_2 - (value.sum_self * value.sum_self))
     * Math.sqrt(value.count * value.sum_other_2 - (value.sum_other * value.sum_other)))
    return value
}

var r = db.sin_clean.pmr(db.sin_noisy, map, reduce, {finalize : finalize})

printjson(r)

