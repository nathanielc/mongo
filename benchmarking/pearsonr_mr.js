
db = db.getSiblingDB('benchmarks')

db.red_cross.drop()
db.red_self_clean.drop()
db.red_self_noisy.drop()
db.red_sum_cross.drop()


function cross_map() {
    emit(this.time,
         {product: this.value,
          count:1,
    });
}

function cross_reduce(key, values) {
    var a = values[0]
    for (var i = 1; i < values.length; i++){
        var b = values[i];
        a.product *= b.product
        a.count += b.count
    }
    return a
}

function sum_cross_map() {
    emit(1,
        {
            sum: this.value.product,
            count: 1,
    })
}

function sum_cross_reduce(key, values) {
    var a = values[0]
    for (var i = 1; i < values.length; i++){
        var b = values[i];
        a.sum += b.sum
        a.count += b.count
    }

    return a
}

function self_map() {
    emit(1,
         {
            sum_square: this.value*this.value,
            sum_value: this.value,
            count:1,
    });
}

function self_reduce(key, values) {
    var a = values[0]
    for (var i = 1; i < values.length; i++){
        var b = values[i];
        a.sum_square += b.sum_square
        a.sum_value += b.sum_value
        a.count += b.count
    }

    return a;
}


//Mapreduce the two data sets into the same collection
db.sin_clean.mapReduce(cross_map, cross_reduce, {out: { reduce : 'red_cross'}})
db.sin_noisy.mapReduce(cross_map, cross_reduce, {out: { reduce : 'red_cross'}})

//Mapreduce red_cross into another document to get sums
db.red_cross.mapReduce(sum_cross_map, sum_cross_reduce, {
        out: { reduce : 'red_sum_cross'}
    })

//Mapreduce clean into a document to get sums of just the clean data set
db.sin_clean.mapReduce(self_map, self_reduce, {
    out: { reduce : 'red_self_clean'}
    })

//Mapreduce noisy into a document to get sums of just the noisy data set
db.sin_noisy.mapReduce(self_map, self_reduce, {
    out: { reduce : 'red_self_noisy'}
    })

//Get the results of each action
red_self_clean = db.red_self_clean.findOne().value
red_self_noisy = db.red_self_noisy.findOne().value
red_sum_cross = db.red_sum_cross.findOne().value


//Cacluate the correlation coefficient
sum_x = red_self_clean.sum_value
sum_y = red_self_noisy.sum_value
sum_xx = red_self_clean.sum_square
sum_yy = red_self_noisy.sum_square
sum_xy = red_sum_cross.sum
count = red_sum_cross.count


r = (count * sum_xy - (sum_x * sum_y) )
    / (
        Math.sqrt(count * sum_xx - (sum_x * sum_x))
        * Math.sqrt(count * sum_yy - (sum_y * sum_y))
    )

print("Pearson R: " + r)

