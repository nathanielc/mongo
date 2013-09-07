
db = db.getSiblingDB('benchmarks')

var clean = db.sin_clean.find()
var noisy = db.sin_noisy.find()

var sum_x = 0
var sum_y = 0
var sum_xx = 0
var sum_yy = 0
var sum_xy = 0
var count = 0

while (clean.hasNext()) {
    x = clean.next().value
    y = noisy.next().value

    sum_x += x
    sum_y += y

    sum_xx += x * x
    sum_yy += y * y

    sum_xy += x * y

    count += 1
}


r = (count * sum_xy - (sum_x * sum_y) )
    / (
        Math.sqrt(count * sum_xx - (sum_x * sum_x)) 
        * Math.sqrt(count * sum_yy - (sum_y * sum_y))
    )

print("Pearson R: " + r)

