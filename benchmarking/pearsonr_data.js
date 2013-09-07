
db = db.getSiblingDB('benchmarks')

count = 100
db.sin_clean.drop()
db.sin_noisy.drop()
for (var i = 1; i <= count; i++) db.sin_clean.insert( { time : i, value : Math.sin(i) } )
for (var i = 1; i <= count; i++) db.sin_noisy.insert( { time : i, value : Math.sin(i) + (Math.random() * 0.5) } )
//for (var i = 1; i <= 100; i++) db.sin_noisy.insert( { time : i, value : i} )

