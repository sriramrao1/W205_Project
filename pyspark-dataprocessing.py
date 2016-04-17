sqlc = pyspark.sql.SQLContext(sc)
bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata")
bdf.count() -- 77445
#bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'stars', 'state', 'type')
bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
bscdf.count()
restrdd = bscdf.rdd.filter(lambda x: "Restaurants" in x[2])
restrdd.count() -- 25071
restdf = sqlc.createDataFrame(restrdd)
restdf.count()
restdf.registerTempTable("business")

#selectcolb = b.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'stars', 'state', 'type')
#selectcolb.count() 
#businessRDD = selectcolb.rdd
#businessRDD.count() 
#restrdd = businessRDD.filter(lambda x: "Restaurants" in x[2])
#restrdd.count() 
#restdf = sqlc.createDataFrame(restrdd)
#restdf.count()
#restdf.registerTempTable("business")

r = sqlc.read.json("/user/w205/project1/yelpreviewdata")
r.count()
r.printSchema()
reviewrdd = r.rdd.filter(lambda x: x[3] >2)
rf = sqlc.createDataFrame(reviewrdd)
rf.count() -- 1774673
rf.registerTempTable("review")
restdf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
rf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

citylistdf = sqlc.sql("select distinct city from business")
citylistdf.registerTempTable("city")

rbf = sqlc.sql("select * from review r, business b where r.business_id=b.business_id")
rbf.cache()
rbf.count() -- 1100181
#rbf1 = rf.join(restdf, "business_id")
#rbf1.count() -- 1100181

rbf1.filter(

rbf.rdd.filter(lambda x: " pizza " in x[4])
