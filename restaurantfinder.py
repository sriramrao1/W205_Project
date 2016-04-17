#!/usr/bin/env python
from __future__ import absolute_import, print_function, unicode_literals
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import psycopg2

def main():

    try:
        total = len(sys.argv)
        print ("The total numbers of args passed to the script: %d " % total)
        sc = SparkContext("local", "weblog app")
        sqlc = SQLContext(sc)
        bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata")
        #bdf.count() -- 77445
        #bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'stars', 'state', 'type')
        bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
        #bscdf.count()
        restrdd = bscdf.rdd.filter(lambda x: "Restaurants" in x[2])
        #restrdd.count() -- 25071
        restdf = sqlc.createDataFrame(restrdd)
        #restdf.count()
        restdf.registerTempTable("business")

        r = sqlc.read.json("/user/w205/project1/yelpreviewdata")
        r.count()
        r.printSchema()
        reviewrdd = r.rdd.filter(lambda x: x[3] >2)
        rf = sqlc.createDataFrame(reviewrdd)
        #rf.count() -- 1774673
        rf.registerTempTable("review")

        # Get the arguments list 
        #cmdargs = str(sys.argv)
        #print ("Args list: %s " % cmdargs)

        #Running finalresults.py without an argument returns all the words in the stream and their total count of occurrences,
        #sorted alphabetically in an ascending order, one word per line.
        if int(total) == 3:
            food = str(sys.argv[1])
            city = str(sys.argv[2])
            query = "select b.business_id, b.name, count(*) as C, avg(r.stars) as average, b.city from review r, business b where r.text like '%"+food+"%' AND r.stars>2 and r.business_id=b.business_id AND b.city='"+city+"' group by b.business_id, b.name, b.city order by C desc limit 3"
            toppizzajoints = sqlc.sql(query)
            toppizzajoints.show()
            print("The query was "+query)

        #This section gets a word as an argument and returns the total number of word occurrences in the stream.
        else:
            query = "select b.business_id, b.name, count(*) as C, avg(r.stars) as average, b.city from review r, business b where r.text like '%pizza%' AND r.stars>2 and r.business_id=b.business_id AND b.city='Las Vegas' group by b.business_id, b.name, b.city order by C desc limit 3"
            toppizzajoints = sqlc.sql(query)
            toppizzajoints.show()
         
    except Exception as inst:
        print(inst.args)
        print(inst)
   

if __name__ == '__main__':
  main() 
