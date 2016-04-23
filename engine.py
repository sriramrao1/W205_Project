#!/usr/bin/env python
from __future__ import absolute_import, print_function, unicode_literals
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import re
import logging
import pprint
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

foodwords = [u'pizza',u'pancake',u'pancakes',u'taco',u'burger',u'pasta', u'BBQ', u'sausage', u'biscuit', u'lobster', u'chicken', u'steak', u'chili', u'salmon', u'salad', u'soup', u'dog', u'pork', u'fish']

class FoodSearchEngine:
    """ A Food Search Engine """
    def __init__(self, sc):
            """Init the food search engine given a Spark context"""

            logger.info("Starting up the food search engine: ")
            self.sc = sc

    def get_top_restaurants(self, c, food):
        toprest = ""
        try:
            sc = self.sc
            sqlc = SQLContext(sc)
            bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata")
            #bdf.count() -- 77445
            bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
            #bscdf.count()
            restrdd = bscdf.rdd.filter(lambda x: "Restaurants" in x[2])
            #restrdd.count() -- 25071
            restdf = sqlc.createDataFrame(restrdd)
            #restdf.count()
            restdf.registerTempTable("business")

            r = sqlc.read.json("/user/w205/project1/yelpreviewdata")
            #r.count()
            #r.printSchema()
            reviewrdd = r.rdd.filter(lambda x: x[3] >2)
            rf = sqlc.createDataFrame(reviewrdd)
            #rf.count() -- 1774673
            rf.registerTempTable("review")

            #Running finalresults.py without an argument returns all the words in the stream and their total count of occurrences,
            food = food
            city = c
            query = "select b.business_id, b.name as Restaurant_Name, count(*) as Num_Reviews, avg(r.stars) as Average_Rating, b.city as City from review r, business b where r.text like '%"+food+"%' AND r.stars>2 and r.business_id=b.business_id AND b.city='"+city+"' group by b.business_id, b.name, b.city order by Num_Reviews desc limit 5"
            toprestaurants = sqlc.sql(query)
            toprestaurants.show()
            toprest = toprestaurants.collect()
            #print("The query was "+query)
            return toprest

                        
        except Exception as inst:
            print(inst.args)
            print(inst)
            return toprest        

    def get_top_foods(self, c, rest):

        restreviewcontent = ""
        try:
            city = c
            restaurant = rest
            #sc = SparkContext("local", "weblog app")
            sc=self.sc
            sqlc = SQLContext(sc)
            bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata")
            bdf.cache()
            #bdf.count() -- 77445
            bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
            bscdf.cache()
            #bscdf.count()
            restrdd = bscdf.rdd.filter(lambda x: restaurant in x[1])
            restrdd.cache()
            print("NUMBER OF RESTAURANTS: ")
            #restrdd.count()
            restcityrdd = restrdd.filter(lambda x: city in x[3])
            restcityrdd.cache()

            if(restcityrdd.count() > 0):
                bid = restcityrdd.collect()
                businessid = bid[0].business_id
                print("the business id of this restaurant is "+businessid)

                r = sqlc.read.json("/user/w205/project1/yelpreviewdata")
                reviewrdd = r.rdd.filter(lambda x: x[3] >2)
                reviewrdd.cache()

                restreviewrdd = reviewrdd.filter(lambda x: businessid in x[0])
                restreviewrdd.cache()
                print("NUMBER OF BUSINESSES: ")
                restreviewrdd.count()
                #lineLengths = lines.map(lambda s: len(s))
                #totalLength = lineLengths.reduce(lambda a, b: a + b)
                userreview = restreviewrdd.map(lambda x: x[4])
                userreview.cache()
                restreviewcontent = userreview.reduce(lambda a, b: a+" "+b)
                restreviewcontent = restreviewcontent.encode('utf-8')
                #restreviewcontent = map(lambda x:x.lower(),restreviewcontent)
                print(restreviewcontent);

                print("Before cleanup words ")
                #cleanup the words
                #validwords = reviewcleanup(restreviewcontent)
                words = restreviewcontent.split()
                                
                print("Before remove stop words ")
                #remove stop words
                validwords = self.removeStopwords(words)

                print("Before count word frequency ")
                #count the frequency of the mentions of food
                wordfreqdict = self.wordListToFreqDict(validwords)

                print("Before sort the words in decreasing order of frequency ")
                #sort the words in decreasing order of frequency
                wordfreqdict1 = self.sortFreqDict(wordfreqdict)

                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(wordfreqdict1)
                
                return wordfreqdict1
             
                                
            else:
                resp = "Review data for the "+restaurant+" restaurant in "+city+" was not found. Please try another search"
                print(resp)
                return resp

              
        except Exception as inst:
            print(inst.args)
            print(inst)
            return "Error"

    def removeStopwords(self, wordlist):
        return [w for w in wordlist if w in foodwords]

    def wordListToFreqDict(self, wordlist):
        wordfreq = [wordlist.count(p) for p in wordlist]
        return dict(zip(wordlist,wordfreq))

    def sortFreqDict(self, freqdict):
        aux = [(freqdict[key], key) for key in freqdict]
        aux.sort()
        aux.reverse()
        return aux

    def reviewcleanup(reviewcontent):
            # Split the tweet into words
            words = reviewcontent.split().lower()

            # Filter out the hash tags, @ and urls
            valid_words = []
            for word in words:

                # Filter the hash tags
                if word.startswith("#"): continue

                # Filter the user mentions
                if word.startswith("@"): continue

                # Filter out the urls
                if word.startswith("http"): continue

                # Strip leading and lagging punctuations
                aword = word.strip("\"?><,'.:;)")

                # now check if the word contains only ascii
                #if len(word) > 0 :
                #    valid_words.append([word])
                if len(aword) > 0 and ascii_string(aword):
                    valid_words.append([aword])

            #if not valid_words: return

            # Emit all the words
            return valid_words

    def ascii_string(s):
      return all(ord(c) < 128 for c in s)

    
