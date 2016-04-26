#!/usr/bin/env python
from __future__ import absolute_import, print_function, unicode_literals
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import re
import logging
import pprint
import psycopg2
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
food = ""

foodwords = [u'pizza',u'pancake',u'pancakes',u'taco',u'burger',u'pasta', u'BBQ', u'sausage', u'biscuit', u'lobster', u'chicken', u'steak', u'chili', u'salmon', u'salad', u'soup', u'dog', u'pork', u'fish']

class FoodSearchEngine:
           
    """ A Food Search Engine """
    def __init__(self, sc):
            """Init the food search engine given a Spark context"""

            logger.info("Starting up the food search engine: ")
            self.sc = sc
            sqlc = SQLContext(sc)

            
            

    ## This Function is to get the Top 5 restuarants that serve the <food> in <c> city
    ## Learning Objective - Use Spark-SQL for this function
    def get_top_restaurants(self, c, food1):
        
        try:
            sc = self.sc
            sqlc = SQLContext(sc)
            food = food1
            city = c

            #Check if the Database already has the results of this request
            df = sqlc.read.format("jdbc").options(url="jdbc:postgresql://localhost:5432/foodninjadb?user=w205&password=postgres", dbtable="toprestaurants",driver="org.postgresql.Driver").load()
            resultrdd = df.rdd.filter(lambda x: city in x[0]).filter(lambda x: food in x[1])

            #If the Database has the data then display the result
            if(resultrdd.count() > 0):
                return sqlc.createDataFrame(resultrdd).collect()

            #Else run the pyspark job to get the result and display it
            else:
                
                # Read the Yelp Business data JSON file from HDFS and store the required columns in a dataframe (COUNT OF RECORDS = 77445)
                bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata").select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
     
                # Store the restaurant business data in a temp table for querying
                bdf.registerTempTable("business")
                
                # Read the Yelp Review data JSON file from HDFS and store all the columns in a dataframe
                rdf = sqlc.read.json("/user/w205/project1/yelpreviewdata")
                #rdf.printSchema()

                # Filter only the rows where the user rating is greater than 2 
                # Since we are interested only in the top rated restaurants, we filter out lower rated reviews
                #reviewrdd = rdf.rdd.filter(lambda x: x[3] >2)

                # Store the review data in a temp table for querying
                #sqlc.createDataFrame(reviewrdd).registerTempTable("review")

                rdf.registerTempTable("review")
                
                #Run the query to get the top 5 restaurants 
                query = "select b.city as City, '"+ food + "' as FOOD, b.business_id as Bid, b.name as Name, count(*) as NReviews, avg(r.stars) as AvgRating  from review r, business b where r.text like '%"+food+"%' AND r.stars>2 and r.business_id=b.business_id AND b.city='"+city+"' group by b.business_id, b.name, b.city order by NReviews desc limit 5"
                toprestaurants = sqlc.sql(query)
                #toprestaurants is a dataframe
                #toprestaurants.show()
                #print("The query was "+query)

                #Since this request was not in the Database, write it now to the database
                props = {
                    "user": "w205",
                    "password": "postgres"
                }
                toprestaurants.write.jdbc(url="jdbc:postgresql://localhost:5432/foodninjadb", table="toprestaurants", mode="append", properties=props)

                return toprestaurants.collect()

                        
        except Exception as inst:
            logger.info(inst.args)
            logger.info(inst)
            return "There was an error in the execution of this request. Please check the input and place the request again."

       

    ## This Function is to get the Top 5 rated food items that are served in the <rest> restaurant in <c> city
    ## Learning Objective - Use PySpark for this function
    def get_top_foods(self, c, rest):

        restreviewcontent = ""
        try:
            city = c
            restaurant = rest
            sc=self.sc
            sqlc = SQLContext(sc)

            # Read the Yelp Business data JSON file and store the required columns in a dataframe (COUNT OF RECORDS = 77445)
            bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata").select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
            bdf.cache()
            
            # Filter or Search for the restaurant and city
            restcityrdd = bdf.rdd.filter(lambda x: restaurant in x[1]).filter(lambda x: city in x[3])
            logger.info("NUMBER OF RESTAURANTS: ")
            
            #if the restaurant is found in the city then obtain its review content from Yelp Review data JSON file
            if(restcityrdd.count() > 0):
                bid = restcityrdd.collect()
                businessid = bid[0].business_id
                logger.info("the business id of this restaurant is "+businessid)

                # Read the Yelp Review data JSON file and store all the columns in a dataframe
                ## There are only a few columns hence storing all columns
                r = sqlc.read.json("/user/w205/project1/yelpreviewdata")

                # Filter the Review data for the specific business ID and only get those reviews where the user rating is greater than 2
                ## We are only interested in food dishes that are rated highly by the users, hence the filter for ratings
                restreviewrdd = r.rdd.filter(lambda x: businessid in x[0]).filter(lambda x: x[3] >2)
                
                # Map (or extract) the review content from each row of data and concatenate them together
                restreviewcontent = restreviewrdd.map(lambda x: x[4]).reduce(lambda a, b: a+" "+b)

                #Encode the content as UTF-8 to account for special and non-latin characters 
                restreviewcontent = restreviewcontent.encode('utf-8')
                logger.info(restreviewcontent);

                logger.info("Before cleanup words ")
                #cleanup the words
                #validwords = reviewcleanup(restreviewcontent)
                words = restreviewcontent.split()
                                
                logger.info("Before remove stop words ")
                #remove stop words
                validwords = self.removeStopwords(words)

                logger.info("Before count word frequency ")
                #count the frequency of the mentions of food
                wordfreqdict = self.wordListToFreqDict(validwords, city, restaurant)

                logger.info("Before sort the words in decreasing order of frequency ")
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
        #w = []
        #for word in wordlist:
        #    if (word.encode('utf-8')) in foodwords:
        #        w.append(word)
        #return w

    def wordListToFreqDict(self, wordlist, city, restaurant):
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

    ## This Function is to get the Top 5 cities where there is opportunity to start a restaurant with the <category> food category
    ## This is stretch goal for the project
    def get_top_cities(self, category):
        
        try:
            sc = self.sc
            sqlc = SQLContext(sc)
            foodcategory = category

            #Check if the Database already has the results of this request
            df = sqlc.read.format("jdbc").options(url="jdbc:postgresql://localhost:5432/foodninjadb?user=w205&password=postgres", dbtable="topcities",driver="org.postgresql.Driver").load()
            resultrdd = df.rdd.filter(lambda x: foodcategory in x[0])

            #If the Database has the data then display the result
            if(resultrdd.count() > 0):
                return sqlc.createDataFrame(resultrdd).collect()

            #Else run the Spark-SQL job to get the result and display it
            else:
             
                # Read the Yelp Business data JSON file from HDFS and store the required columns in a dataframe (COUNT OF RECORDS = 77445)
                bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata").select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type', 'stars')
     
                # Store the restaurant business data in a temp table for querying
                bdf.registerTempTable("business")
                
                #Run the query to get the 5 cities with poor ratings for the food category
                query = "select '"+ foodcategory + "' as Category, city as City, count(*) as Num_Restaurants, avg(stars) as Average_Rating  from business b where b.categories[1] like '%"+foodcategory+"%' group by b.city order by Average_Rating, Num_Restaurants, City asc limit 5"
                topcities = sqlc.sql(query)
                #topcities.show()
                #print("The query was "+query)

                #Since this request was not in the Database, write it now to the database
                props = {
                    "user": "w205",
                    "password": "postgres"
                }
                topcities.write.jdbc(url="jdbc:postgresql://localhost:5432/foodninjadb", table="topcities", mode="append", properties=props)
                
                return topcities.collect()

                        
        except Exception as inst:
            logger.info(inst.args)
            logger.info(inst)
            return "There was an error in the execution of this request. Please check the input and place the request again."        
