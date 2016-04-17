#!/usr/bin/env python
from __future__ import absolute_import, print_function, unicode_literals
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import re
import psycopg2

foodwords = [u'pizza',u'pancake',u'pancakes',u'taco',u'burger',u'pasta', u'BBQ', u'sausage', u'biscuit', u'lobster', u'chicken', u'steak', u'chili', u'salmon', u'salad', u'soup', u'dog', u'pork', u'fish']

def main():
    restreviewcontent = ""
    try:
        total = len(sys.argv)
        print ("The total numbers of args passed to the script: %d " % total)
        if int(total) == 3:
            city = str(sys.argv[1])
            restaurant = str(sys.argv[2])
            sc = SparkContext("local", "weblog app")
            sqlc = SQLContext(sc)
            bdf = sqlc.read.json("/user/w205/project1/yelpbusinessdata")
            #bdf.count() -- 77445
            #bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'stars', 'state', 'type')
            bscdf = bdf.select('business_id', 'name', 'categories', 'city', 'full_address', 'review_count', 'state', 'type')
            #bscdf.count()
            restrdd = bscdf.rdd.filter(lambda x: restaurant in x[1])
            print("NUMBER OF RESTAURANTS: ")
            #restrdd.count()
            restcityrdd = restrdd.filter(lambda x: city in x[3])

            if(restcityrdd.count() > 0):
                bid = restcityrdd.collect()
                businessid = bid[0].business_id
                print("the business id of this restaurant is "+businessid)

                r = sqlc.read.json("/user/w205/project1/yelpreviewdata")
                reviewrdd = r.rdd.filter(lambda x: x[3] >2)

                restreviewrdd = reviewrdd.filter(lambda x: businessid in x[0])
                print("NUMBER OF BUSINESSES: ")
                restreviewrdd.count()
                #lineLengths = lines.map(lambda s: len(s))
                #totalLength = lineLengths.reduce(lambda a, b: a + b)
                userreview = restreviewrdd.map(lambda x: x[4])
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
                validwords = removeStopwords(words)

                print("Before count word frequency ")
                #count the frequency of the mentions of food
                wordfreqdict = wordListToFreqDict(validwords)

                print("Before sort the words in decreasing order of frequency ")
                #sort the words in decreasing order of frequency
                wordfreqdict = sortFreqDict(wordfreqdict)

                print(wordfreqdict)
                                
            else:
                print("Review data for the "+restaurant+" restaurant in "+city+" was not found. Please try another search")

        #This section gets a word as an argument and returns the total number of word occurrences in the stream.
        else:
            print("Incorrect Number of Arguments. Correct Usage: /data/Project1/foodfinder.py Phoenix Vovomeena")
         
    except Exception as inst:
        print(inst.args)
        print(inst)

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

def wordListToFreqDict(wordlist):
    wordfreq = [wordlist.count(p) for p in wordlist]
    return dict(zip(wordlist,wordfreq))

def sortFreqDict(freqdict):
    aux = [(freqdict[key], key) for key in freqdict]
    aux.sort()
    aux.reverse()
    return aux

def removeStopwords(wordlist):
    return [w for w in wordlist if w in foodwords]

def ascii_string(s):
  return all(ord(c) < 128 for c in s)

if __name__ == '__main__':
  main() 
