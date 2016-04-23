from flask import Flask, render_template, json, request
from flask import Blueprint
app = Blueprint('main', __name__)
import os
from engine import FoodSearchEngine
import pyspark, sys, re

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#@app.route("/")
#def main():
#    return render_template('index.html')

@app.route("/home")
def home():
    return render_template('index.html')

@app.route('/showSearch')
def showSearch():
    return render_template('search.html')

@app.route('/foodsearch',methods=['POST','GET'])
@app.route('/foodsearch/<string:city>/<string:restaurant>')
def search(city, restaurant):
        result = food_engine.get_top_foods(city,restaurant)
        # read the posted values from the UI
        #_name = request.json.getform['inputName']
        #_email = request.form['inputEmail']
    
        # validate the received values
        #if _name and _email:
        print result
        return json.dumps(result)
        #else:
        #    return json.dumps({'html':'<span>Enter the required fields</span>'})

@app.route('/restsearch',methods=['POST','GET'])
@app.route('/restsearch/<string:city>/<string:food>')
def fsearch(city, food):
        result = food_engine.get_top_restaurants(city, food)
          
        print result
        return json.dumps(result)
        

def create_app(spark_context):
    global food_engine 

    food_engine  = FoodSearchEngine(spark_context)    

    app1 = Flask(__name__)
    app1.register_blueprint(app)
    return app1    


#if __name__ == "__main__":
#    #foodrecengine = FoodSearchEngine()
#    port = int(os.environ.get("PORT", 8080))
#    app.run(host='0.0.0.0', port=port, debug=True)

