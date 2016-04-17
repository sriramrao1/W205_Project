from flask import Flask, render_template, json, request
#from flask_restful import Resource, Api
import os
import foodfinder
import restaurantfinder
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route("/")
def main():
    return render_template('index.html')

@app.route("/home")
def home():
    return render_template('index.html')

@app.route('/showSearch')
def showSearch():
    return render_template('search.html')

#def main():
#    return "Welcome!"

@app.route('/search',methods=['POST','GET'])
@app.route('search/<string:food>")
def search():
        
        # read the posted values from the UI
        #_name = request.form['inputName']
        #_email = request.form['inputEmail']
    
        # validate the received values
        #if _name and _email:
    return json.dumps({'html':'<span>All fields good !!</span>'})
        #else:
        #    return json.dumps({'html':'<span>Enter the required fields</span>'})
        
#def search():
        #return render_template('index.html')
        # read the posted values from the UI
        #_name = request.form['inputName']
        #_email = request.form['inputEmail']
    
        # validate the received values
        #if _name and _email:
    #return json.dumps({'html':'<span>All fields good !!</span>'})
        #else:
        #    return json.dumps({'html':'<span>Enter the required fields</span>'})

    


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=True)

