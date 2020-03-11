from flask import Flask,jsonify,request
from flask import render_template
import ast

HOST = "localhost"
PORT = 9002
app = Flask(__name__)

labels = []
values = []
tweets = []
probs = []

@app.route("/")
def chart():
    global labels, values
    labels = []
    values = []
    return render_template('dashboard.html',
                           values=values, labels=labels,
                           tweets=tweets, probs=probs)


@app.route('/refresh')
def refresh():
    global labels, values
    # print("labels refresh: " + str(labels))
    # print("data refresh: " + str(values))
    # print("tweets refresh: " + str(tweets))
    # print("probs refresh: " + str(probs))
    return jsonify(slabel=labels, sdata=values, stweets=tweets, sprobs=probs)


@app.route('/update', methods=['POST'])
def update_post():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success", 201

@app.route('/pred', methods=['POST'])
def update_monitor():
    global labels, values, tweets, probs
    if not request.form or 'probs' not in request.form:
        print('fail')
        return "error", 400
    tweets = ast.literal_eval(request.form['tweets'])
    probs = ast.literal_eval(request.form['probs'])
    return "success", 201

if __name__ == "__main__":
    app.run(host=HOST, port=PORT)