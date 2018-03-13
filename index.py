from flask import Flask, jsonify, request
from flask_cors import CORS
app = Flask('Euroflask')
CORS(app)

import eurostat_data

@app.route('/unemployment')
def unemployment():
    geoKeys = request.args.get('countries').split(',')
    return jsonify(eurostat_data.getUnemploymentData(geoKeys))
