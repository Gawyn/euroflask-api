from flask import Flask, jsonify, request
from flask_cors import CORS
app = Flask('Euroflask')
CORS(app)

import eurostat_data

@app.route('/unemployment')
def unemployment():
    geoKeys = request.args.get('countries').split(',')
    return jsonify(eurostat_data.getUnemploymentData(geoKeys))

@app.route('/migrants-comparison')
def migrantsComparison():
    geoKeys = request.args.get('countries').split(',')
    return jsonify(eurostat_data.getMigrantsComparisonData(geoKeys))

@app.route('/migration-history-from')
def populationOrigin():
    migrationTo = request.args.get('to')
    migrationFrom = request.args.get('from')
    return jsonify(eurostat_data.getMigrationFromHistory(migrationTo, migrationFrom))
