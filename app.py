from flask import Flask, jsonify, request
from flask_cors import CORS
app = Flask(__name__)
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

@app.route('/population')
def populationOrigin():
    migrationTo = request.args.get('country')
    migrationFrom = request.args.get('from')
    if migrationFrom == None:
        migrationFrom = ['TOTAL']
    else:
        if migrationFrom == 'EU':
            migrationFrom = eurostat_data.europeanUnionCountries()
        else:
            migrationFrom = migrationFrom.split(",")

    return jsonify(eurostat_data.getMigrationFromHistoryMultipleCountries(migrationTo, migrationFrom))
