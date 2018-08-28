import os
from pandasdmx import Request
import pandas as pd
estat = Request('ESTAT')
estat.client.config = {'stream': True, 'timeout': 120}

import redis
redisClient = redis.from_url(os.environ['REDISCLOUD_URL'])

def europeanUnionCountries():
    return [
    'BE', 'BG', 'CZ', 'DK', 'DE', 'EE', 'IE', 'EL', 'ES', 'FR',
    'HR', 'IT', 'CY', 'LV', 'LT', 'LU', 'HU', 'MT', 'NL', 'AT',
    'PL', 'PT', 'RO', 'SI', 'SK', 'FI', 'SE', 'UK'
    ]

def getUnemploymentData(countryCodes):
    result = dict()
    data = fetchUnemployment()

    for countryCode in countryCodes:
        values = data[[countryCode]].values.tolist()[::-1]
        result[countryCode] = [item for sublist in values for item in sublist]

    years = []
    for year in list(data.index)[::-1]:
        years.append(year.qyear)

    return {'years': years, 'data': result}

def fetchUnemployment(useCache=True):
    unpacked_data = redisClient.get('unemployment')

    if unpacked_data is None or useCache is False:
        resp = estat.data('une_rt_a', key={'GEO': '+'.join(europeanUnionCountries())})
        data = resp.write(s for s in resp.data.series if s.key.AGE == 'TOTAL')
        data = data.where((pd.notnull(data)), None)
        data = data.loc[:, ('PC_ACT', 'TOTAL', 'T')]

        redisClient.set('unemployment', data.to_msgpack(compress='zlib'))
    else:
        data = pd.read_msgpack(unpacked_data)


def getMigrantsComparisonData(countryCodes):
    return {'to':
        {
            countryCodes[0]: __getLastMigrationFromTo(countryCodes[0], countryCodes[1]),
            countryCodes[1]: __getLastMigrationFromTo(countryCodes[1], countryCodes[0])
        }
    }

def getMigrationFromHistory(migrationTo, migrationFrom):
    popHistory = getMigrationTo(migrationTo)
    popHistory = popHistory.where((pd.notnull(popHistory)), None)
    popHistory = popHistory.xs(migrationFrom, level='CITIZEN', axis=1)
    popHistory = list(popHistory.to_dict().values())[0]

    res = {}
    for key, value in popHistory.items():
        res[key.qyear] = value

    return { migrationFrom: res }

def getMigrationFromHistoryMultipleCountries(migrationTo, migrationFrom):
    res = {}

    for countryCode in migrationFrom:
        res.update(getMigrationFromHistory(migrationTo, countryCode))

    return res

def getMigrationTo(migrationTo, useCache=True):
    unpacked_data = redisClient.get(__migrationToKey(migrationTo))
    if unpacked_data is None or useCache is False:
        resp = estat.data('migr_pop1ctz', key={'GEO': migrationTo, 'SEX': 'T', 'AGE': 'TOTAL'})
        data = resp.write()
        redisClient.set(__migrationToKey(migrationTo), data.to_msgpack(compress='zlib'))
    else:
        data = pd.read_msgpack(unpacked_data)

    return data

def __migrationToKey(migrationTo):
    return "migrationTo" + migrationTo

def __getLastMigrationFromTo(migrationTo, migrationFrom):
    return getMigrationTo(migrationTo).xs(migrationFrom, level='CITIZEN', axis=1).iloc[0][0]
