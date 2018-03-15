from pandasdmx import Request
import pandas as pd
estat = Request('ESTAT')
estat.client.config = {'stream': True, 'timeout': 120}

import redis
redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)
import pandas as pd

def getUnemploymentData(countryCodes):
    result = dict()
    geoKeys = "+".join(countryCodes)
    resp = estat.data('une_rt_a', key={'GEO': geoKeys})
    data = resp.write(s for s in resp.data.series if s.key.AGE == 'TOTAL')
    data = data.where((pd.notnull(data)), None)
    data = data.loc[:, ('PC_ACT', 'TOTAL', 'T')]

    for countryCode in countryCodes:
        values = data[[countryCode]].values.tolist()[::-1]
        result[countryCode] = [item for sublist in values for item in sublist]

    years = []
    for year in list(data.index)[::-1]:
        years.append(year.qyear)

    return {'years': years, 'data': result}

def getMigrantsComparisonData(countryCodes):
    return {'to':
        {
            countryCodes[0]: __getMigrationFromTo(countryCodes[0], countryCodes[1]),
            countryCodes[1]: __getMigrationFromTo(countryCodes[1], countryCodes[0])
        }
    }

def __getMigrationFromTo(migrationTo, migrationFrom):
    unpacked_data = redisClient.get("migrationFromTo" + migrationTo + migrationFrom)
    if unpacked_data is None:
        resp = estat.data('migr_pop1ctz', key={'GEO': migrationTo, 'SEX': 'T', 'AGE': 'TOTAL'})
        data = resp.write()
        redisClient.set("migrationFromTo" + migrationTo + migrationFrom, data.to_msgpack(compress='zlib'))
    else:
        data = pd.read_msgpack(unpacked_data)

    return data.xs(migrationFrom, level='CITIZEN', axis=1).iloc[0][0]
