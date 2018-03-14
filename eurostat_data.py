from pandasdmx import Request
import pandas as pd
estat = Request('ESTAT')
estat.client.config = {'stream': True, 'timeout': 120}

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
    resp = estat.data('migr_pop1ctz', key={'GEO': migrationTo, 'SEX': 'T', 'AGE': 'TOTAL'})
    data = resp.write(s for s in resp.data.series if s.key.CITIZEN == migrationFrom)
    return data.values[0][0]
