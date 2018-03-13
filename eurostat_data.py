from pandasdmx import Request
import pandas as pd
estat = Request('ESTAT')

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
