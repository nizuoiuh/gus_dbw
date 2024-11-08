import pandas as pd 
from map import months
from datetime import datetime

def szereg_czasowy_cpi_ogolem(data:pd.DataFrame)->pd.DataFrame:
    id_pozycja2 = 6656078
    id_pozycja3 = 6902025
    id_prezentacja = 5
    data = data[(data['id-pozycja-3']==id_pozycja3) & (data['id-pozycja-2']==id_pozycja2) & (data['id-sposob-prezentacji-miara']==id_prezentacja)]
    columns = ['id-daty','id-okres','wartosc']
    data = data[columns]
    data['id-okres'] = data['id-okres'].replace(months)
    data.columns = ['year','month','wartosc']
    data['date'] = pd.to_datetime(data[['year','month']].assign(DAY=1),format='%Y%m%d')
    data.set_index('date',inplace=True)
    data.drop(['year','month'],axis=1,inplace=True)
    return data