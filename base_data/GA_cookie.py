import os
import pandas
from apiclient.discovery import build
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
from pd_gbq import *


class ga_connect:
    #   Задаём ключ из файла
    credentials = service_account.Credentials.from_service_account_file('kalmuktech-5b35a5c2c8ec.json',)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
    def __init__(self, viewId):
    # Оптределяем аккаунт откуда бер1м данные
        self.viewId = viewId

        
    def request(self, dates, metrics, dimetions, filters = []):    
    # Забираем сырые данные
        return ga_connect.analytics.reports().batchGet(
                    body={
                        'reportRequests': [
                        { "samplingLevel": "LARGE",
                        'viewId': self.viewId,
                        'dateRanges': [dates], 
                        'metrics': [metrics],
                        'dimensions': [dimetions],
                        'pageSize': '10000',
                        'dimensionFilterClauses': [filters]
                        }]
                    }
                    ).execute()
    def report_df(self, dates, metrics, dimetions, filters = []):
#     Отдаём таблицу готовых данных
        report = ga_connect.request(self, dates, metrics, dimetions, filters = [])
        columnHeader = report['reports'][0]['columnHeader']
        columns = columnHeader['dimensions'] + [i['name'] for i in columnHeader['metricHeader']['metricHeaderEntries']]
        data = report['reports'][0]['data']['rows']
        data_table = [i['dimensions'] + i['metrics'][0]['values'] for i in data]           
        return pandas.DataFrame(data_table, columns = columns)
def df_proc(frame):
    frame['ga_date']  = frame['ga_date'].apply(lambda x : pandas.Timestamp(datetime.datetime.strptime(x,'%Y%m%d')))
    return frame


def dedublicate_table(df):
    ens = pandas.DataFrame()
    dict_of_dims_date = {}
    for i in df.itertuples():
        ids = i.ga_dimension1
        if ids in dict_of_dims_date:
            if 'cpc' in i.ga_medium and 'cpc' not in dict_of_dims_date[ids].ga_medium:

                dict_of_dims_date[ids] = i
                
        else:
            dict_of_dims_date[ids] = i
    
    return pandas.DataFrame(dict_of_dims_date.values())

def ga_cookie_refresh():
    
    
    i_cap_GA_old = ga_connect('215283946')
    start = '2020-04-01'
    end = str(datetime.date.today())
    params = { 'dimetions': [{'name': 'ga:dimension1'},
                                {'name': 'ga:date'},
                                {'name': 'ga:fullReferrer'},
                            {'name': 'ga:source'},
                            {'name': 'ga:medium'},
                            {'name': 'ga:campaign'},
                            {'name':'ga:keyword'}
                            ],
            'metrics':[{'expression': 'ga:sessions'}],
            'dates': {'startDate': f'{start}', 'endDate': f'{end}'}
            }
    i_cap_DF_GA=i_cap_GA_old.report_df(**params)
    
    columns = [i.replace(':', '_') for i in i_cap_DF_GA.columns]
    i_cap_DF_GA.columns = columns
    i_cap_DF_GA = df_proc(i_cap_DF_GA)
    df_cooks = dedublicate_table(i_cap_DF_GA)
    df_cooks = df_cooks.drop(columns = ['Index', 'ga_sessions'])
    
    return df_cooks