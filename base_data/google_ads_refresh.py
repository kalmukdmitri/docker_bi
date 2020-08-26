import os
import requests
import json
import pandas
import datetime
from apiclient.discovery import build
from google.cloud import bigquery
from google.oauth2 import service_account

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
             

    def extract_expanse(self,date_start):
        ga_expanse_params = {
          'dimetions': [{'name': 'ga:adwordsCampaignID'},
                        {'name': 'ga:campaign'},
                        {'name': 'ga:date'},
                        {'name': 'ga:adDestinationUrl'},
                        {'name': 'ga:keyword'}
                        
                        ],
          'metrics': [{'expression': 'ga:adCost' },
                      {'expression': 'ga:adClicks' },
                     ],
          'filters': [{"filters": [{
              "dimensionName": "ga:source",
              "operator": "EXACT",
              "expressions": ["google"]
            }
        ]}]
        }
        start = date_start
        end = str(datetime.date.today())
        ga_expanse_params['dates'] = {'startDate': f'{start}', 'endDate': f'{end}'}
        report = self.request(**ga_expanse_params)
        columnHeader = report['reports'][0]['columnHeader']
        columns = columnHeader['dimensions'] + [i['name'] for i in columnHeader['metricHeader']['metricHeaderEntries']]
        columns = [i.replace(':', '_') for i in columns]
        data = report['reports'][0]['data']['rows']
        data_table = [i['dimensions'] + i['metrics'][0]['values'] for i in data]

        return pandas.DataFrame(data_table, columns = columns)
    
def split_url(url):
    url_parts= url.split('?')
    base = [url_parts[0]]
    base += [i for i in url_parts[-1].split('&') if 'lang=' in i]
    base = ''.join(base)
    return base

def df_proc(frame):

    frame['ga_date']  = frame['ga_date'].apply(lambda x : pandas.Timestamp(datetime.datetime.strptime(x,'%Y%m%d')))
    frame['ga_adCost']  = frame['ga_adCost'].apply(float)
    frame['ga_adClicks']  = frame['ga_adClicks'].apply(int)
    frame['ga_adDestinationUrl']  = frame['ga_adDestinationUrl'].apply(split_url)

    return frame

def ga_refresh():
    date_start = '2020-04-25'
        
    i_cap_GA_old = ga_connect('202103298')
    i_cap_DF_old=i_cap_GA_old.extract_expanse(date_start)
    i_cap_DF_old = df_proc(i_cap_DF_old)
    i_cap_DF_old['Site'] = 'i-cap'
    
#     date_start = '2020-01-01'
#     Workface = ga_connect('195038144')
#     Workface_df=Workface.extract_expanse(date_start)
#     Workface_df = df_proc(Workface_df)
#     Workface_df['Site'] = 'Workface'

#     objs = [Workface_df, i_cap_DF_old]
#     google_ads_joined = pandas.concat(objs, axis=0, join='outer', ignore_index=True, keys=None,
#             levels=None, names=None, verify_integrity=False, copy=True)
    return i_cap_DF_old