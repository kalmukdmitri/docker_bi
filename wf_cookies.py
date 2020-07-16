import os
import pandas
from apiclient.discovery import build
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
from pd_gbq import *
import mysql.connector as mysql
from doc_token import get_tokens
token = get_tokens()

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
        report = ga_connect.request(self, dates, metrics, dimetions, filters = filters)
        columnHeader = report['reports'][0]['columnHeader']
        columns = columnHeader['dimensions'] + [i['name'] for i in columnHeader['metricHeader']['metricHeaderEntries']]
        data = report['reports'][0]['data']['rows']
        data_table = [i['dimensions'] + i['metrics'][0]['values'] for i in data]           
        return pandas.DataFrame(data_table, columns = columns)

    


def query_df(qry, token):
    devDB  = token
    cnx = mysql.connect(**devDB)
    cursor = cnx.cursor()
    cursor.execute(qry)
    resula = [i for i in cursor]
    field_names = [i[0] for i in cursor.description]
    cursor.close()
    cnx.close()
    db_data_df = pd.DataFrame(resula,
                           columns = field_names)
    return db_data_df
    
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
    
    
def refresh_wf_ga_tables():
    
    
    i_cap_GA_old = ga_connect('195060854')
    start = '2020-07-01'
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
    ga_table = gbq_pd('ga_wf_cookies', 'wf_bi')
    ga_table.replace(df_cooks)

    print('ga_wf_cookies рефр')
    print(len(df_cooks))

    q_user = """SELECT 
        users_uid.user_id as user_id,
        u_id,
        email,
        phone,
        create_date
    FROM `users_uid`
    left join (SELECT user_id, email, phone, from_unixtime(create_date) as create_date FROM `users`) as users on users.user_id = users_uid.user_id"""
    users_df = query_df(q_user,token['wf_base'])
    wf_user_id = gbq_pd('users_id', datasetId = 'wf_bi')
    wf_user_id.replace(users_df)
    
    print('users_id рефр')
    print(len(users_df))
    
  

    # Получить данных по хосту

    params = { 'dimetions': [{'name': 'ga:dimension1'},
                            {'name': 'ga:hostname'},
                             {'name': 'ga:date'}, 
                             {'name': 'ga:hour'},
                            ],
            'metrics':[{'expression': 'ga:sessions'}],
            'dates': {'startDate': f'{start}', 'endDate': f'{end}'}
            }

    hosts_ga_df=i_cap_GA_old.report_df(**params)

    columns = [i.replace(':', '_') for i in hosts_ga_df.columns]
    hosts_ga_df.columns = columns
    hosts_ga_df['ga_sessions'] = hosts_ga_df['ga_sessions'].apply(lambda x: int(x))

    dates_cook = [i[1].ga_date+i[1].ga_hour for i in  hosts_ga_df.iterrows()]
    hosts_ga_df['dates'] = dates_cook
    hosts_ga_df = hosts_ga_df.drop(columns = ['ga_date', 'ga_hour', 'ga_sessions'])

    df_data = {}
    for i in hosts_ga_df.iterrows():
        row = i[1]

        if row.ga_dimension1 in df_data:
            if df_data[row.ga_dimension1].dates > row.dates:
                df_data[row.ga_dimension1] = row
        else:
            df_data[row.ga_dimension1] = row
    hosts = pandas.DataFrame(df_data.values())

    # Обновить данных по хосту
    t_wf_hosts = gbq_pd('wf_hosts', datasetId = 'wf_bi')
    t_wf_hosts.replace(hosts)
    
    print('Хост рефр')
    print(len(hosts))

    # Получить данных по домену

    params = { 'dimetions': [{'name': 'ga:eventCategory'},
                             {'name': 'ga:eventAction'},
                             {'name': 'ga:eventLabel'},
                            ],
            'metrics':[{'expression': 'ga:totalEvents'}],
            'dates': {'startDate': f'{start}', 'endDate': f'{end}'},
            'filters': [{"filters": [{
                  "dimensionName": "ga:eventCategory",
                  "operator": "EXACT",
                  "expressions": ["reg_id"]
                }
            ]}]
            }
    wf_domain=i_cap_GA_old.report_df(**params)
    wf_domain['reg_domain'] = wf_domain['ga:eventAction'].apply(lambda x: x.replace('https://', '').split('/')[0])
    columns = [i.replace(':', '_') for i in wf_domain.columns]
    wf_domain.columns = columns
    wf_domain = wf_domain.drop(columns = ['ga_totalEvents', 'ga_eventCategory', 'ga_eventAction'])

    # Обновить данных по домену

    t_wf_domains = gbq_pd('wf_domain', datasetId = 'wf_bi')
    t_wf_domains.replace(wf_domain)
    
    print('Домен рефр')
    print(len(wf_domain))

    # Получить данные по визитам

    params = { 'dimetions': [{'name': 'ga:dimension1'}
                            ],
            'metrics':[{'expression': 'ga:sessions'}],
            'dates': {'startDate': f'{start}', 'endDate': f'{end}'}
            }

    visits_df=i_cap_GA_old.report_df(**params)

    columns = [i.replace(':', '_') for i in visits_df.columns]
    visits_df.columns = columns
    visits_df['ga_sessions'] = visits_df['ga_sessions'].apply(lambda x: int(x))

    # Обновить данные по визитам

    visits_table = gbq_pd('user_sess', 'wf_bi')
    visits_table.replace(visits_df)
    print('Визиты рефр')
    print(len(visits_df))
