import os
import pandas
from apiclient.discovery import build
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
from pd_gbq import *
import requests
import json
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector as mysql

from doc_token import get_tokens

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


def shop_icap_tables():
    log = ""
    token = get_tokens()['wf_base']
    shop_i_cap_GA = ga_connect('195060854')
    start = '2020-04-01'
    end = str(datetime.date.today())
    params = { 'dimetions': [{'name': 'ga:dimension1'},
                                {'name': 'ga:dimension2'},
                             {'name': 'ga:date'},
                            ],
            'metrics':[{'expression': 'ga:sessions'}],
            'dates': {'startDate': f'{start}', 'endDate': f'{end}'}
            }
    i_cap_DF_GA=shop_i_cap_GA.report_df(**params)

    columns = [i.replace(':', '_') for i in i_cap_DF_GA.columns]
    i_cap_DF_GA.columns = columns
    i_cap_DF_GA = df_proc(i_cap_DF_GA)
    df_cooks = i_cap_DF_GA.drop(columns = ['ga_sessions'])

    params = { 'dimetions': [{'name': 'ga:dimension2'},
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
    cooks_2_sourese=shop_i_cap_GA.report_df(**params)
    columns = [i.replace(':', '_') for i in cooks_2_sourese.columns]
    cooks_2_sourese.columns = columns
    cooks_2_sourese = df_proc(cooks_2_sourese)
    cooks_2_sourese = cooks_2_sourese.drop(columns = ['ga_sessions'])

    cooks_2_sourese_table = gbq_pd('shop_cooks_sourse', 'marketing_bi')
    cooks_2_sourese_table.replace(cooks_2_sourese)
    log += f"По таблице shop_cooks_sourse обновилось {len(cooks_2_sourese)} строк \n"

    df_cooks_table = gbq_pd('shp_icap_wf_cooks', 'marketing_bi')
    df_cooks_table.replace(df_cooks)
    log += f"По таблице shp_icap_wf_cooks обновилось {len(df_cooks)} строк \n"

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


    get_buyers = """SELECT
      u.user_id AS user_id,
      u.phone
    FROM
      users AS u

    JOIN
      `companies` AS c
    ON
      c.user_id = u.user_id

    INNER JOIN (
      SELECT
        DISTINCT consumer_profile_id AS cmp_id
      FROM
        `deals`
      WHERE
        supplier_company_id = 46) AS cmp
    ON
      cmp.cmp_id = c.company_id"""

    companies = query_df(get_buyers,token)
    wf_buyers = gbq_pd('wf_buyers_icap', 'marketing_bi')
    wf_buyers.replace(companies)
    log += f"По таблице wf_buyers_icap обновилось {len(companies)} строк \n"
    return log
