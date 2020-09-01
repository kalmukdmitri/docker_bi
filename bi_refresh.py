import time
import sys
import datetime
import requests
import json
import datetime
import os
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from pd_gbq import gbq_pd

from extract.vk_refresh import vk_refresh

from extract.google_ads_refresh import ga_refresh
from extract.wf_google_ads_refresh import wf_google_ads_refresh

from extract.direct_refresh import y_direct_refresh
from extract.wf_yandex import wf_y_direct_refresh

from extract.fb_refresh import refresh_fb
from extract.callibri import callibri_refresh
from extract.GA_cookie import ga_cookie_refresh



timer_bi = datetime.datetime.today()

modules = [
    {'name': 'Google Ads',
     'table': 'GoogleAds',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : ga_refresh
    },
    {'name': 'Google Ads WF',
     'table': 'wf_google_ads',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : wf_google_ads_refresh
    },
    {'name': 'VK реклама',
     'table': 'VkAds',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : vk_refresh
    },
    {'name': 'Яндекс Директ',
     'table': 'YandexAds',
     'dataset' : 'marketing_bi',
     'modif_type' : 'append',
     'func' : y_direct_refresh
    },
    {'name': 'Wf yandex',
     'table': 'wf_yandex',
     'dataset' : 'marketing_bi',
     'modif_type' : 'append',
     'func' : wf_y_direct_refresh
    },
    {'name': 'Facebook Ads',
     'table': 'FacebookAds',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : refresh_fb
    },
    {'name': 'Callibri data',
     'table': 'callibri_data',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : callibri_refresh
    },
    {'name': 'GA Cookie',
     'table': 'base_ga_cookie',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : ga_cookie_refresh
    },
]

def open_system_refresher(modul):
    systems_log = f"\n Скрипт {modul['name']} запустился "
    datalog = [modul['name']]
    try:
        start_time = datetime.datetime.today()
        table = gbq_pd(modul['table'], modul['dataset'])
        data = modul['func']()
        if len(data) == []:
            systems_log += f"\n Выполнение заняло {datetime.datetime.today() - start_time }"
            systems_log += f"\n Данных из {modul['name']} не получено "
            datalog.append(0)
        else:
            if modul['modif_type'] == 'append':
                table.append(data)
            else:
                table.replace(data)
            systems_log += f"\n Выполнение заняло {datetime.datetime.today() - start_time }"
            systems_log += f"\n Получено {len(data)} строк"
            datalog.append(len(data))
    except:
        systems_log += f"\n При выполнение скрипта {modul['name']} возникла следующая ошибка\n {str(sys.exc_info()[1])}"
        datalog.append(str(sys.exc_info()[1]))
    return systems_log,datalog

log  = f"""\n Выполнение скрипта обновления BI {str(datetime.datetime.today())[:19]} Началось

"""
log_table = [["Дата выполнения",str(datetime.datetime.today())[:19]]]

for i in modules:
    log_file = open_system_refresher(i)
    log+= log_file[0]
    log_table.append(log_file[1])
    
from extract.amo_tilda_reshresh import Amo_refresh
from extract.shop_icap import shop_icap_tables
from report.refresh_reports import bi_report_refresh
from report.personal_reports import refresh_personal_reports
from extract.wf_amo import wf_amo_refresh
from transform_mix.wf_cookies import refresh_wf_ga_tables

closed_modules = [
    {
        'name' : 'Таблицы АМО i-cap',
        'func' : Amo_refresh
    },
    {
        'name' : 'BI отчёт по АМО i-cap',
        'func' : bi_report_refresh
    },
    {
        'name' : 'Личные отчёты',
        'func' : refresh_personal_reports
    },
    {
        'name' : 'Таблицы АМО и базы workface',
        'func' : wf_amo_refresh
    },
    {
        'name' : 'Таблицы Workface cookie',
        'func' : refresh_wf_ga_tables
    },
    {
        'name' : 'Таблицы shop i-cap cookie',
        'func' : shop_icap_tables
    }
]

def closed_system_refresher(modul):
    systems_log = f"\n Скрипт {modul['name']} запустился "
    data_lod = [modul['name']]
    try:
        start_time = datetime.datetime.today()
        log = modul['func']()
        systems_log += f"\n Выполнение заняло {datetime.datetime.today() - start_time }"
        systems_log += f"\n {log}"
        data_lod.append("ОК")
    except:
        systems_log += f"\n При выполнение скрипта {modul['name']} возникла следующая ошибка\n {str(sys.exc_info()[1])}"
        data_lod.append(str(sys.exc_info()[1]))
    return systems_log,data_lod


for i in closed_modules:
    log_file_с = closed_system_refresher(i)
    log+= log_file_с[0]
    log_table.append(log_file_с[1])
    
log += f"\nВыполнение всех скриптов завершено за {datetime.datetime.today() - timer_bi }\n"
    
SCOPES = ['https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/documents']

credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
service = build('docs', 'v1', credentials=credentials)

def add_log_to_doc(docname, log):
    
    googe_request = service.documents().get(documentId = docname).execute()
    token_str=googe_request['body']['content'][1]['paragraph']['elements'][0]['textRun']['content']
    doc_lenth = len(token_str)
    requests = [
        {'insertText': {
                'location': {
                    'index': doc_lenth,
                },
                'text': str(log)
            }
        }
    ]
    result = service.documents().batchUpdate(
    documentId=docname, body={'requests': requests}).execute()

log_file_name = '15BR7oalSjLt_q619rF74LUvn6qFCzYY56y9geHJ-rVo'
add_log_to_doc(log_file_name,f'\n {str(log)}')

import json, requests,datetime
import mysql.connector as mysql
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
gc = gspread.authorize(credentials)

def GoogleListUpdate(doc_Name,list_Name,dataframe, offsetright=1, offsetdown=1):
    edex = len(list(dataframe.axes[0]))
    eter = len(list(dataframe.axes[1]))
    print(edex,eter)
    sh = gc.open(doc_Name)
    worksheet1 = sh.worksheet(list_Name)
    for dexlet in range(0,eter):
        i = 0
        cell_list = worksheet1.range(offsetdown,dexlet+offsetright,edex+offsetdown-1,dexlet+offsetright)
        for cell in cell_list:
            insert_data = int(dataframe[dexlet][i]) if str(dataframe[dexlet][i]).isdigit() else dataframe[dexlet][i]
            cell.value = insert_data
            i+=1  
        worksheet1.update_cells(cell_list)

def GoogleListextract(doc_Name,list_Name):
    sh = gc.open(doc_Name)
    worksheet1 = sh.worksheet(list_Name)
    values_list = worksheet1.row_values(1)
    end_ls = []
    for i in range(1,len(values_list)+1):
        end_ls.append(list(worksheet1.col_values(i)))

    return end_ls

def modify_log(old_log, new_log):
    heads_old = { head[0]:num for num,head in enumerate(old_log)}
    heads_new  = { head[0]:num for num,head in enumerate(new_log)}
    
    for i in old_log:
        if i[0] not in heads_new:
            i.insert(1,0)

    for i in new_log:
        if i[0] in heads_old:
            old_row = heads_old[i[0]]
            old_log[old_row].insert(1,i[1])
        else:
            old_log.append(i)
    return pd.DataFrame(old_log).transpose()

old_log= GoogleListextract('logs','log')
new_log = modify_log(old_log,log_table)

GoogleListUpdate('logs','log',new_log)