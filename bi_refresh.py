from pd_gbq import *
from base_data.vk_refresh import *
from base_data.google_ads_refresh import *
from base_data.direct_refresh import *
from base_data.fb_refresh import *
from base_data.callibri import *
from base_data.GA_cookie import *

import time
import sys
import datetime
import requests
import json
import datetime
import os

timer_bi = datetime.datetime.today()

modules = [
    {'name': 'Google Ads',
     'table': 'GoogleAds',
     'dataset' : 'marketing_bi',
     'modif_type' : 'replace',
     'func' : ga_refresh
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
    try:
        start_time = datetime.datetime.today()
        table = gbq_pd(modul['table'], modul['dataset'])
        data = modul['func']()
        if len(data) == []:
            systems_log += f"\n Выполнение заняло {datetime.datetime.today() - start_time }"
            systems_log += f"\n Данных из {modul['name']} не получено "
        else:
            if modul['modif_type'] == 'append':
                table.append(data)
            else:
                table.replace(data)
            systems_log += f"\n Выполнение заняло {datetime.datetime.today() - start_time }"
            systems_log += f"\n Получено {len(data)} строк"
    except:
        systems_log += f"\n При выполнение скрипта {modul['name']} возникла следующая ошибка\n {str(sys.exc_info()[1])}"

    return systems_log

log  = f"""\n Выполнение скрипта обновления BI {str(datetime.datetime.today())[:19]} Началось

"""

for i in modules:
    log+= open_system_refresher(i)

from base_data.amo_tilda_reshresh import *
from base_data.shop_icap import *
from refresh_reports import *
from personal_reports import *
from base_data.wf_amo import *
from wf_cookies import *

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
    try:
        start_time = datetime.datetime.today()
        log = modul['func']()
        systems_log += f"\n Выполнение заняло {datetime.datetime.today() - start_time }"
        systems_log += f"\n {log}"
    except:
        systems_log += f"\n При выполнение скрипта {modul['name']} возникла следующая ошибка\n {str(sys.exc_info()[1])}"

    return systems_log


for i in closed_modules:
    log+= closed_system_refresher(i)
    
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