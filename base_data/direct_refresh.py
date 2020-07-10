import pandas
import os
import requests
import json
import datetime
from pd_gbq import *

class direct_acc:
    client_id = 'a7be650488b1441f92b3e51f09de5132'
    
    def __init__(self, token,name , metr_id='42660959', direct_client_logins = '' ):
        """Получаем токен и имя подключения"""
        
        self.headers = {"Authorization": "Bearer " + token}
        self.direct_client_logins = direct_client_logins
        self.metr_id = str(metr_id)
        self.name = name
        
    def get_data(self, dimestions, metrics, date1, date2 = str(datetime.date.today())):
        """Забираем сырые данные из метрики"""
        
        base_url = "https://api-metrika.yandex.net/stat/v1/data/?accuracy=full&limit=100000&attribution=last&group=day"
        base_url +=f'&id={self.metr_id}'
        base_url +=f'&dimensions={dimestions}'
        base_url +=f'&metrics={metrics}'
        base_url +=f'&date1={date1}'
        base_url +=f'&date2={date2}'
        base_url += f"&direct_client_logins={self.direct_client_logins}"
        print(base_url)
        metrica_answer = requests.get(base_url, headers=self.headers)
        results = json.loads(metrica_answer.text)
        return results
    
    def get_dates_from(self, dt_str):
        """Получаем даты списком от указаннов до указанной"""
        dates =[]
        tday = datetime.date.today() - datetime.timedelta(days=1)
        start_date = datetime.date.fromisoformat(dt_str)
        c = 0
        while start_date < tday and c<1000:
            dates.append(str(start_date))
            start_date = start_date + datetime.timedelta(days=1)
        return dates
    
    def proccess_direct_data(self, row):
        """ Обрабатываем входящие данные по стандарту"""
        
        proccced_row = []
        date = row['query']['date2']
        for data in row['data']:
            
            internal_row = [i['name'] for i in data['dimensions']]
            internal_row.extend(data['metrics'])
            internal_row.append(date)
    #       добавляем конечную дату
            proccced_row.append(internal_row)
        return proccced_row
    
    def get_lists(self, dt_str,  dimestions, metrics, data_procces = None):
        """На основе созданных выше функций получаем данные по списку дат
        Обрабатываем их выдаём чистую таблицу"""
        
        if not data_procces:
            data_procces = self.proccess_direct_data
        dates = self.get_dates_from(dt_str)
        end_lst = []
        if len(dates) == 0: 
            print("Данные актуальны")
            return []
        print(f"Нужны данные за {len(dates)} дат")
        req_counter = 0
        for date in dates:
            row = self.get_data(dimestions = dimestions,
                                         metrics = metrics,
                                         date1 = date,
                                         date2 = date)
            if req_counter % 20 == 0:
                print(f"Осталось {len(dates)-req_counter}")
            req_counter+=1
            end_lst.extend(data_procces(row))
        return end_lst

def dedouble_dict_table(table):
    rows_dict = {}
    for i in table:
        table_id = tuple(i[:2])
        if table_id in rows_dict:
            if rows_dict[table_id][-1] < i[-1]:
                rows_dict[table_id] = i[2:]
        else:
            rows_dict[table_id] = i[3:]
    return [list(i)+e[:-2] for i,e in rows_dict.items()]

def get_match_table( ym_class, date_st = '2020-04-25'):
    dimestions = "ym:s:<attribution>DirectClickOrder,ym:s:<attribution>DirectPhraseOrCond,ym:s:UTMSource,ym:s:UTMCampaign,ym:s:UTMTerm"
    metrics = "ym:s:visits"
    x = ym_class.get_data( dimestions, metrics, date_st)
    y = ym_class.proccess_direct_data(x)
    print(len(y))
    header_str = dimestions+","+metrics
    
    header_str = header_str.replace(":","_")
    header_str = header_str.replace("<attribution>","")
    header_str = header_str.replace("<currency>","")
    pure_head_table = dedouble_dict_table(y)
    print(len(pure_head_table))
    columns_headers = header_str.split(",")
    return pandas.DataFrame(pure_head_table, columns = columns_headers[:2]+columns_headers[-3:-1])

def match_dict(df):
    main_dict = {}
    for i in df.itertuples():
        index = i.ym_s_DirectClickOrder +'/'+i.ym_s_DirectPhraseOrCond
        if index in main_dict:
            pass
        else:
            main_dict[index] = [i.ym_s_UTMCampaign,i.ym_s_UTMTerm]
    return main_dict

def add_utm_to_df_yandex(dt_frm,utms):
    c = 0
    end_list  =[]
    for i in dt_frm.itertuples():
        row = []
        index_df = i.ym_ad_DirectOrder +'/'+i.ym_ad_DirectPhraseOrCond
        row.extend(i[1:])
        if index_df in utms:
            
            row.extend(utms[index_df])
        else:
            c +=1
            row.extend([None,None])
        end_list.append(row)
    columns= list(dt_frm.columns) + ['Utm_campaign', 'utm_term']
    return pandas.DataFrame(end_list, columns = columns) 


# Подключаемся с метрике и забираем данные
def y_direct_refresh():
    start_time = datetime.datetime.today()
    
    from doc_token import get_tokens
    token = get_tokens()

    dimestions = "ym:ad:<attribution>DirectOrder,ym:ad:<attribution>DirectPhraseOrCond"
    metrics = "ym:ad:<currency>AdCost,ym:ad:clicks"
    
    try:
        bq_yandex=gbq_pd( 'YandexAds', 'marketing_bi')
        q_last_date = "SELECT date(max(date)) as last_date  FROM `kalmuktech.marketing_bi.YandexAds`"
        last_date = bq_yandex.df_query(q_last_date).iloc[0,0].date()
        date = str(last_date)[:10]
        print(date)
    except:
        date = str(datetime.datetime.today())
    
    header_str = dimestions+","+metrics
    header_str = header_str.replace(":","_")
    header_str = header_str.replace("<attribution>","")
    header_str = header_str.replace("<currency>","")
    column_headers = header_str.split(",") + ['date']

    caps = token['yandex']['caps']
    caps_dirc = direct_acc(**caps)
    probk_list = caps_dirc.get_lists(date, dimestions,metrics)
    probk_df = pandas.DataFrame(probk_list, columns = column_headers)
    
    wf = token['yandex']['wf']
    wf_dirc = direct_acc(**wf)
    wf_list = wf_dirc.get_lists(date, dimestions,metrics)
    wf_df = pandas.DataFrame(wf_list, columns = column_headers)
    
    if probk_list == [] and wf_list == []:
        print(f"Выполнение заняло {datetime.datetime.today() - start_time }")
        return pandas.DataFrame([], columns = column_headers)
    
    direct_table_full = pandas.concat([probk_df,wf_df ], axis=0, join='outer', ignore_index=True, keys=None,
            levels=None, names=None, verify_integrity=False, copy=True)
    direct_table_full['date'] = direct_table_full['date'].apply(pandas.Timestamp)


    cross_data1 = get_match_table(ym_class = caps_dirc)
    cross_data2 = get_match_table(ym_class = wf_dirc)

    cross_data = pandas.concat([cross_data1,cross_data2 ], axis=0, join='outer', ignore_index=True, keys=None,
            levels=None, names=None, verify_integrity=False, copy=True)

    utm_data = match_dict(cross_data)

    final_table_yandex =add_utm_to_df_yandex(direct_table_full,utm_data)
    print(f"Всего получено {len(final_table_yandex)} строк")
    print(f"Выполнение заняло {datetime.datetime.today() - start_time }")
    if len(final_table_yandex) > 0:
        bq_yandex.append(final_table_yandex)
    return final_table_yandex
