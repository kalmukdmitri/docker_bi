import json
import requests
import datetime
import pandas
from pandas import DataFrame as df
from pd_gbq import *
import time 

from doc_token import get_tokens
passwords = get_tokens()

class callibri():

    base = 'https://api.callibri.ru/'
    def __init__(self, token = passwords['callibri'], 
                user_email = 'user_email=kalmukdmitri@gmail.com',
                 site_id = 'site_id=37222'):
        self.token = token
        self.user_email = user_email
        self.site_id= site_id
    def get_stats(self, date1,date2):
        date1= date1.strftime("%d.%m.%Y")
        date2 = date2.strftime("%d.%m.%Y")
        request_url  = f"{callibri.base}site_get_statistics?{self.token}&{self.user_email}&{self.site_id}&date1={date1}&date2={date2}"
        answer = requests.get(request_url)
        results = json.loads(answer.text)
        return results
    
    
def date_pairs(date1, date2):
    pairs= []
    while date2 > date1:
        prev_date = date2 - datetime.timedelta(days=6) if date2 - datetime.timedelta(days=6) > date1 else date1
        pair = [prev_date, date2]   
        date2 -= datetime.timedelta(days=7)
        pairs.append(pair)
    pairs.reverse()
    return pairs

def process_data(callibri_data):
    c = 0
    for lead_type in callibri_data['channels_statistics'][0]:
#       Пропускаем неинформативные блоки
        if lead_type in ['numbers', 'name_channel']:
            continue
        typed_data = callibri_data['channels_statistics'][0][lead_type]
        
        if c == 0:
            end_dataframe = df(typed_data)
            end_dataframe.insert(0,column ='type',value = lead_type)
            c+=1
        elif type(typed_data) == list and len(typed_data)> 0: 
            
            end_dataframe_extra = df(callibri_data['channels_statistics'][0][lead_type])
            end_dataframe_extra.insert(0,column ='type',value = lead_type)
            end_dataframe = end_dataframe.append(end_dataframe_extra)
        else:
            continue
    to_drop_colums = ['channel_id','is_lid',
                'comment','query','traffic_type',
                'landing_page','responsible_manager',
               'custom_variable','content',
               'form_name','status','link_download',
               'duration','conversations_number',
               'lid_landing','accurately','call_status']
    drop_colums = [i for i in to_drop_colums if i in end_dataframe.columns]
    end_dataframe = end_dataframe.drop(columns = drop_colums)
    
    end_dataframe['date'] = [pandas.Timestamp(datetime.datetime.strptime(i[:10]+i[11:19],
                                                                         '%Y-%m-%d%X')) for i in end_dataframe['date']]
    return end_dataframe

def callibri_refresh():

    callibri_table = gbq_pd( 'callibri_data', 'marketing_bi')
    callibri_connect = callibri()
    date1 = datetime.date(2020, 4, 1)
    date2 = datetime.date.today()
    dates = date_pairs(date1, date2)
    callibri_df  = df()

    for i in dates:
        callibri_data  = callibri_connect.get_stats(*i)
        proccesed_data = process_data(callibri_data) if callibri_data['channels_statistics'] != [] else df()
        callibri_df = callibri_df.append(proccesed_data, ignore_index= True)
        time.sleep(1)

    callibri_table = gbq_pd( 'callibri_data', 'marketing_bi')
    callibri_table.replace(callibri_df)
    return len(callibri_df)