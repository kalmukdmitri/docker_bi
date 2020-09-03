import pandas
import os
import requests
import json
import datetime
import time

def get_vk(token = '', method = 'ads.getStatistics', params=''):
    user_id_V = "user_id=32785745&v=5.103"
    base_url = f"https://api.vk.com/method/{method}?{token}{user_id_V}&{params}"
    metrica_answer = requests.get(base_url)
    results = json.loads(metrica_answer.text)
    return results

def campaign_to_table(campaign_stats,campaign_dict):
    tables = [] 
    for campaign in campaign_stats['response']:
        campaigns_table = []
        campaign_id = str(campaign['id'])
        campaign_name = campaign_dict[campaign_id]  
        for row in campaign['stats']:
            spent = 0.0 if 'spent'  not in row else row['spent']
            clicks = 0.0 if 'clicks'  not in row else row['clicks']
            campaigns_table.append([campaign_id,
                                    campaign_name,
                                    row['day'],
                                    float(spent),
                                    row['impressions'],
                                    clicks,
                                    row['reach']
                                   ])
        tables.extend(campaigns_table)
    return tables


def dict_vk(vk_answer):
    dict_vk = {}
    for i in vk_answer['response']:
        url = i['link_url']

        if 'utm_source' in i['link_url']:
            tags = i['link_url'].split('?')[1].split('&')
            url = [i for i in tags if 'utm_campaign' in i][0].split('=')[1]
            domain = i['link_url'].split("/")[2]
            
        dict_vk[i['campaign_id']] = [url,domain]
    
    return dict_vk

def get_vk_ads(token = '', method = 'ads.getAdsLayout', params='account_id=1604608801'):
    user_id_V = "user_id=32785745&v=5.103"
    base_url = f"https://api.vk.com/method/{method}?{token}{user_id_V}&{params}"
    metrica_answer = requests.get(base_url)
    results = json.loads(metrica_answer.text)
    return results



def vk_refresh():

    
    from doc_token import get_tokens
    passwords = get_tokens()
    token = passwords['vk']


    campaigns = get_vk(token = token, method = 'ads.getCampaigns', params= 'account_id=1604608801')
    camps_ids = ",".join([str(i['id']) for i in campaigns['response']])
    date_to = str(datetime.date.today())
    get_campaign_stats = get_vk(token = token, params = f'ids_type=campaign&date_from=2020-04-25&date_to={date_to}&period=day&account_id=1604608801&ids={camps_ids}')
    campaigns_dict = {str(i['id']):i['name'] for i in campaigns['response']}
    time.sleep(1)
    vk_table = campaign_to_table(get_campaign_stats,campaigns_dict)
    time.sleep(1)

    columns = ['cmp_id',
            'capmaign',
            'date',
            'cost',
            'impressions',
            'clicks',
            'reach']
    vk_df = pandas.DataFrame(vk_table, columns=columns)
    time.sleep(1)
    respose = get_vk_ads(token = token)

    utms = dict_vk(respose)

    vk_df['umt_campaing'] = vk_df['cmp_id'].apply(lambda x: utms[int(x)][0])
    vk_df['project'] = vk_df['cmp_id'].apply(lambda x: utms[int(x)][1])
    vk_df['date'] = vk_df['date'].apply(lambda x : pandas.Timestamp(datetime.datetime.strptime(x,'%Y-%m-%d')))
    return vk_df
