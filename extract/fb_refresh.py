import pandas
import os
import requests
import json
import datetime

from doc_token import get_tokens
passwords = get_tokens()
token = passwords['facebook']


def fb_add_x(cell, fld):
    ld_click = 0
    if type(cell) == list:
        for i in cell:
            if i['action_type'] == fld: 
                ld_click = int(i['value'])
    return ld_click

def utm_to_colums_full(column):
    sourse_list = []
    medium_list = []
    project_list = []
    for row in column:
        
        if row and 'utm_source' in row:
            utm = row.split('?')
            domain = row.split("/")[2]
            tag_list = [y.split('=') for y in utm[-1].split('&')]
            tags= {i[0]:i[1] for i in tag_list}
            sourse_list.append(tags['utm_source'])
            medium_list.append(tags['utm_campaign'])
            project_list.append(domain)
        else:
            sourse_list.append(None)
            medium_list.append(None)
            project_list.append(None)
            
    return (sourse_list, medium_list,project_list)

def get_match_table():
    
    time= 'time_range={"since":"2020-04-25","until":"'+str(datetime.date.today())+'"}&time_increment=1'
    fields = 'fields=campaign_name,ad_id,campaign_id,spend,actions'
    base_url= f"https://graph.facebook.com/v6.0/act_124276768426220/ads?limit=10000&fields=creative&{token}"
    results = requests.get(base_url)
    data = json.loads((results.text))
    fb_df = pandas.DataFrame(data['data'])
    fb_df['creative'] = fb_df['creative'].apply(lambda x: x['id'])
    
    report_matcher = {i.id:i.creative for i in fb_df.itertuples()}
    
    return report_matcher

def extract_url(row):
    c= 0
    end = []
    for i in row:
        c+=1
        
        if type(i) == dict:
            if 'link_data' in i:
                if 'link' in i['link_data']:
                    end.append(i['link_data']['link'])
                elif i and 'child_attachments' in i['link_data']:
                    end.append(i['link_data']['child_attachments'][0]['link'])
                else:
                    end.append(None)
            elif 'video_data' in i and 'link' in i['video_data']['call_to_action']['value']:
                end.append(i['video_data']['call_to_action']['value']['link'])
            else:
                end.append(None)
        else: 
            end.append(None)
    return end


def get_url_fb():
    
    time= 'time_range={"since":"2020-01-01","until":"'+str(datetime.date.today())+'"}&time_increment=1'
    fields = 'fields=campaign_name,ad_id,campaign_id,spend,actions'
    base_url= f"https://graph.facebook.com/v6.0/act_124276768426220/adcreatives?limit=10000&fields=object_story_spec&{token}"
    results = requests.get(base_url)
    data = json.loads((results.text))
    fb_df = pandas.DataFrame(data['data'])
    
    fb_df['object_story_spec'] = extract_url(fb_df['object_story_spec'])
    url_creative = {i.id:i.object_story_spec for i in fb_df.itertuples()}
    
    return url_creative

def refresh_fb():

    # Facebook coonect and update

    date_lst = []
    dates = datetime.date.today()

    while '2020-04-25' < str(dates):

        date2 = dates - datetime.timedelta(days = 30)

        date_lst.append([date2, dates])
        dates = date2 - datetime.timedelta(days = 1)

    date_lst = date_lst[::-1]

    fb_dfs = []
    for i in date_lst:
        time= 'time_range={"since":"'+str(i[0])+'","until":"'+str(i[1])+'"}&time_increment=1'
        fields = 'fields=campaign_name,ad_id,campaign_id,spend,actions'
        base_url= f"https://graph.facebook.com/v6.0/act_124276768426220/insights?limit=10000&{time}&{fields}&level=ad&{token}"
        results = requests.get(base_url)
        data = json.loads((results.text))

        fb_df = pandas.DataFrame(data['data'])
        if len(fb_df)==0:
            continue
        fb_df["lead"] =  fb_df['actions'].apply(fb_add_x, fld = 'lead')
        fb_df['click'] =  fb_df['actions'].apply(fb_add_x, fld = 'link_click')
        fb_df['spend'] =  fb_df['spend'].apply(float)
        fb_df = fb_df.drop(columns = 'actions')
        fb_df = fb_df.drop(columns = 'date_stop')

        urls = get_url_fb()
        report_matcher = get_match_table()
        ids = {i:urls[report_matcher[i]] if report_matcher[i] in urls else print(i) for i in report_matcher}
        urls_list = [ids[i]  if i in ids else None for i in fb_df["ad_id"]]
        fb_df['source'], fb_df['capmapaign'], fb_df['project'] = utm_to_colums_full(urls_list)

        fb_df['date_start'] = fb_df['date_start'].apply(lambda x : pandas.Timestamp(datetime.datetime.strptime(x,'%Y-%m-%d')))
        fb_dfs.append(fb_df)

    fb_dataframe_full = pandas.concat(fb_dfs, axis=0, join='outer', ignore_index=True, keys=None,
                levels=None, names=None, verify_integrity=False, copy=True)
    return fb_dataframe_full
