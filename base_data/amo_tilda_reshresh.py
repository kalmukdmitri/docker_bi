import pd_gbq
from pd_gbq import *
import time
import requests
import json
import datetime
import pandas
import os
from google.cloud import bigquery
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

def Amo_refresh():
    SCOPES = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/documents']

    credentials = ServiceAccountCredentials.from_json_keyfile_name('kalmuktech-5b35a5c2c8ec.json', SCOPES)
    service = build('docs', 'v1', credentials=credentials)



    def get_old_token(docname):
        """Intup: None
        Output: Old token"""
        
        googe_request = service.documents().get(documentId = docname).execute()
        token_str=googe_request['body']['content'][1]['paragraph']['elements'][0]['textRun']['content']
        doc_lenth = len(token_str)
        token = json.loads(token_str.strip().replace("'", '"'))
        return token,doc_lenth

    def write_new_token(docname, token, doc_lenth):
        requests = [
            {'deleteContentRange': {
                'range' : {
                    "startIndex": 1,
        "endIndex": doc_lenth
                }
            }},
            {'insertText': {
                    'location': {
                        'index': 1,
                    },
                    'text': str(token)
                }
            }
        ]
        result = service.documents().batchUpdate(
            documentId=docname, body={'requests': requests}).execute()
            

    def get_new_token(docname):
        """Intup: None
        Process: write new token instead of old one
        Output: New token """
        
        old_token,doc_lenth = get_old_token(docname)
        url = 'https://officeicapru.amocrm.ru/oauth2/access_token'
        data = json.dumps({
        "client_id": "890911e6-74ae-4569-b79b-c0a054f0068d",
        "client_secret": "o7KnfRWwB5VxBM3c0gU49MD9qs7btt1CNiNZKCjPii3mx9wQHw8orygepLmgmNdu",
        "grant_type": "refresh_token",
        'redirect_uri':"https://officeicapru.amocrm.ru/",
        "refresh_token": old_token['refresh_token']
                        })
        
        
        token = json.loads(requests.post(url, headers = {"Content-Type":"application/json"},data=data).text)
        write_new_token(docname,token,doc_lenth)
        
        return token


    class get_AMO:
        m_url = "https://officeicapru.amocrm.ru/api/v2/"
        def __init__(self, token):
            self.headers = {
            'Authorization': f"Bearer {token}",
            "Content-Type":"application/json"
            }
        def get_data(self, prm):
            url = f"{get_AMO.m_url}{prm}"
            reqv = requests.get(url, headers = self.headers)       
            return json.loads(reqv.text)
        
        def get_big_amo(self,params):
            i = True
            c = -1
            res = []
            while i:
                c+=1
                offset = c * 500
                print(f'{params}?limit_rows=500&limit_offset={offset}')
                params_url = f'{params}?limit_rows=500&limit_offset={offset}'
                result = self.get_data(params_url)['_embedded']['items']
                res.extend(result)
                len_res= len(result)
                print(len_res)
                if c == 100 or len_res < 500: 
                    i = False
            return res
        
    current_token = get_new_token("1o0--ETQ4EqyoqkX8uM5Xik9CIL-MRiGInR6LCPCa-As")
    amo_connect = get_AMO(current_token['access_token'])
    dicts_amo = amo_connect.get_data("account?with=pipelines,custom_fields,users")

    def pipiline_loc(pipelines):
        pipeline_dicts = {}
        for i in pipelines:

            pipeline_dicts[i] = [pipelines[i]['name']]
            statuse = {}

            for j in pipelines[i]['statuses']:
                statuse[j] = pipelines[i]['statuses'][j]['name']

            pipeline_dicts[i].append(statuse)
        return pipeline_dicts

    pips = pipiline_loc(dicts_amo['_embedded']['pipelines'])

    def tilda_form_process(row_lead,pipeline):
        lead  = None
        if 'tags' in row_lead:
            tags = {i['id']:i['name'] for i in row_lead['tags']}
            if 450639 in tags:
                contacts = []
                if 'id' in row_lead['contacts']:
                    contacts = [int(i) for i in row_lead['contacts']['id']]
                custom_values = {i['id']:i['values'][0]['value'] for i in row_lead['custom_fields']}
                utm_sourse = custom_values[86255] if 86255 in custom_values else None
                utm_campaign = custom_values[677919] if 677919 in custom_values else None
                utm_keyword = custom_values[677921] if 677921 in custom_values else None
                cookies = custom_values[679173] if 679173 in custom_values else None
                ym_cookie = None
                if cookies:
                    cookie_dict = {i.split('=')[0]:i.split('=')[1] for i in cookies.split('; ')}
                    ym_cookie = cookie_dict['_ym_uid'] if '_ym_uid' in cookie_dict else None
                date = datetime.date.fromtimestamp(row_lead['created_at'])
                pipeline_name = pipeline[str(row_lead['pipeline_id'])][0]
                status = pipeline[str(row_lead['pipeline_id'])][1][str(row_lead['status_id'])] 
                sale = row_lead['sale']
                lead = [
                    ym_cookie,
                    row_lead['id'],
                    utm_sourse,
                    utm_campaign,
                    utm_keyword,
                    date,
                    sale,
                    pipeline_name,
                    status,
                    contacts
                ]
        return lead

    def parse_amo_leads(lead, pipeline):
        leads = []
        pipeline_name = pipeline[str(lead['pipeline_id'])][0]
        status = pipeline[str(lead['pipeline_id'])][1][str(lead['status_id'])]
        company = lead['company']['id'] if 'id' in lead['company'] else 'None'
        lead_data = [
            lead['id'],
            lead['name'],
            datetime.datetime.fromtimestamp(lead['created_at']),
            lead['sale'],
            pipeline_name,
            status,
            company
        ]
        contacts = [None]
        if 'id' in lead['contacts']:
            contacts = [int(i) for i in lead['contacts']['id']]
            
        for i in contacts:
            leads.append(lead_data+[i])

        return leads

    pips = pipiline_loc(dicts_amo['_embedded']['pipelines'])

    leads = amo_connect.get_big_amo("leads")
    forms = []
    for i in leads:
        tilda_lead = tilda_form_process(i,pips)
        if tilda_lead:
            forms.append(tilda_lead)

    forms_df = pd.DataFrame(forms,
                            columns = ['cookie', 'id','utm_sourse','utm_campaign','utm_keyword',
                                        'date','sale','pipeline_name','status', 'contacts'])
    forms_df['date'] = forms_df['date'].apply(pandas.Timestamp)

    forms_gbq = gbq_pd('base_tilda_forms', datasetId = 'marketing_bi')

    time.sleep(10)
    forms_gbq.replace(forms_df)

    leads_processed = []
    [leads_processed.extend(parse_amo_leads(i,pips)) for i in leads]

    leads_processed_gbq = gbq_pd('base_amo_leads', datasetId = 'marketing_bi')
    leads_processed_df = pd.DataFrame(leads_processed,
                            columns = ['lead_id','lead_name','datetime_creat','sale','pipeline_name',
                                        'status', 'company_id' , 'contact_id'])

    leads_processed_gbq.replace(leads_processed_df)
    def proccess_unsort(unsl, pipeline):
        
        uns_lead_date =  datetime.datetime.fromtimestamp(unsl['created_at']),
        
        uns_lead = unsl['incoming_entities']['leads'][0]
        uns_contact = unsl['incoming_entities']['contacts'][0] if 'contacts' in unsl['incoming_entities'] else {}
        
        uns_lead_id = uns_lead['id']
        uns_lead_name = uns_lead['name']
        
        pipeline_name = pipeline[str(uns_lead['pipeline_id'])][0]
        status = pipeline[str(uns_lead['pipeline_id'])][1][str(uns_lead['status_id'])]
        
        uns_contact_name = uns_contact['name'] if 'name' in uns_contact else 'NoName'
        uns_contact_id = uns_contact['id'] if 'id' in uns_contact else 'NoId'
        
        email = unsl['incoming_lead_info']['email'] if 'email' in unsl['incoming_lead_info'] else "NoMail"
        
        custom_fields= {}
        
        if 'custom_fields' in uns_contact:
            for i in uns_contact['custom_fields']:
                values = next(iter(uns_contact['custom_fields'][i]['values'].values()))['value']
                custom_fields[i] = values

        
        phone= custom_fields['50597'] if '50597' in custom_fields else 'Nophone'
        
        dtkt_sourse = custom_fields['668903'] if '668903' in custom_fields else 'Nophone'
        dtkt_medium = custom_fields['668905'] if '668905' in custom_fields else 'Nomedium'
        dtkt_campaign = custom_fields['668907'] if '668907' in custom_fields else 'Nocampaign'
        dtkt_key = custom_fields['668911'] if '668911' in custom_fields else 'Nokey'
        
        return [
            
            uns_lead_id,
            uns_lead_name,
            uns_lead_date,
            pipeline_name,
            status,
            uns_contact_name,
            uns_contact_id,
            phone,
            dtkt_sourse,
            dtkt_medium,
            dtkt_campaign,
            dtkt_key,
            email
        ]
        
    unsort=amo_connect.get_big_amo("incoming_leads")
    unsort_leads_processed = [(proccess_unsort(i,pips)) for i in unsort]
    unsort_leads_processed_df = pd.DataFrame(unsort_leads_processed,
                            columns = ['uns_lead_id','uns_lead_name','uns_lead_date',
                                        'pipeline_name','status','uns_contact_name','uns_contact_id',
                                        'phone','dtkt_sourse','dtkt_medium','dtkt_campaign',
                                        'dtkt_key','email'])
    unsort_leads_processed_gbq = gbq_pd('base_amo_unsort', datasetId = 'marketing_bi')
    unsort_leads_processed_gbq.add(unsort_leads_processed_df, if_exists = 'replace')

    def number_fix(number):
        number  = "".join([i for i in number if i.isnumeric()])
        if len(number) < 9 or len(number) > 14 or number[0] == '0':
            return 'None'
        if number[0] == '8' or number[0] == '9':
            number = '7'+number[1:]



        return number
    def parse_amo_contacts(conts):
        contacts = []
        
        company_id = conts['company']['id'] if 'id' in conts['company'] else 'None'
        company_name = conts['company']['name'] if 'name' in conts['company'] else 'None'
        
        contact_data = [
            conts['id'],
            conts['name'],
            datetime.datetime.fromtimestamp(conts['updated_at']),
            datetime.datetime.fromtimestamp(conts['created_at']),
            company_id,
            company_name
        ]
        
        email = ['None']
        
        phone = ['None']
        
        
        custom_fields = {i['id']: i['values'] for i in conts['custom_fields']}
        
        if 50599 in custom_fields:
            email = [i['value'] for i in custom_fields[50599]]
        if 50597 in custom_fields:
            phone = [number_fix(i['value']) for i in custom_fields[50597]] 
        
        if email == ['None'] and phone == ['None']:
            return []
            
        for i in email:
            for j in phone:
                contacts.append(contact_data+[i,j])

        return contacts

    contacts = amo_connect.get_big_amo("contacts")
    all_contacts = []

    for i in contacts:
        all_contacts.extend(parse_amo_contacts(i))
        
    contacts_df = pd.DataFrame(all_contacts,
                            columns = ['conts_id',
                                        'conts_name',
                                        'datetime_creat',
                                        'created_at',
                                        'company_id',
                                        'company_name',
                                        'email',
                                        'phone'])
    AMO_contacts_gbq = gbq_pd('base_amo_contacts', datasetId = 'marketing_bi')
    AMO_contacts_gbq.replace(contacts_df)