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
import mysql.connector as mysql

def wf_amo_refresh():
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
        url = 'https://workface.amocrm.ru/oauth2/access_token'
        data = json.dumps({
        "client_id": "f3c9786b-3c1b-4d89-92de-9a5fca1b1b09",
        "client_secret": "IfPcbgAecKWDgh91YpmnAWmhgW2oXv49bqhbolb9OTuNl2ew30LrrzfSu94f77iw",
        "grant_type": "refresh_token",
        'redirect_uri':"https://workface.amocrm.ru/",
        "refresh_token": old_token['refresh_token']
                        })


        token = json.loads(requests.post(url, headers = {"Content-Type":"application/json"},data=data).text)
        write_new_token(docname,token,doc_lenth)

        return token

    class get_AMO:
        m_url = "https://workface.amocrm.ru/api/v2/"
        def __init__(self, token):
            self.headers = {
              'Authorization': f"Bearer {token}",
              "Content-Type":"application/json"
            }
        def get_data(self, prm):
            url = f"{get_AMO.m_url}{prm}"
            reqv = requests.get(url, headers = self.headers)   
            print(reqv)
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
                response = self.get_data(params_url)
                if '_embedded' not in response:
                    break
                result = response['_embedded']['items']
                res.extend(result)
                len_res= len(result)
                print(len_res)
                if c == 100 or len_res < 500: 
                    i = False
            return res

    current_token = get_new_token("1gq6h7JRgx4y29tSi6omP4TNpqoAnLHux4nBEbKY4CCk")
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

    leads = amo_connect.get_big_amo("leads")

    leads_processed = []
    [leads_processed.extend(parse_amo_leads(i,pips)) for i in leads]

    leads_processed_df = pd.DataFrame(leads_processed,
                               columns = ['lead_id','lead_name','datetime_creat','sale','pipeline_name',
                                          'status', 'company_id' , 'contact_id'])
    leads_processed_gbq = gbq_pd('AMO_leads', datasetId = 'wf_bi')
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
    unsort_leads_processed_gbq = gbq_pd('AMO_unsort', datasetId = 'wf_bi')
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

        if 78685 in custom_fields:
            email = [i['value'] for i in custom_fields[78685]]
        if 78683 in custom_fields:
            phone = [number_fix(i['value']) for i in custom_fields[78683]] 

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
    AMO_contacts_gbq = gbq_pd('AMO_contacts', datasetId = 'wf_bi')
    AMO_contacts_gbq.replace(contacts_df)




    def universal_requestor(db_connect,query):
        cnx = mysql.connect(**db_connect)
        cursor = cnx.cursor()
        cursor.execute(query)
        resula = [i for i in cursor]
        cursor.close()
        cnx.close()
        return resula

    def query_df(qry):
        devDB  = {
            'host' :"185.180.163.10",
            'user' : "dima_statist",
            'passwd' : "YandexGoogle",
            'database' : "workface.ru"
        }

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


    wf_compaies = gbq_pd('wf_compaies', datasetId = 'wf_bi')
    query_compaines = 'SELECT company_id, user_id, about, name, is_paid FROM `companies`'
    companies = query_df(query_compaines)
    wf_compaies.replace(companies)

    wf_deals = gbq_pd('wf_deals', datasetId = 'wf_bi')
    query_deals = '''SELECT showcase_id , phone, email, domain,deals ,oborot  FROM `showcase_info` as info
    left join (SELECT  showcase_id as swd, count(distinct deal_id) as deals, sum(total_price) as oborot FROM `deals` group by 1) s on s.swd = info.showcase_id'''
    deals = query_df(query_deals)
    wf_deals.replace(deals)

    wf_users = gbq_pd('wf_users', datasetId = 'wf_bi')
    query_users = """
    SELECT users.user_id as user_id,
    users.phone as phone,
    users.email as email,
    FROM_UNIXTIME( create_date ) AS date,
    company_id FROM `users` as users
    left join `companies` on users.user_id = companies.user_id """
    users = query_df(query_users)
    wf_users.replace(users)

    join_table_gains_query= """
    with amo as (
    SELECT email, phone, CONCAT('https://workface.amocrm.ru/contacts/detail/',conts_id) as url,
    created_at, pipeline_name, status 
    FROM `kalmuktech.wf_bi.AMO_contacts` as cnt
    join `kalmuktech.wf_bi.AMO_leads` as lds on lds.contact_id = cnt.conts_id),

     db as (
    SELECT users.phone as u_phone, deals, oborot, 
    users.email as u_email, date, if(about>'',1,0) as cmp_info, 
    name, ifnull(is_paid, 0) as paid FROM `kalmuktech.wf_bi.wf_users` as users
    left join `kalmuktech.wf_bi.wf_compaies` as cmp on cmp.user_id = users.user_id
    left join `kalmuktech.wf_bi.wf_deals`  as deals on deals.phone = users.phone)


    select * from amo
    full join db on db.u_phone = amo.phone
    """
    report = gbq_pd('WF_db', datasetId = 'wf_bi')
    report_df = report.df_query(join_table_gains_query)
    report.replace(report_df)


    second_report = gbq_pd('ann_report', datasetId= 'wf_bi')
    query_ann = """
    SELECT
  CONCAT("https://workface.amocrm.ru/leads/detail/",lead_id) AS lead_url,
  CONCAT("https://workface.amocrm.ru/contacts/detail/",c) AS contact_url,
  amo_date,
  amo_phone,
  reg_date,
  registred,
  case
    when user_url > 0 then CONCAT("https://workface.ru/ru/company/",user_url)
  else
    '0'
  end as user_url
FROM (
  SELECT
    c,
    created_at AS amo_date,
    phn AS amo_phone,
    date AS reg_date,
  IF
    ( user_id IS NOT NULL,
      1,
      0 ) AS registred,
  IF
    ( company_id IS NOT NULL,
     company_id,
      0 ) AS user_url
  FROM (
    SELECT
      conts_id AS c,
      created_at,
      email AS a_email,
      phone AS phn
    FROM
      `kalmuktech.wf_bi.AMO_contacts`
    WHERE
      phone != 'None') AS amo_c
  LEFT JOIN
    `kalmuktech.wf_bi.wf_users` AS users
  ON
    users.phone = amo_c.phn) AS contacts
LEFT JOIN
  `kalmuktech.wf_bi.AMO_leads` AS AMO_leads
ON
  AMO_leads.contact_id = contacts.c
ORDER BY
  registred DESC
    """
    ann_report = second_report.df_query(query_ann)
    second_report.replace(ann_report)