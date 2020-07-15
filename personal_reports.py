from pd_gbq import *
import requests
import json
import datetime
import os
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector as mysql
from doc_token import get_tokens


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

def refresh_personal_reports():

    token = get_tokens()
    wf_regs_table = gbq_pd('report_regs', datasetId = 'wf_bi')
    query_compaines = """
        SELECT 
        users.user_id,
        if(notif, users.email, False) as notif_mail,
        users.phone as phone, 
        from_unixtime(users.create_date) as reg_date,
        if(users.last_date> 0, from_unixtime(last_date),from_unixtime(create_date)) as last_dt, 
        if(last_date >0, '1+', 0) as visit_count,
        showcase.vitrins,
        showcase.domain,
        ifnull(reg_sourse.reg_donner, 'Прямая') as reg_donner,
        reg_sourse.reg_donner_id,
        concat("https://workface.ru/ru/company/",company_id) as company_url
    FROM `users`as users

    left join (
        SELECT 
            user_id as usr, 
            if(sum(email_messages+email_deals+email_relationships)>0, True,False) as notif
        FROM `users_notifications` 
        group by user_id
    ) as nots on nots.usr = users.user_id

    left join (
        SELECT
            user_id,
            deals.showcase_id as reg_donner_id,
            domain as reg_donner
        FROM `deals`  as deals
        inner join users on users.create_date = deals.dt_create
        join showcase_info as vitrins on deals.showcase_id = vitrins.showcase_id
    ) as reg_sourse on reg_sourse.user_id = users.user_id

    left join (
    SELECT 
    user_shw.showcase_id as showcase_id,
    user_id,
    domain,
    1 as vitrins,
        company_id
    FROM (
    SELECT  
        showcase_id,
        user_id,
        cmp_id as company_id
    FROM (
        SELECT 
            good_offer_id as showcase_id, 
            company_id as cmp_id
        FROM `good_offers`
        where is_showcase = 2) as jnt_cmp
    join `companies` as cmp on cmp.company_id = jnt_cmp.cmp_id) as user_shw
    JOIN showcase_info on showcase_info.showcase_id = user_shw.showcase_id
    ) as showcase on showcase.user_id = users.user_id
        """
    regs_query = query_df(query_compaines,token['wf_base'])
    wf_regs_table.replace(regs_query)

    wf_regs_table = gbq_pd('report_virs', datasetId = 'wf_bi')
    query_compaines = """
    SELECT 
        FROM_UNIXTIME(create_date) as date,
        user_id,
        phone,
        domain,
        ifnull(deals.sums, 0 ) as deal_sums,
        ifnull(deals.deals_cnt, 0) as deal_cnt,
        ifnull(regs.reg_cnt, 0)  as reg_cnt,
        ifnull(goods.goods_cnt, 0) as goods
    FROM (
        SELECT 
            good_offer_id as showcase_id, 
            company_id  
        FROM `good_offers`
        where is_showcase = 2
    ) as showcase

    LEFT JOIN (
        SELECT 
            showcase_id as shw_id,
            sum(total_price) as sums,
            count(distinct deal_id) as deals_cnt
        FROM `deals` 
        GROUP BY shw_id
    ) AS deals on deals.shw_id = showcase.showcase_id

    LEFT JOIN (
        SELECT
            count(distinct user_id) as reg_cnt,
            reg_srs.showcase_id as reg_donner_id
        FROM `deals`  as reg_srs
        inner join users on users.create_date = reg_srs.dt_create
        GROUP BY reg_donner_id
    ) AS regs on regs.reg_donner_id = showcase.showcase_id

    LEFT JOIN (
        SELECT 
            count(distinct good_offer_id) as goods_cnt,
            showcase_id  as v_goods_ids
        FROM `good_offers` 
        GROUP BY showcase_id
    ) as goods on goods.v_goods_ids = showcase.showcase_id

    LEFT JOIN (
        SELECT users.user_id as user_id,
            users.phone as phone,
            create_date,
            company_id as cmp 
        FROM `users`
        left join companies on users.user_id = companies.user_id
        where company_id is not null 
        ) as users on users.cmp = showcase.company_id

    LEFT JOIN (
        SELECT 
            showcase_id as sh_id, 
            domain 
        FROM `showcase_info`
        ) as showcase_info on showcase_info.sh_id = showcase.showcase_id
        """
    regs_query = query_df(query_compaines,token['wf_base'])
    wf_regs_table.replace(regs_query)
    
    q_add_visits = """
    SELECT 
    user_id,
    notif_mail,
    phone,
    reg_date,
    last_dt,
    visits as visit_count,
    vitrins,
    domain,
    reg_donner,
    reg_donner_id,
    company_url
    FROM `kalmuktech.wf_bi.report_regs` as regs
    left join (SELECT user_id as users_v, ifnull(sum(ga_sessions),0) as visits FROM `kalmuktech.wf_bi.users_id`  as u 
    left join `kalmuktech.wf_bi.user_sess`  as uses on u.u_id = uses.ga_dimension1
    group by user_id) as user_visits on user_visits.users_v = regs.user_id
    """
    res_plus_visits = wf_regs_table.df_query(q_add_visits)
    wf_regs_table.replace(res_plus_visits)

    query = """
    SELECT
    utm_source,
    utm_campaign,
    date(date) as date,
    CONCAT('https://officeicapru.amocrm.ru/leads/detail/',conts_id) AS all_leads,
    status AS failed,
    sale  as sold,
    sum
    from  (select conts_id as conts_id,
        phone,
        status,
        utm_source,
        utm_campaign,
        utm_term,
        date,
        sale,
        sum
    from 
    (SELECT
        DISTINCT (lead_id) as conts_id,
        phone,
        status,
        utm_source,
        utm_campaign,
        utm_term,
        date,
        sale
    FROM (
        SELECT
        lead_id,
        status,
        sale,
        phone AS ml
        FROM
        kalmuktech.marketing_bi.base_amo_leads AS AMO_leads
        JOIN
        kalmuktech.marketing_bi.base_amo_contacts AS cncts
        ON
        cncts.conts_id = AMO_leads.contact_id
        WHERE
        phone != 'None') AS lead_st
    JOIN (
        SELECT
    min(ga_date) as date,
    phone,
    ga_source as utm_source,
    ga_campaign as utm_campaign,
    ga_keyword as utm_term,
    FROM (
    SELECT
        phone,
        metrika_client_id
    FROM
        `kalmuktech.marketing_bi.callibri_data` AS data
    WHERE
        metrika_client_id IS NOT NULL
        AND phone IS NOT NULL ) AS data
    JOIN
    `kalmuktech.marketing_bi.base_ga_cookie` AS ga
    ON
    data.metrika_client_id = ga.ga_dimension1
    group by 
    2,3,4,5
    ) AS colibr_mail
    ON
        colibr_mail.phone = lead_st.ml) as tbl
    LEft join (SELECT sum(otruzka_sum)/100 as sum, phone as phone_ms FROM `kalmuktech.marketing_bi.ms_sold` group by phone) as ms_sold on ms_sold.phone_ms = tbl.phone
    )

    union ALL

    SELECT
    utm_source,
    utm_campaign,
    date(date) as date,
    CONCAT('https://officeicapru.amocrm.ru/leads/detail/',conts_id) AS all_leads,
    status AS failed,
    sale  as sold,
    0 as sum
    FROM (
    SELECT
        DISTINCT (lead_id) as conts_id,
        email,
        status,
        utm_source,
        utm_campaign,
        utm_term,
        date,
        sale
    FROM (
        SELECT
        lead_id,
        status,
        sale,
        email AS ml
        FROM
        kalmuktech.marketing_bi.base_amo_leads AS AMO_leads
        JOIN
        kalmuktech.marketing_bi.base_amo_contacts AS cncts
        ON
        cncts.conts_id = AMO_leads.contact_id
        WHERE
        email != 'None') AS lead_st
    JOIN (
        SELECT
    min(ga_date) as date,
    email,
    ga_source as utm_source,
    ga_campaign as utm_campaign,
    ga_keyword as utm_term,
    FROM (
    SELECT
        email,
        metrika_client_id
    FROM
        `kalmuktech.marketing_bi.callibri_data` AS data
    WHERE
        metrika_client_id IS NOT NULL
        and email IS NOT NULL
        AND phone IS NULL ) AS data
    JOIN
    `kalmuktech.marketing_bi.base_ga_cookie` AS ga
    ON
    data.metrika_client_id = ga.ga_dimension1
    group by 
    2,3,4,5) 
        AS colibr_mail
    ON
        colibr_mail.email = lead_st.ml)

    
    union ALL

    SELECT
    ga_source,
    ga_campaign,
    date(ga_date),
    CONCAT('https://officeicapru.amocrm.ru/leads/detail/',id) AS all_leads,
    status  AS status,
    sale  as sold,
    0 as sum
    FROM
    kalmuktech.marketing_bi.base_tilda_forms as tilda
    JOIN
    `kalmuktech.marketing_bi.base_ga_cookie` AS ga
    ON
    tilda.cookie = ga.ga_dimension1
    """

    Ann_report = gbq_pd('Ann_report', 'marketing_bi')
    dates_str = Ann_report.df_query(query)
    import pandas
    def transform_sourse(data):
        dict_of_dims_date = []
        for i in data.iterrows(): 
            
            
            if i[1][1] == '(not set)' and i[1][0] == 'google':
                i[1][0] = 'Google СЕО Поиск'
            elif i[1][1] == '(not set)' and 'yandex' in i[1][0]:
                i[1][0] = 'Яндекс СЕО Поиск'
            elif i[1][0] == '(direct)':
                i[1][0] ='Прямые'
            dict_of_dims_date.append(i[1])    
        
        return pandas.DataFrame(dict_of_dims_date)
    dates_str = transform_sourse(dates_str)
    Ann_report.replace(dates_str)