import pd_gbq
from pd_gbq import *
import time
import requests
import json
import datetime
import pandas
import os
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials

def bi_report_refresh():
    log = ""
    
    query = """
    SELECT
      utm_source,
      utm_campaign,
      utm_term,
      DATE(date) AS date,
      COUNT(conts_id) AS all_leads,
      COUNTIF(status = 'Мусор') AS failed,
      COUNTIF(status != 'Мусор') AS normal,
      SUM(CASE
          WHEN status = 'Успешно реализовано' THEN sale
        ELSE
        0
      END
        ) AS sold,
      SUM(CASE
          WHEN status != 'Успешно реализовано' AND status != 'Мусор' AND status != 'Закрыто и не реализовано' THEN sale
        ELSE
        0
      END
        ) AS insale,
      ifnull(SUM(sum),
        0) AS sum
    FROM (
      SELECT
        conts_id AS conts_id,
        phone,
        status,
        utm_source,
        utm_campaign,
        utm_term,
        date,
        sale,
        sum
      FROM (
        SELECT
          DISTINCT (conts_id) AS conts_id,
          phone,
          status,
          utm_source,
          utm_campaign,
          utm_term,
          date,
          sale
        FROM (
          SELECT
            conts_id,
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
            MIN(ga_date) AS date,
            phone,
            ga_source AS utm_source,
            ga_campaign AS utm_campaign,
            ga_keyword AS utm_term,
          FROM (
            SELECT
              phone,
              metrika_client_id
            FROM
              `kalmuktech.marketing_bi.callibri_data` AS DATA
            WHERE
              metrika_client_id IS NOT NULL
              AND phone IS NOT NULL ) AS DATA
          JOIN
            `kalmuktech.marketing_bi.base_ga_cookie` AS ga
          ON
            DATA.metrika_client_id = ga.ga_dimension1
          GROUP BY
            2,
            3,
            4,
            5 ) AS colibr_mail
        ON
          colibr_mail.phone = lead_st.ml) AS tbl
      LEFT JOIN (
        SELECT
          SUM(otruzka_sum)/100 AS sum,
          phone AS phone_ms
        FROM
          `kalmuktech.marketing_bi.ms_sold`
        GROUP BY
          phone) AS ms_sold
      ON
        ms_sold.phone_ms = tbl.phone )
    GROUP BY
      1,
      2,
      3,
      4
    UNION ALL
    SELECT
      utm_source,
      utm_campaign,
      utm_term,
      DATE(date) AS date,
      COUNT(conts_id) AS all_leads,
      COUNTIF(status = 'Мусор') AS failed,
      COUNTIF(status != 'Мусор') AS normal,
      SUM(CASE
          WHEN status = 'Успешно реализовано' THEN sale
        ELSE
        0
      END
        ) AS sold,
      SUM(CASE
          WHEN status != 'Успешно реализовано' AND status != 'Мусор' AND status != 'Закрыто и не реализовано' THEN sale
        ELSE
        0
      END
        ) AS insale,
      0 AS sum
    FROM (
      SELECT
        DISTINCT (conts_id),
        email,
        status,
        utm_source,
        utm_campaign,
        utm_term,
        date,
        sale
      FROM (
        SELECT
          conts_id,
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
          MIN(ga_date) AS date,
          email,
          ga_source AS utm_source,
          ga_campaign AS utm_campaign,
          ga_keyword AS utm_term,
        FROM (
          SELECT
            email,
            metrika_client_id
          FROM
            `kalmuktech.marketing_bi.callibri_data` AS DATA
          WHERE
            metrika_client_id IS NOT NULL
            AND email IS NOT NULL
            AND phone IS NULL ) AS DATA
        JOIN
          `kalmuktech.marketing_bi.base_ga_cookie` AS ga
        ON
          DATA.metrika_client_id = ga.ga_dimension1
        GROUP BY
          2,
          3,
          4,
          5) AS colibr_mail
      ON
        colibr_mail.email = lead_st.ml)
    GROUP BY
      1,
      2,
      3,
      4
      
    UNION ALL
    
    SELECT
      ga_source,
      ga_campaign,
      ga_keyword,
      DATE(ga_date) AS date,
      COUNT(id) AS all_leads,
      COUNTIF(status = 'Мусор') AS failed,
      COUNTIF(status != 'Мусор') AS normal,
      SUM(CASE
          WHEN status = 'Успешно реализовано' THEN sale
        ELSE
        0
      END
        ) AS sold,
      SUM(CASE
          WHEN status != 'Успешно реализовано' AND status != 'Мусор' AND status != 'Закрыто и не реализовано' THEN sale
        ELSE
        0
      END
        ) AS insale,
      0 AS sum
    FROM
      kalmuktech.marketing_bi.base_tilda_forms AS tilda
    JOIN
      `kalmuktech.marketing_bi.base_ga_cookie` AS ga
    ON
      tilda.cookie = ga.ga_dimension1
    GROUP BY
      1,
      2,
      3,
      4

    UNION ALL

    SELECT
      ga_source,
      ga_campaign,
      ga_keyword,
      DATE(ga_date) AS date,
      COUNT(cont_id) AS all_leads,
      COUNTIF(status = 'Мусор') AS failed,
      COUNTIF(status != 'Мусор') AS normal,
      SUM(CASE
          WHEN status = 'Успешно реализовано' THEN sale
        ELSE
        0
      END
        ) AS sold,
      SUM(CASE
          WHEN status != 'Успешно реализовано' AND status != 'Мусор' AND status != 'Закрыто и не реализовано' THEN sale
        ELSE
        0
      END
        ) AS insale,
      0 AS sum
    FROM
      `kalmuktech.marketing_bi.chats_data` AS chat
    JOIN
      `kalmuktech.marketing_bi.base_ga_cookie` AS ga
    ON
      chat.ym_cookie = ga.ga_dimension1
    LEFT JOIN
      `kalmuktech.marketing_bi.base_amo_leads` AS leads
    ON
      leads.contact_id = chat.cont_id
    GROUP BY
      1,
      2,
      3,
      4
     
    UNION ALL
    SELECT
      ga_source AS utm_source,
      ga_campaign AS utm_campaign,
      ga_keyword AS utm_term,
      DATE( ga.ga_date ) AS date,
      COUNT(conts_id) AS all_leads,
      COUNTIF(status = 'Мусор') AS failed,
      COUNTIF(status != 'Мусор') AS normal,
      SUM(CASE
          WHEN status = 'Успешно реализовано' THEN sale
        ELSE
        0
      END
        ) AS sold,
      SUM(CASE
          WHEN status != 'Успешно реализовано' AND status != 'Мусор' AND status != 'Закрыто и не реализовано' THEN sale
        ELSE
        0
      END
        ) AS insale,
      0 AS sum
    FROM
      kalmuktech.marketing_bi.base_amo_contacts AS c
    JOIN
      kalmuktech.marketing_bi.base_amo_leads AS l
    ON
      l.contact_id = c.conts_id
    INNER JOIN
      `kalmuktech.marketing_bi.wf_buyers_icap` AS w
    ON
      w.phone = c.phone
    LEFT JOIN
      `kalmuktech.wf_bi.users_id` AS wf_u
    ON
      wf_u.user_id = w.user_id
    INNER JOIN
      kalmuktech.marketing_bi.shp_icap_wf_cooks AS wc
    ON
      wc.ga_dimension1 = wf_u.u_id
    JOIN
      `kalmuktech.marketing_bi.base_ga_cookie` AS ga
    ON
      wc.ga_dimension2 = ga.ga_dimension1
    GROUP BY
      1,
      2,
      3,
      4
    """

    time.sleep(2)
    callibri_table = gbq_pd('predifined_leads_data_cookie', 'marketing_bi')
    time.sleep(2)
    dates_str = callibri_table.df_query(query)

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
    callibri_table.replace(dates_str)
    
    log += f"По таблице predifined_leads_data_cookie обновилось {len(dates_str)} строк \n"

    join_table_gains_query  = """
    SELECT
    campaign_name AS camp,
    cost,
    click AS clicks,
    system.date AS date,
    'Яндекс Директ' AS systems,
    'yandex' AS utm_source,
    capmapaign AS utm_campaign,
    keyword_join AS utm_term,
    ifnull(all_leads,
        0) AS all_leads,
    ifnull(normal,
        0) AS normal,
    ifnull(insale,
        0) AS insale,
    ifnull(sold,
        0) AS sold,
            ifnull(sum,
        0) AS sum
    FROM (
    SELECT
        ym_ad_DirectOrder AS campaign_name,
        'yandex' AS source,
        Utm_campaign AS capmapaign,
        ym_ad_DirectPhraseOrCond AS keyword_spent,
        utm_term AS keyword_join,
        date,
        ym_ad_AdCost AS cost,
        ym_ad_clicks AS click
    FROM
        `kalmuktech.marketing_bi.YandexAds`) AS system
    LEFT OUTER JOIN
    `kalmuktech.marketing_bi.predifined_leads_data_cookie` AS leads
    ON
    system.source = leads.utm_source
    AND system.capmapaign = leads.utm_campaign
    AND system.date = leads.date
    AND system.keyword_join = leads.utm_term
    
    UNION ALL

    SELECT
    campaign_name AS camp,
    cost,
    click AS clicks,
    date_start AS date,
    'Facebook Ads' AS systems,
    ifnull(fb.source,
        'facebook') AS utm_source,
    capmapaign AS utm_campaign,
    'none' AS utm_term,
    ifnull(all_leads,
        0) AS all_leads,
    ifnull(normal,
        0) AS normal,
    ifnull(insale,
        0) AS insale,
    ifnull(sold,
        0) AS sold,
            ifnull(sum,
        0) AS sum
    FROM (
    SELECT
        campaign_name,
        campaign_id,
        source,
        capmapaign,
        date_start,
        SUM(lead) AS lead,
        SUM(click) AS click,
        SUM(spend) AS cost,
    FROM
        `kalmuktech.marketing_bi.FacebookAds` AS fb
    GROUP BY
        1,
        2,
        3,
        4,
        5)AS fb
    LEFT OUTER JOIN
    `kalmuktech.marketing_bi.predifined_leads_data_cookie` AS leads
    ON
    fb.source = leads.utm_source
    AND fb.capmapaign = leads.utm_campaign
    AND fb.date_start = leads.date
    
    UNION ALL
    SELECT
    ga_campaign AS camp,
    ga_adCost AS cost,
    ga_adClicks AS clicks,
    system.ga_date AS date,
    'GoogleAds' AS systems,
    'google' AS utm_source,
    ga_adwordsCampaignID AS utm_campaign,
    ga_keyword AS utm_term,
    ifnull(all_leads,
        0) AS all_leads,
    ifnull(normal,
        0) AS normal,
    ifnull(insale,
        0) AS insale,
    ifnull(sold,
        0) AS sold,
            ifnull(sum,
        0) AS sum
    FROM
    `kalmuktech.marketing_bi.GoogleAds` AS system
    LEFT OUTER JOIN
    `kalmuktech.marketing_bi.predifined_leads_data_cookie` AS leads
    ON
    leads.utm_source = 'google'
    AND system.ga_adwordsCampaignID = leads.utm_campaign
    AND system.ga_date = leads.date
    AND system.ga_keyword = leads.utm_term
    
    UNION ALL
    SELECT
    capmaign AS camp,
    cost,
    clicks AS clicks,
    vk.date AS date,
    'VK Реклама' AS systems,
    'vk' AS utm_source,
    umt_campaing AS utm_campaign,
    'none' AS utm_term,
    ifnull(all_leads,
        0) AS all_leads,
    ifnull(normal,
        0) AS normal,
    ifnull(insale,
        0) AS insale,
    ifnull(sold,
        0) AS sold,
            ifnull(sum,
        0) AS sum
    FROM
    kalmuktech.marketing_bi.VkAds AS vk
    LEFT OUTER JOIN
    `kalmuktech.marketing_bi.predifined_leads_data_cookie` AS leads
    ON
    'vk' = leads.utm_source
    AND vk.umt_campaing = leads.utm_campaign
    AND vk.date = leads.date
    
    UNION ALL  
    
    SELECT
    'Органика i-cap' AS camp,
    0,
    0 AS clicks,
    date AS date,
    utm_source AS systems,
    utm_source,
    utm_campaign,
    'none' AS utm_term,
    ifnull(all_leads,
        0) AS all_leads,
    ifnull(normal,
        0) AS normal,
    ifnull(insale,
        0) AS insale,
    ifnull(sold,
        0) AS sold,
            ifnull(sum,
        0) AS sum
    FROM
    `kalmuktech.marketing_bi.predifined_leads_data_cookie`
    where utm_campaign = '(not set)'
    
    UNION ALL
    
    select * from  `kalmuktech.old_ads.old_data_report` 
    
    UNION ALL
    
    SELECT
      'Неопределено AMO i-cap' AS camp,
      0 as cost,
      0 AS clicks,
      datetime_creat AS date,
      'Неопределено' AS systems,
      'Неопределено'AS utm_source,
      'Неопределено' AS utm_campaign,
      'none' AS utm_term,
      COUNTIF(status = 'Мусор') AS failed,
      COUNTIF(status != 'Мусор') AS normal,
      SUM(CASE
          WHEN status = 'Успешно реализовано' THEN sale
        ELSE
        0
      END
        ) AS sold,
      SUM(CASE
          WHEN status != 'Успешно реализовано' AND status != 'Мусор' AND status != 'Закрыто и не реализовано' THEN sale
        ELSE
        0
      END
        ) AS insale,
      0 AS sum
    FROM
      `kalmuktech.marketing_bi.base_amo_leads`
    WHERE
      contact_id IN (
      SELECT
        DISTINCT conts_id
      FROM
        `kalmuktech.marketing_bi.base_amo_contacts`
      WHERE
        phone NOT IN (
        SELECT
          ifnull( phone,
            '') AS phone
        FROM
          `kalmuktech.marketing_bi.callibri_data` )
        AND email NOT IN (
        SELECT
          ifnull( email,
            '') AS email
        FROM
          `kalmuktech.marketing_bi.callibri_data` a) )
    GROUP BY
      4
    """
    report_expense = gbq_pd( 'report', 'BI_Dataset')
    report_df = report_expense.df_query(join_table_gains_query)

    def get_project(columns):
        
        for i in [ 'cap', 'робк','утылк', 'bottle', 'fric', 'Органика i-cap' ]:
            if i in columns:
                return 'I-Cap'
        for i in ['wf', 'orkface', 'ворк','work', 'Аудит', 'VCRU']:
            if i in columns:
                return 'Workface'
        if 'arweg' in  columns:
            return 'Carwego'
                
            
        return 'Неопределено'

    report_df['project'] = report_df['camp'].apply(get_project)
    report_expense = gbq_pd( 'report', 'BI_Dataset')
    report_expense.replace(report_df)
    
    log += f"По таблице report обновилось {len(report_df)} строк \n"
    
    return log