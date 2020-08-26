import pandas
import os
import requests
import json
import datetime
from pd_gbq import *
from base_data.direct_refresh import direct_acc, dedouble_dict_table, get_match_table, match_dict,add_utm_to_df_yandex

def wf_y_direct_refresh():
    from doc_token import get_tokens
    token = get_tokens()

    dimestions = "ym:ad:<attribution>DirectOrder,ym:ad:<attribution>DirectPhraseOrCond"
    metrics = "ym:ad:<currency>AdCost,ym:ad:clicks"

    try:
        bq_yandex=gbq_pd( 'wf_yandex', 'marketing_bi')
        q_last_date = "SELECT date(max(date)) as last_date FROM `kalmuktech.marketing_bi.wf_yandex`"
        last_date = bq_yandex.df_query(q_last_date).iloc[0,0].date()
        date = str(last_date)[:10]
    except:
        date = '2020-04-10'


    header_str = dimestions+","+metrics
    header_str = header_str.replace(":","_")
    header_str = header_str.replace("<attribution>","")
    header_str = header_str.replace("<currency>","")
    column_headers = header_str.split(",") + ['date']


    wf = token['yandex']['wf']
    wf_dirc = direct_acc(**wf)
    wf_list = wf_dirc.get_lists(date, dimestions,metrics)
    direct_table_full = pandas.DataFrame(wf_list, columns = column_headers)

    if wf_list == []:
        return []

    direct_table_full['date'] = direct_table_full['date'].apply(pandas.Timestamp)

    cross_data2 = get_match_table(ym_class = wf_dirc)
    utm_data = match_dict(cross_data2)

    final_table_yandex =add_utm_to_df_yandex(direct_table_full,utm_data)

    return final_table_yandex