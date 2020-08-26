import pandas
import os
import requests
import json
import datetime
from pd_gbq import *
from base_data.google_ads_refresh import *

def wf_google_ads_refresh():
    
    
    date_start = '2020-01-01'
    Workface = ga_connect('195038144')
    Workface_df=Workface.extract_expanse(date_start)
    Workface_df = df_proc(Workface_df)
    Workface_df['Site'] = 'Workface'
    
    
    return Workface_df