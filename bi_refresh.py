from pd_gbq import *
from base_data.fb_refresh import *
from base_data.vk_refresh import *
from base_data.google_ads_refresh import *
from base_data.direct_refresh import *
from base_data.callibri import *
from base_data.GA_cookie import *
from base_data.amo_tilda_reshresh import *
from refresh_reports import *
from personal_reports import *

try:
    y_direct_refresh()
    print('Yandex success')
except:
    print('Yandex Fail')

try:
    google_table = gbq_pd( 'GoogleAds', 'marketing_bi')
    ga_ads = ga_refresh()
    google_table.replace(ga_ads)
    print('Google success')
except:
    print('Google Ads Fail')

try:
    vk_campaing_cost = gbq_pd( 'VkAds', 'marketing_bi')
    vk_ads = vk_refresh()
    vk_campaing_cost.replace(vk_ads)
    print('VK success')
except:
    print('Vk Fail')

try:
    fb_table = gbq_pd( 'FacebookAds', 'marketing_bi')
    fb_ads = refresh_fb()
    fb_table.replace(fb_ads)
    print('FB success')
except:
    print('FB Fail')

try:
    cooks_df = ga_cookie_refresh()
    print(len(cooks_df))
    print('Cookies success')
except:
    print('cookies Fail')
    
try:
    lnth_clb = callibri_refresh()
    print('Callibri success')
except:
    print('callibri Fail')
    
try:
    Amo_refresh()
    print('Amo success')
except:
    print('Amo Fail')
    
try:
    bi_report_refresh()
    print('BI success and cool')
except:
    print('BI Fail')
    
    
try:
    refresh_personal_reports()
    print('personal success')
except:
    print('personal fail')