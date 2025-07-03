def ads_query(start_date,end_date):
    query2 = f'''
    select distinct platform, "campaignName","adsetName","adName","adId","clicks","impressions","reach","spend","reportDate" from "AdStats"
where "reportDate" between '{start_date}' and '{end_date}'
'''
    return query2
def ads_query2(start_date,end_date):
    query2 = f'''
    select distinct platform, "campaignName","adsetName","adName","adId","clicks","impressions","reach","spend","reportDate","landingUrl" from "AdStats"
where "reportDate" between '{start_date}' and '{end_date}'
'''
    return query2
finance_keywords = ['hjst', 'kpsst', 'psst', 'cjot', 'gvlrs', 'dsaii', 'psis',
                    'bkhgt', 'jsnit', 'akiob', 'jhft', 'mbcwc', 'amct', 'hspct',
                    'asatp', 'kjamat', 'tw']



def categorize(row):
    campaign = str(row['CampaignName']).lower()
    ad_name = str(row['AdName']).lower()
    platform = str(row['Platform']).lower()

    if platform == 'taboola':
        if 'al-' in ad_name:
            return 'Spirituality'
        else:
            return 'Finance'
 
    elif any(keyword in campaign for keyword in finance_keywords) or any(keyword in ad_name for keyword in finance_keywords):
        return 'Finance'
    else:
        return 'Spirituality'

