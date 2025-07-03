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


finance_keywords = [
    'hjst', 'kpsst', 'psst', 'cjot', 'gvlrs', 'dsaii', 'psis',
    'bkhgt', 'jsnit', 'akiob', 'jhft', 'mbcwc', 'amct', 'hspct',
    'asatp', 'kjamat', 'tw'
]

def categorize(row):
    campaign = str(row.get('CampaignName', '')).lower()
    ad_name = str(row.get('AdName', '')).lower()
    platform = str(row.get('Platform', '')).lower()

    # Priority condition for Taboola platform with 'al-' in AdName
    if platform == 'taboola' and 'al-' in ad_name:
        return 'Spirituality'

    # Check if any finance keyword is in campaign or ad name
    if any(keyword in campaign or keyword in ad_name for keyword in finance_keywords):
        return 'Finance'

    return 'Spirituality'


