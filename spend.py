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


def categorize(row):
    campaign = str(row['CampaignName']).lower()
    ad_name = str(row['AdName']).lower()
    platform = str(row['Platform']).lower()

    if platform == 'taboola':
        if 'al-' in ad_name:
            return 'Spirituality'
        else:
            return 'Finance'
    elif (('_al' in campaign or '_aj' in campaign) and '_tw' not in campaign):
        return 'Spirituality'
    elif '_tw' in campaign:
        return 'Finance'
    else:
        return None

