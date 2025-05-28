import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials


def classes(report_date):
    query2 = f'''
    SELECT DISTINCT l."masterclassSlotId",mc."title",
FIRST_VALUE(l."createdAt") OVER (PARTITION BY l."masterclassSlotId" ORDER BY l."createdAt" ASC) AS "First lead",
LAST_VALUE(l."createdAt") OVER (PARTITION BY l."masterclassSlotId" ORDER BY l."createdAt"  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "last lead"
FROM "Leads" l
JOIN "MasterClassSlots" mcs ON l."masterclassSlotId"=mcs."id"
JOIN "MasterClass" mc ON mc."id"=mcs."masterClassId"
JOIN "Bootcamp" b ON b."id"=mc."bootcampId"
WHERE DATE(mcs."startDateTime")='{report_date}'
and source not in ('ret', 'arvind.tech', 'act*', 'retdm', 'null', 'cal','calendar', 'dm', 'email', 'push-notification', 'sms', 'freshdm', 'retp', 'api-ops', 'act', 'Zoom Reschedule', 'Livekit_Reschedule','default_source','gifff','Instagram','LMS','TradeWise')

'''
    return query2
def google_cred():
    json_key = {
    "type": "service_account",
    "project_id": "dailyclassspend",
    "private_key_id": "ddf20259a503e370a10a1d15faa23176de46e4ed",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC4Q+fi4MP6LM5V\nNvhi5IBTxLZ9MfZAO6l815ZULnJXZeP9N5nGKPdeftGWS5HpxLd5lgP94fL7CRAy\nStlYD+oY8my8zdxnc46rl0Jv3p6DrZCMsSoieqnbqqbul7IwiZZG1a4IiylA0BzM\nwV5T4buTinAErkmPLe34k3DmrZ1iHm2UyDjKvpDxfGqF+G8ck5PLibFrkbZHVvVz\nkVdAhb19UMdyZwGeIRA9QE2DLBQ2j+MdpW5Tpc0L1zYgAcQk4FiHgpf+UnK0+1Y7\nbnYarBQmRg+d7tq1FwWRTPUJI1Z/RqH1NEXVpvCrU/iSw4vO8EcMa4KGOF1iN+zN\nIRfp5yOLAgMBAAECggEABiyBnqiWQxHGQLUB9y+dxxtpBo0qfuDLidB1YSZZ6Wi+\nDf6+FcAgowIPxqqLMzBxT08XR3s5whnTI5mTalpvfFzUXN0ZkekdciK0H1MaWjdy\nB5drXdK1/JNdWtLVWGqmZi2EUmqgkWMyW9VCHQBhYbelOjBBbF0xwymO16510/2P\nSm9Blt+ntPOyDy9nH7fhIP2D9V8caSuLFn7aRLRJp5+5SBiTq6tk4K3L0oYLxbU1\nYKPp72RstXsaY1YIYZ87RWl/nxFQ59ckDKkg+K9xrNmQ11/2vp+NSn7k+0po3yxX\nkXBhgWRH9Zw23o5ygS66wgY9cvFkjCsKbaBYJcIp+QKBgQD76Alr6NnjZS1ysi8X\nPqJfldomuvHgVJC+XWHi7pHsn7L0llKwOyydjA0MIc7cK1OYUF/bhCHCv5Zc689E\nbbqs/WBnOk9RiXX5NIi6YHwxCDsQzBx7ONfP+0RqzPqlcOuzu+KpETTFaREWoWeG\nyH66ROKmVp5DZmNXNNoFYLpLgwKBgQC7QnkecAApvKTjtgDrAHhhyjx/kb2l/9ZL\nDpNRKc1NGMImlpyN+1K9ahA9taGUpACSUxifExfCpfewBI/0b/CUDO3mAgXkUGod\nahcIex/ubj6PzJ4cG9r06j52grmCaaKkBiTooc5xiHa07dgsyOYGU/X3gPAkVUx1\n9ieVBU0hWQKBgF9CVOXArTzHkxMvfI1OuzpoQZGp2jPZ5s6GI7EcRIR1s8e4XNaV\n2f5N6tUup7osjDUqF8W8RsQNjT/gUIXw9MncGyuOKlaMI9e0XQwV1oD4OtXUSeTz\nDvFwdGPq1dHgTNGv+Du07P7GB5dGPA8FHJ8103vRf5G/U0u0CcpE1M4fAoGAatW7\n4D+p+CXmNPwLfgegproI1gdCGcjia6P/LFkbBhdP/VAENYTjaalvZqWPcAw5P0Il\nU+xCrAygSU3xC1Yp/W169LTGw8GVKP7z8SS9zPwcuVHWCtdhjkVuKZLfX6YA4m+W\n8xoNe/yNLSBdKv/bLFUOiYyIdVIs80fiwURrAtECgYBByHiLW1MSe9TsfriBiQ/f\n9/EUW+KP0zyqFGg4rktmy9gP9v4Wvzy0eiqDUY2r/zzv4Uj1uOSI8QIGcb9qQ2a4\nxrNbi8YYrH4K/Px2iong8/ITJ3vWfrjPKpm0oIZcchyzm4qwr5bQkT8eIMuYB9/z\nCY9VGsKgQ8aX9m0vByBNjw==\n-----END PRIVATE KEY-----\n",
    "client_email": "class-spend@dailyclassspend.iam.gserviceaccount.com",
    "client_id": "102229672555957037003",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/class-spend%40dailyclassspend.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    }

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_key, scope)
    client = gspread.authorize(credentials)
    return client

