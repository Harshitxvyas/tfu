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
  "private_key_id": "dcb1744486edadfa2762c2a20cd00bda396f5ffe",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDLBETIZ9Il/7dJ\nXgPtyLkHeXaJTYh3eeLyOlJ2dPHvCt/3f2bDGXlJM4KB2lDZu5w6KCx0d+g4T6MW\nt1+7QQQiPatRp9ITUK8xco5i1+7g6FdFecCh8pwMhsiS3Uuvn8jgd4RrL8VuWLSf\n191/LwmtW8u4oznsSj6G3F2veElSlY8mKla+mFIgAdTqwwBZVsvrmjFN9gLQo8fe\n5k/8kmVchJA/AFah/AWqPXzTrCScClaxhRpWppsjceOhFGzLt+HJzpfvSpJ0ENK+\nI86jj0I1/iA7F545wrzWF6lvJI4nNYyXnYKhjD0BWFNrBflKHbUcnAD6Xik2hoSq\nez0eagJ/AgMBAAECggEAAbfGTo7t5b7chy4Rs42BXt3SI7v0/4ztCiMD+vODfXXG\npyp8CZEJugdmVMD11fVhOgtr3qf7wdY5yWVPnNfpcO6CqH7IQ/TotpmfqphznDyz\n9MqnuvcWByypr9ORG2MAoghH+x/EBrGQy+5Zkna3GQaMYYl506pdm/iH1mnwCx5I\nVjjLAWRN9henFvMeRRtsmZ4RcPiV+3rPs4D9WuOcdEyRZeJH+VK5MRnK/St6MDd2\nd2GYkhOSk1vkoklBYjy56WAY6SVWGdJmmo2pRSoCZzklzGr0EsjeBLsnBpuP0Qxs\nsEbVj2R5zDz30CuHRXydjGcxiHMgX/excNc+0H8zsQKBgQDlBITziyuG5H4ySDVM\nIiSdrUKzpCM4frG3wiPgSeCn3cePpyU17XjUhg8CS+7HOTkc/4ZigkrXegxS3fBO\nozSsAwBo8GGYcHpqTK9OnjNxl3gCewI3gPeAy1HbGLMb5T4+X/VIzxJGVlvtJjhP\nm7pSCpJEupeKKuKWJ7xb/XbNuQKBgQDi74ZacnlZ8x0e1+d3e+pABdEhEAlV6FPL\nPDDHq0XD5ZGykvo5qXhwd0g8vXvngO7dY8v1/j/eNiG1geWka1jTP5g+8CBcbBhJ\n8gZ0+JyXeYtWhsAu4gJPkJEWTxfzVc6zZrUSrx6TSvjjTpit4VKL/kF34yUuxkMB\nf7Xbc6At9wKBgCbXVGElylUPbaPDgV6PL9yaJQToopyTSDrdL0572SE/SPhBJdt2\nkhahQexmynF5cAlOARG1/VF8PpjTUU1U+rrtq2Ug17yN3wUmSlkwFZN/V/g7uo2F\nTvTGBvT8xGvHvn2/so8Np5DMVrzqzYQa6ke9yT7k0oy5Z0KGLLkBTOPpAoGBAKah\n7mtXhxevVgCh1Ep96KGeI9M9LT2xXGXtCnxynMWdOgB/v9C7Sb57N8Wx9NsmZz6U\n2t0EFe2aneHjQbYbRJvJJIeCVqDGlocleexF6OWoz9F3HILQXZYCtyZEaXe52A8P\nKYJQqAjLkjmGKsHyo2Q8C+J2HYVc+zKMjgJXwpQ9AoGBAJkZs9DjhI6zHUAs79Mj\npZYl0Cz+UfNmhEQSgIcEfgQ6fYeCHl2PA8b+aYNDeWfwnC58IN1ZrPFcTSqEtkVg\n7SKYBmbfIP4ou+mK9m+GVNXoWtveqeoG5pfrShBstC5q1XJcMl9maOqGSQq4gDwI\nATtVXKJcokXszRlnamPxrQiq\n-----END PRIVATE KEY-----\n",
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

