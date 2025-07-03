import psycopg2
import pandas as pd
from datetime import datetime, timedelta,date
current_date = date.today()
yesterday = (current_date - timedelta(days= 0))
def execute_query(query):
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
           host = "data-analysis-db-ro-postgresql-blr1-73858-do-user-13062511-0.m.db.ondigitalocean.com",
database = "test",
user = "doadmin",
password = "AVNS_zuOg83f71JBINpat9pi",
port = "25060"
        )
        cur = conn.cursor()

        # Execute the query
        cur.execute(query)
        results = cur.fetchall()
        return results

    except (Exception, psycopg2.Error) as error:
        print("Error while executing query:", error)
        return None

    finally:
        if conn:
            if cur:
                cur.close()
            conn.close()
            print("PostgreSQL connection is closed")
            print(query)




    