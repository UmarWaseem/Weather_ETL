import schedule
import time
from etl_pipeline import run_etl

# Schedule the ETL job
schedule.every().day.at("02:00").do(run_etl)

print("⏰ Scheduler is running. Waiting for scheduled ETL runs...")

while True:
    schedule.run_pending()
    time.sleep(60)  # check every minute
