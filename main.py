from prometheus_client import start_http_server, Gauge
from simple_salesforce import Salesforce
import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Salesforce credentials from environment variables
sf_username = os.getenv('SALESFORCE_USERNAME')
sf_password = os.getenv('SALESFORCE_PASSWORD')
sf_security_token = os.getenv('SALESFORCE_SECURITY_TOKEN')
sf_instance = os.getenv('SALESFORCE_INSTANCE_URL')
sf_domain = os.getenv('SF_DOMAIN')

sf = Salesforce(username=sf_username, password=sf_password, security_token=sf_security_token, domain=sf_domain)

# Create metrics
transactions_created_last_minute = Gauge('transactions_created_last_minute', 'Number of transactions created in the last minute')
transactions_created_last_10_minutes = Gauge('transactions_created_last_10_minutes', 'Number of transactions created in the last 10 minutes')
transactions_created_last_hour = Gauge('transactions_created_last_hour', 'Number of transactions created in the last hour')
transactions_integrated_last_10_minutes = Gauge('transactions_integrated_last_10_minutes', 'Number of transactions integrated in the last 10 minutes')
transactions_integrated_last_hour = Gauge('transactions_integrated_last_hour', 'Number of transactions integrated in the last hour')
transactions_failed = Gauge('transactions_failed', 'Number of transactions that failed to integrate')

def get_created_transactions_last_minute():
    now = datetime.datetime.utcnow()
    one_minute_ago = now - datetime.timedelta(minutes=1)
    query = f"""
        SELECT COUNT()
        FROM BR_TransacaoPagamento__c
        WHERE CreatedDate >= {one_minute_ago.isoformat()}Z
        AND CreatedDate < {now.isoformat()}Z
    """
    result = sf.query(query)
    return result['totalSize']

def get_created_transactions_last_10_minutes():
    now = datetime.datetime.utcnow()
    ten_minutes_ago = now - datetime.timedelta(minutes=10)
    query = f"""
        SELECT COUNT()
        FROM BR_TransacaoPagamento__c
        WHERE CreatedDate >= {ten_minutes_ago.isoformat()}Z
        AND CreatedDate < {now.isoformat()}Z
    """
    result = sf.query(query)
    return result['totalSize']

def get_created_transactions_last_hour():
    now = datetime.datetime.utcnow()
    one_hour_ago = now - datetime.timedelta(hours=1)
    query = f"""
        SELECT COUNT()
        FROM BR_TransacaoPagamento__c
        WHERE CreatedDate >= {one_hour_ago.isoformat()}Z
        AND CreatedDate < {now.isoformat()}Z
    """
    result = sf.query(query)
    return result['totalSize']

def get_integrated_transactions_last_10_minutes():
    now = datetime.datetime.utcnow()
    ten_minutes_ago = now - datetime.timedelta(minutes=10)
    query = f"""
        SELECT COUNT()
        FROM BR_TransacaoPagamento__c
        WHERE BR_IntegradoSAP__c = True
        AND LastModifiedDate >= {ten_minutes_ago.isoformat()}Z
        AND LastModifiedDate < {now.isoformat()}Z
    """
    result = sf.query(query)
    return result['totalSize']

def get_integrated_transactions_last_hour():
    now = datetime.datetime.utcnow()
    one_hour_ago = now - datetime.timedelta(hours=1)
    query = f"""
        SELECT COUNT()
        FROM BR_TransacaoPagamento__c
        WHERE BR_IntegradoSAP__c = True
        AND LastModifiedDate >= {one_hour_ago.isoformat()}Z
        AND LastModifiedDate < {now.isoformat()}Z
    """
    result = sf.query(query)
    return result['totalSize']

def get_failed_transactions():
    now = datetime.datetime.utcnow()
    ten_minutes_ago = now - datetime.timedelta(minutes=10)
    query = f"""
        SELECT COUNT()
        FROM BR_TransacaoPagamento__c
        WHERE BR_IntegradoSAP__c = False
        AND CreatedDate >= {ten_minutes_ago.isoformat()}Z
        AND CreatedDate < {now.isoformat()}Z
    """
    result = sf.query(query)
    return result['totalSize']

def collect_metrics():
    while True:
        transactions_created_last_minute.set(get_created_transactions_last_minute())
        transactions_created_last_10_minutes.set(get_created_transactions_last_10_minutes())
        transactions_created_last_hour.set(get_created_transactions_last_hour())
        transactions_integrated_last_10_minutes.set(get_integrated_transactions_last_10_minutes())
        transactions_integrated_last_hour.set(get_integrated_transactions_last_hour())
        transactions_failed.set(get_failed_transactions())

        time.sleep(60)

if __name__ == '__main__':
    start_http_server(8000)
    collect_metrics()
