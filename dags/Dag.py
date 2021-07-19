#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
import requests
import json
import pandas as pd
import smtplib,ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urlparse
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def sendmail():
        requestUrl = "https://api.nytimes.com/svc/books/v3/lists.json?list=combined-print-and-e-book-fiction&api-key=K8AvTI6UaukztoAULxEc6exUGtlEWnQo"
        requestHeaders = {
        "Accept": "application/json"
        }
       
        request_fiction = requests.get(requestUrl, headers=requestHeaders).text
       
        print(request_fiction)
        data_fiction=json.loads(request_fiction)
        data_fiction_final = data_fiction["results"]
        titles_fiction = []
        for i in data_fiction_final:
            details = i['book_details']
            for j in details:
                titles_fiction.append(j['title'])
        requestUrl = "https://api.nytimes.com/svc/books/v3/lists.json?list=combined-print-and-e-book-nonfiction&api-key=K8AvTI6UaukztoAULxEc6exUGtlEWnQo"
        requestHeaders = {
        "Accept": "application/json"
        }
        request_nonfiction = requests.get(requestUrl, headers=requestHeaders).text
        data_nonfiction=json.loads(request_nonfiction)
        data_nonfiction_final=data_nonfiction['results']
        titles_nonfiction = []
        for i in data_nonfiction_final:
            details_nonfiction = i['book_details']
            for j in details_nonfiction:
                titles_nonfiction.append(j['title'])
        titles_nonfiction = '\n'.join([str(elem) for elem in titles_nonfiction])
        titles_fiction = '\n'.join([str(elem) for elem in titles_fiction])
        port = 587
        smtp_server="smtp-mail.outlook.com"
        sender_email="Book.List1@outlook.com"
        password="TESTpassword"
        message=MIMEMultipart("alternative")
        message['Subject']="List of Latest Fictional and Non Fictional Books"
        message["From"]=sender_email
        receiver_email="manojkthanneru@gmail.com"
        message['To']=receiver_email
        text="Hi,\nPlease find the list of lastest Fiction and Non fiction books below\nFiction books are:\n" + titles_nonfiction + "\n\nNon Fiction books are:\n" + titles_fiction
       
       
        part=MIMEText(text,'plain')
       
        message.attach(part)
       
        context=ssl.create_default_context()
        with smtplib.SMTP(smtp_server,port) as server:
            server.starttls(context=context)
            server.login(sender_email,password)
            server.sendmail(sender_email,receiver_email,message.as_string())           
default_args ={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2021,7,18),
    'retries':0
}

dag=DAG(dag_id='Dag',default_args=default_args,catchup=False, schedule_interval='@daily')


PythonOperator(dag=dag,
               task_id='Fetching_List_of_Books',
               python_callable=sendmail,
               op_args=[],
               op_kwargs={})

