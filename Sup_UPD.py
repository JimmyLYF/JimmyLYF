#!/usr/bin/env python
# coding: utf-8

# In[1]:


from databricks import sql

import pandas as pd
import numpy as np


# In[2]:


from datetime import date
from datetime import datetime
import datetime


# In[3]:


server_hostname = 'adb-1926513958772259.19.azuredatabricks.net'
http_path = '/sql/1.0/endpoints/3e54edcb9549b785'
access_token = 'dapi4b236f777f4f17f11819a725426dc097'


# In[4]:


connection = sql.connect(
    server_hostname = server_hostname,
    http_path = http_path,
    access_token = access_token)


# In[5]:


Check_date = datetime.date.today() - datetime.timedelta(days=1)
sample_date = '2021-08-28'


# In[6]:


print("Check_date is: ",Check_date)



# In[7]:


query_zsycpf = """
select 
user_profile as lanid,
clntnum as client_no,
letter_type,
hsublet as sub_letter_type,
zsldrsn as reason_cd,
datime as date_of_suppression,
datefrm as suppression_date_from,
dateto as suppression_date_to
from ops_aos_read.il_zsycpf a 
where validflag = '1' and trim(zappusr) = '' and date_trunc('DD',datime) <= '{}'
""".format(Check_date)

query_zsyppf = """
select distinct
user_profile as lanid,
chdrnum as policy_no,
b.proposal_num as aia_policy_no,
letter_type,
hsublet as sub_letter_type,
zsldrsn as reason_cd,
datime as date_of_suppression,
datefrm as suppression_date_from,
dateto as suppression_date_to
from ops_aos_read.il_zsyppf a 
left join ods_hist.adam_policy_hist b 
on a.chdrnum = b.pol_num
where ods_valid_ind = 1 and validflag = '1' and trim(zappusr) = '' and date_trunc('DD',a.datime) <= '{}' 
""".format(Check_date)


# In[8]:

print("Start query_zsycpf" )

cursor_zsycpf = connection.cursor()
cursor_zsycpf.execute(query_zsycpf)
result_zsycpf = cursor_zsycpf.fetchall()


# In[9]:


column_names_zsycpf = ['LAN ID','Client No','Letter Type','Sub-letter Type','Reason Code','Date of Suppression','Suppression Date From','Suppression Date To']
df_zscpf = pd.DataFrame(result_zsycpf, columns = column_names_zsycpf).replace(99999999,20991231).sort_values(by = ['Date of Suppression']).reset_index(drop = True)

print("Completed query_zsycpf" )

# In[10]:

print("Start query_zsyppf" )

cursor_zsyppf = connection.cursor()
cursor_zsyppf.execute(query_zsyppf)
result_zsyppf = cursor_zsyppf.fetchall()


# In[11]:


column_names_zsyppf = ['LAN ID','Policy No','AIA Policy No','Letter Type','Sub-letter Type','Reason Code','Date of Suppression','Suppression Date From','Suppression Date To']
df_zsppf = pd.DataFrame(result_zsyppf, columns = column_names_zsyppf).replace(99999999,20991231).sort_values(by = ['Date of Suppression']).reset_index(drop = True)

print("Completed query_zsycpf" )

# In[12]:


df_zsppf


# In[13]:


try:
    df_zsppf['Date of Suppression'] = df_zsppf['Date of Suppression'].dt.strftime("%Y-%m-%d")
    df_zsppf['Suppression Date From'] = pd.to_datetime(df_zsppf['Suppression Date From'], format = '%Y%m%d')
    df_zsppf['Suppression Date To'] = pd.to_datetime(df_zsppf['Suppression Date To'], format = '%Y%m%d')
except:
    if len(df_zsppf) == 0:
        print('zsppf no record')
    else:
        print('error occured')

# In[14]:


df_zsppf


# In[15]:


try:
    df_zscpf['Date of Suppression'] = df_zscpf['Date of Suppression'].dt.strftime("%Y-%m-%d")
    df_zscpf['Suppression Date From'] = pd.to_datetime(df_zscpf['Suppression Date From'], format = '%Y%m%d')
    df_zscpf['Suppression Date To'] = pd.to_datetime(df_zscpf['Suppression Date To'], format = '%Y%m%d')
except:
    if len(df_zscpf) == 0:
        print('zscpf no record')
    else:
        print('error occured')

print("Format Completed" )		

# In[16]:


print("Data is Ready")


# ## Prep Email

# In[17]:


import smtplib
import csv
from string import Template
 
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# In[18]:


from email.mime.application import MIMEApplication


# ### Detail & Body

# In[19]:


subject = "Suppression Letter Records Extract as of "+str(Check_date)
body_msg_with = """
Hi Team,

Attached list of suppression letter recorded till """+str(Check_date)+""". 

pls follow up to check and endorse accordingly. Thanks. 

Regards
Life Ops, AOS Data Analytics Team
"""

body_msg_without = """
Hi Team,

There is no suppression letter recorded. Thanks. 

Regards
Life Ops, AOS Data Analytics Team
"""
sender_email = "SGP.LO-AdvOpsSolns-DataAnalytics@aia.com"
receiver_email = """BiRong.Lim@aia.com, NurAsilah.Hisyam@aia.com, Mun-Hung.Wong@aia.com, Jojo-KL.Wong@AIA.COM, Rathyka.Rahim@aia.com, Joy-YX.Lim@AIA.COM, Chrissy-WT.Yeo@aia.com, Wallice-WK.Choo@AIA.COM, NorhaslindaKarina.AbdulLatif@aia.com, Nililia.AhmadZainudin@aia.com, Shannon-PS.Koh@aia.com, Fion-HT.Tan@AIA.COM, LiTing.Lim@aia.com, Ray-PL.Zhang@aia.com, Norzanah.Abd-Rahman@AIA.com, Sherlyn-SS.Larang@aia.com, Tutie-H.Kh-Annuar@AIA.com, ChemVoon.Lim@aia.com, Fazlida.MohamedNor@aia.com, NurHaziqah.MohdYunis@aia.com, Michelle-pl.lim@aia.com, Joraine-kj.ho@aia.com, Janyll-JY.Foo@aia.com, Ariel-HQ.Goh@aia.com"""
cc_email = """jimmy.li@aia.com, Daniel-MP.Teng@aia.com"""


# In[20]:


msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = subject
msg['cc'] = cc_email
body_with = body_msg_with
body_without = body_msg_without


# ### Attachment

# In[21]:


import io

def export_csv(df):
  with io.StringIO() as buffer:
    df.to_csv(buffer)
    return buffer.getvalue()


# In[22]:


EXPORTERS1 = {'Policy Suppression Letter.csv': export_csv}
EXPORTERS2 = {'Client Suppression Letter.csv': export_csv}

if len(df_zsppf) == 0 and len(df_zscpf) == 0:
    msg.attach(MIMEText(body_without, 'plain'))
else:
    if len(df_zsppf) > 0:
        for filename in EXPORTERS1:
            attachment1 = MIMEApplication(EXPORTERS1[filename](df_zsppf))
            attachment1['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)
            msg.attach(attachment1)
    
    if len(df_zscpf) > 0:
        for filename in EXPORTERS2:
            attachment2 = MIMEApplication(EXPORTERS2[filename](df_zscpf))
            attachment2['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)    
            msg.attach(attachment2)

    msg.attach(MIMEText(body_with, 'plain'))
    msg.attach(MIMEText(body_with, 'html'))
    
    


# In[23]:


print("Email is Ready, about to send out.")


# In[24]:


s = smtplib.SMTP(host='10.111.195.29', port=25, timeout = 120)
s.connect("10.111.195.29",25)
 
s.ehlo()
s.starttls()
s.ehlo()


# In[25]:


s.send_message(msg)
s.quit()


# In[ ]:

print("Whole Process Completed.")



# In[ ]:




