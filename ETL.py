import json
import mysql.connector
import os
import datetime
import pandas as pd
from io import StringIO # python3; python2: BytesIO 
import boto3

def normalizeText(x):
  if isinstance(x, str):
    return x.upper()
  else:
    return x


def defaultconverter(o):
  if isinstance(o, datetime.datetime):
      return o._str_()
      
def lambda_handler(event, context):
  
    #------------------------ DATABASE CONNECTION ------------------------------------
    HOST = os.environ['HOST']
    DATABASE = os.environ['DATABASE']
    USER = os.environ['USER']
    PASSWORD = os.environ['PASSWORD']

    connection = mysql.connector.connect(
                                user=USER, 
                                password=PASSWORD,
                                host=HOST,
                                database=DATABASE)
                                
    mycursor = connection.cursor(dictionary=True)
    
    
    #------------------------ QUERY DATABASE ------------------------------------
    mycursor.execute(
    "SELECT l.leadsid, l.aspirante, l.edad, l.municipio, l.experiencia, l.escolaridad, l.candidato, l.created_at, l.sourcer, l.comentariosLead, l.fechallamada, l.buzon, v.sueldo, v.puesto, v.empresa FROM leads As l LEFT JOIN vacantes As v ON l.id_vacantes = v.id WHERE l.created_at > '2022-06-01' LIMIT 10;"
    )
    myresult = mycursor.fetchall()
    
    df = pd.DataFrame(myresult)
    
    
    #------------------------ CLEANING DATA ------------------------------------
    df = df[df["municipio"]!=""]
    df = df[df["experiencia"]!=""]
    df = df[df["escolaridad"]!=""]
    
    df["municipio"] = df["municipio"].apply(normalizeText) 
    df["escolaridad"] = df["escolaridad"].apply(normalizeText) 
    
    
    #------------------------ UPLOAD TO S3 ------------------------------------
    bucket = 'etl-bucket-data'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    current_time = datetime.datetime.now().strftime("%H:%M_%m-%d-%Y")

    s3_resource.Object(bucket, 'data/leads.csv').put(Body=csv_buffer.getvalue())
      
      
    connection.close()
    return json.dumps({
      "Result":"The operation has been successfully accomplished"
    })
