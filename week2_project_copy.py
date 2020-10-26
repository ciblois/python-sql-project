import os
import numpy as np
import pandas as pd
import mysql.connector
import getpass

import re
from pandas.io.json import json_normalize
import requests

from sqlalchemy import create_engine
import pymysql

# write a word document for output
#from docx import Document
#from docx.shared import Inches

#estabilishing connection first

cnx = mysql.connector.connect(user = 'root', password = getpass.getpass(), host ='localhost', database = 'ironhack', auth_plugin='mysql_native_password')
cnx.is_connected()
cursor = cnx.cursor()

if cnx.is_connected():
    print("Connection open")
    # do stuff you need to the database
else:
    print("Connection is not successfully open")

"""Create a new database on MySQL"""
#query = ("""CREATE DATABASE project2""")
#cursor.execute(query)


query = ("CREATE TABLE IF NOT EXISTS "
"students ("
"id INT(64),"
"name TEXT,"
"anonymous BOOLEAN,"
"graduatingYear DOUBLE,"
"isAlumni BOOLEAN,"
"jobTitle TEXT,"
"tagline TEXT,"
"createdAt TEXT,"
"queryDate TEXT,"
"program TEXT,"
"overallScore TEXT,"
"overall TEXT,"
"curriculum TEXT,"
"jobSupport TEXT,"
"school TEXT,"
"school_id DOUBLE,"
"PRIMARY KEY (school_id))")

cursor.execute(query)

querry = ("CREATE TABLE IF NOT EXISTS "
"school_ ("
"website VARCHAR(50),"
"description TEXT,"
"LogoUrl TEXT,"
"school VARCHAR(50),"
"school_id DOUBLE,"
"PRIMARY KEY (school_id))")

cursor.execute(querry)

#querry = ("CREATE TABLE IF NOT EXISTS "
#"locations_ ("
#"id DOUBLEs,"
#"country.id DOUBLE,"
#"country.name TEXT,"
#"city.id DOUBLE"
#"city.name TEXT"
#"city.keyword VARCHAR(50)"
#"school VARCHAR(50),"
#"school_id DOUBLE,"
#"PRIMARY KEY (school_id))")

#cursor.execute(querry)

"""query = colocar aqui as lst comprehensions criadas para gerar as tabelas"""
schools = {   
'ironhack' : 10828,
'app-academy' : 10525,
'springboard' : 11035    
}

def get_comments_school(school):
    TAG_RE = re.compile(r'<[^>]+>')
    # defines url to make api call to data -> dynamic with school if you want to scrape competition
    url = "https://www.switchup.org/chimera/v1/school-review-list?mainTemplate=school-review-list&path=%2Fbootcamps%2F" + school + "&isDataTarget=false&page=3&perPage=10000&simpleHtml=true&truncationLength=250"
    #makes get request and converts answer to json
    # url defines the page of all the information, request is made, and information is returned to data variable
    data = requests.get(url).json()
    #converts json to dataframe
    reviews =  pd.DataFrame(data['content']['reviews'])
  
    #aux function to apply regex and remove tags
    def remove_tags(x):
        return TAG_RE.sub('',x)
    reviews['review_body'] = reviews['body'].apply(remove_tags)
    reviews['school'] = school
    return reviews
comments = [get_comments_school(school) for school in schools.keys()]

comments = pd.concat(comments)

from pandas.io.json import json_normalize

def get_school_info(school, school_id):
    url = 'https://www.switchup.org/chimera/v1/bootcamp-data?mainTemplate=bootcamp-data%2Fdescription&path=%2Fbootcamps%2F'+ str(school) + '&isDataTarget=false&bootcampId='+ str(school_id) + '&logoTag=logo&truncationLength=250&readMoreOmission=...&readMoreText=Read%20More&readLessText=Read%20Less'

    data = requests.get(url).json()

    data.keys()

    courses = data['content']['courses']
    courses_df = pd.DataFrame(courses, columns= ['courses'])

    locations = data['content']['locations']
    locations_df = json_normalize(locations)

    badges_df = pd.DataFrame(data['content']['meritBadges'])
    
    website = data['content']['webaddr']
    description = data['content']['description']
    logoUrl = data['content']['logoUrl']
    school_df = pd.DataFrame([website,description,logoUrl]).T
    school_df.columns =  ['website','description','LogoUrl']

    locations_df['school'] = school
    courses_df['school'] = school
    badges_df['school'] = school
    school_df['school'] = school
    

    locations_df['school_id'] = school_id
    courses_df['school_id'] = school_id
    badges_df['school_id'] = school_id
    school_df['school_id'] = school_id

    return locations_df, courses_df, badges_df, school_df

locations_list = []
courses_list = []
badges_list = []
schools_list = []

for school, id in schools.items():
    #print(school)
    a,b,c,d = get_school_info(school,id)
    
    locations_list.append(a)
    courses_list.append(b)
    badges_list.append(c)
    schools_list.append(d)

locations = pd.concat(locations_list)
courses = pd.concat(courses_list)
badges = pd.concat(badges_list)
schools = pd.concat(schools_list)

#location = pd.DataFrame(location_list)
#courses = pd.DataFrame(courses_list)
#badges = pd.DataFrame(badges_list)
#school = pd.DataFrame(schools_list)

"""Analysing data"""
stud_a = comments.drop(columns = ['hostProgramName','body','user','comments','review_body'])

stud_a.loc[stud_a['school'] == 'ironhack','school_id'] = 10828
stud_a.loc[stud_a['school'] == 'app-academy','school_id'] = 10525
stud_a.loc[stud_a['school'] == 'springboard','school_id'] = 11035


locations.isnull().sum()
"""As únicas colunas completas são: id, description, school, school_id"""
locations.describe

locations_a = locations.drop(columns=['country.id','country.name','country.abbrev','city.id','city.name','city.keyword', 'state.id', 'state.name','state.abbrev','state.keyword'])

#locations_a = locations.drop(columns=['country.abbrev','city.keyword', 'state.id', 'state.name','state.abbrev','state.keyword'])

#locations_a = locations_a.rename(columns = {'country.id': 'country_id'}, inplace = False)
#locations_a = locations_a.rename(columns = {'country.name': 'country_name'}, inplace = False)
#locations_a = locations_a.rename(columns = {'city.id': 'city_id'}, inplace = False)
#locations_a = locations_a.rename(columns = {'city.name': 'city_name'}, inplace = False)



# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="Ci860226!",
                               db="project2"))

#data without analysis
stud_a.to_sql('students', con = engine, if_exists = 'append', chunksize = 1000, index = False)
locations_a.to_sql('locations', con = engine, if_exists = 'append', chunksize = 1000, index = False)
courses.to_sql('courses_', con = engine, if_exists = 'append', chunksize = 1000, index = False)
badges.to_sql('badges_', con = engine, if_exists = 'append', chunksize = 1000, index = False)
schools.to_sql('school_', con = engine, if_exists = 'append', chunksize = 1000, index = False)