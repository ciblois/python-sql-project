#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 19:08:43 2020

@author: ironhack
"""

import mysql.connector
import getpass

psw = getpass.getpass()

        ###  JUST CHOOSE YOUR DATABASE ### 

cnx = mysql.connector.connect(user='root', password=psw, 
                              host='localhost', database='ironhack', use_pure=True) 


# see if is connected
if cnx.is_connected():
    print('You are connected to the databse')
else:
    print('Connection is not open')


# we need to define the object we will use to interact with the database
cursor = cnx.cursor()

# drop table if already exists querry 
q_drop_names =("DROP TABLE IF EXISTS names")


#let's execute the querry above in order to garatee that the table does not exists
cursor.execute(q_drop_names)

# simple querry to create a table in ironhack_personell database named 'names' with three columns (studens, age, school):

    ### ALSO CHECK IF YOUR QUERIES WERE WELL DONE ###
    
querry = ("CREATE TABLE IF NOT EXISTS "
"names ("
"student VARCHAR(50) PRIMARY KEY ,"
"age INT,"
"school VARCHAR(10))")

# this line executes the above querry to create an empty table in MySQL
cursor.execute(querry)


# saving any changes you did on the database
cnx.commit()
# clear the cursor
cursor.close()
# closes the door between python and SQL
cnx.close()
