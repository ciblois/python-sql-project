#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 19:43:22 2020

@author: ironhack
"""

import pandas as pd
   

   ### THE NAME AND TYPES SHOULD MATCH THE ONES IN YOUR TABLE IN SQL ###

# create a dictionary
dictionary = {'student' : ['jose', 'david', 'pedro'],
              'age' : [27,30,27],
              'school' : ['iron', 'hack', 'cool']}

#From the dictionary create an Dataframe (TABLE)
df = pd.DataFrame(dictionary)

#CHECK THE TYPES# 
# WE HAVE VARCHAR for students column SO IT SHOULD BE A OBJECT/STRING IN PYTHON 
# WE HAVE INT for age column SO IT SHOULD BE A INT IN PYTHON
# WE HAVE VARCHAR for school column SO IT SHOULD BE A OBJECT/STRING IN PYTHON

print(df.dtypes)

# With SqL alchemy we populate with the hole table 
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="xxxx",
                               db="Ironhack"))


df.to_sql('names', con = engine, if_exists = "append", index=False)



# NOTE:the dataframe don't need to have all the cvolumns that your table has in SQL. Just the right names!
