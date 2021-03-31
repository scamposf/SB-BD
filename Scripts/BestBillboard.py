#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from datetime import date


# In[2]:


sc = SparkContext()


# In[3]:


spark = SQLContext(sc)


# In[4]:


file_path = 'gs://billboard8817/descomprimido/charts.csv'


# In[5]:


music = spark.read.csv(file_path, header=True)


# In[6]:


music.show(10)


# In[8]:


music.columns


# In[10]:


music.registerTempTable("music")


# In[11]:


todo = "select * from music"


spark.sql(todo).show(10)


# In[23]:


ncpa = "select artist, count(song) as total from music group by artist order by total desc limit 5"

five = spark.sql(ncpa)



# In[25]:


five.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save('gs://billboard8817/BestFive/BestFive.csv')
    


# In[28]:


BestSong = "select count(song) as number, song as best, artist  from music group by best, artist order by number desc limit 20"

best = spark.sql(BestSong)

best.show()


# In[29]:


best.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save('gs://billboard8817/BestSong/BestSong.csv')


# In[ ]:




