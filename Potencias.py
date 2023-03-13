#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[ ]:





# In[ ]:





# In[1]:


from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import row_number, rank,asc,desc,dense_rank


# In[3]:


spark = SparkSession .builder .master('local') .appName('potencias_demo8') .getOrCreate()


# In[ ]:


df1 = spark.createDataframe([
    ("800", 'BMW', "8000")
    ("110", 'Bugatti', "8000")
    ("208", 'Peaugeot', "5400")
    ("Atlas", 'Volswagen', "5000")
    ("Mustang", 'Ford', "5000")
    ("C500", 'Mercedes', "5000")
    ("Prius", 'Toyota', "3200")
    ("Landcruiser", 'Toyota', "3000")
    ("Accord", 'Honda', "2000")
    ("C200", 'Mercedes', "2000")
    ("Corolla", 'Toyota', "1800")],
    ["name", "company", "power"]
)


# In[ ]:


rank1 =df1.withColumn("row_number", row number().over(Window.partitionBy("power")))
rank2 =df1.withColumn("dense_rank", row number().over(Window.partitionBy("power")))
rank3 =df1.withColumn("rank", row number().over(Window.partitionBy("power")))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




