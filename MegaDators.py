import sys
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt

from pyspark.sql.functions import mean, min, max
import numpy as np


data = {}


orders = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("instacart_2017_05_01/orders.csv")

products = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("instacart_2017_05_01/products.csv")
  
aisles = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("instacart_2017_05_01/aisles.csv")

departments = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("instacart_2017_05_01/departments.csv")
  
order_products__train = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("instacart_2017_05_01/order_products__train.csv")

order_products__prior = sqlContext.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("instacart_2017_05_01/order_products__prior.csv")


#TASK 1.a
print("# of columns = ", len(orders.columns))

#number of orders
print("number of orders = ", orders.count())

#number of orders
print("number of products = ", products.count())

print(orders)
#>>> df = df.dropDuplicates()
users = orders.select("user_id").dropDuplicates()
print("number of users =  ", users.count())

#206209 diff users in the datasets 
#order_products__train.select("order_id").filter(order_products__train.order_id == "1").show()
#order_products__prior.select("order_id").filter(order_products__prior.order_id == "1").show()

#TASK 1.b
#order_products__prior.show()

order_products = order_products__prior.union(order_products__train)

#print("number of columns of order_products =  ", order_products.count())

order_products = order_products.select("order_id","product_id")
print("number of orders =  ", order_products.select("order_id").dropDuplicates().count())

#num_products=order_products.groupby('order_id').count()
#num_products.show()

#order_products.select("order_id","product id")
#print("number of orders =  ", order_products.select("order_id").dropDuplicates().count())

num_products=order_products.groupby('order_id').count()
num_products.show()
x = num_products.select("order_id").take(2000)
x = np.array([item for sublist in x for item in sublist])
y = num_products.select("count").take(2000)
y = np.array([item for sublist in y for item in sublist])

ynew = []
xnew = []
for i,j in enumerate(y):
    if j < 30:
        ynew.append(j)
        xnew.append(x[i])

fig = plt.figure(1)
plt.bar(xnew,ynew,100)

fig.savefig("./hackathondata/task1b.png")
data["x"] = list(x)
data["y"] = list(y)
with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)
#TASK 1.c
#orders.show()
#order_products join orders sto order_id
user_ave=orders.groupby('user_id').count()
# counting works good
#user_ave.select("count").filter(user_ave.user_id=="1").show()

import pyspark.sql.functions as func


a = num_products.join(orders, num_products.order_id == orders.order_id).groupby("user_id").avg("count")
b = a.take(1000)
b = np.array([item for sublist in b for item in sublist])
fig = plt.figure()

bnew = []
for i in b:
    if i < 50:
        bnew.append(i) 
plt.hist(bnew, bins=100)
fig.savefig("./hackathondata/task1c")
data["b"] = list(b)


#join(department, people("deptId") === department("id")
#from pyspark.sql.functions import col
#df1.alias('a').join(df2.alias('b'),col('b.id') == col('a.id')).select([col('a.'+xx) for xx in a.columns] + [col('b.other1'),col('b.other2')])
with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)


#Task 1.d
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc


#users = orders.select("user_id").dropDuplicates()
#products.show()

#separate_products=products.select("product_id").dropDuplicates()
products_times=order_products.groupby('product_id').count()
products_times.show()

#products_times.select("product_id","count").orderBy((products_times.count).desc())

best = products_times.selectExpr("product_id","count as siz").sort(desc("count")).take(10)
x = []
y = []
for i in best:
    x.append(i.product_id)
    y.append(i.siz)

print(x,y)

fig = plt.figure()

xmnew = []
for i in x:
    xmnew.append(str(i))
plt.bar(xmnew,y,0.5)

fig.savefig("./hackathondata/task1d")

data["xm"] = x
data["ym"] = y
with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)


#Task 1.e

day = orders.select("order_dow").groupby("order_dow").count().sort(desc("count"))
hour = orders.select("order_hour_of_day").groupby("order_hour_of_day").count().sort(desc("count"))

h = hour.selectExpr("order_hour_of_day","count as siz").take(1000)
d = day.selectExpr("order_dow", "count as siz").take(1000)

xd = []
yd = []
xh = []
yh = []
for i in h:
    xh.append(i.order_hour_of_day)
    yh.append(i.siz)


for i in d:
    xd.append(i.order_dow)
    yd.append(i.siz)

fig = plt.figure()
plt.bar(xd,yd,1)
fig.savefig("./hackathondata/task1ed")

fig = plt.figure()
plt.bar(xh,yh,1)
fig.savefig("./hackathondata/task1eh")

data["xd"] = xd
data["yd"] = yd
data["xh"] = xh
data["yh"] = yh
with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)


#Task 1.f



#orders.sort(desc("days_since_prior_order")).show()
#orders.select("user_id","count").select(groupby("days_since_prior_order").count()).sort(desc("count")).show()
avge = orders.groupby("user_id").avg("days_since_prior_order")
avge = avge.select("avg(days_since_prior_order)")
avge = avge.collect()
avge = [item for sublist in avge for item in sublist]


data["avg"] = avge

with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)

fig = plt.figure(1)

avnew = []
for i in avge:
    if i <30 :
        avnew.append(i)
plt.hist(avge, bins = 200)
fig.savefig("./hackathondata/task1f")

with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)
#b = orders.groupby("user_id").values()
#c = b.groupby("days_since_prior_order").where(b.user_id).count().show()


# Task 2.a
cord = orders.select("order_id","user_id")

ordpr = order_products.select("order_id","product_id")
f = ordpr.join(cord, "order_id")
more = cord.groupby("user_id").count()
larger = f.groupby("user_id").count()

more = more.sort(asc("user_id"))
more.show()
mo = more.select("count")
mo = mo.collect()

larger = larger.sort(asc("user_id"))
la = larger.select("count")
la = la.collect()

#A = more.join(larger, more.user_id == larger.user_id)
#a =more.select("count")
#a = more.collect()
#a = np.array(a)
#b = larger.select("count")
#b = more.collect()
#b = np.array(b)
#



from pyspark.mllib.stat import Statistics
r1 = Statistics.corr(sc.parallelize(mo),sc.parallelize(la),method="pearson").head()
print("Correlation\n" + str(r1))





