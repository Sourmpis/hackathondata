import sys
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)


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


order_products = order_products__prior.union(order_products__train)

order_products = order_products.select("order_id","product_id")


from pyspark.sql.functions import col

## Make a database of just icecreams and hours
#icecream = products.select("product_name","product_id").filter(products.product_name.like("%Ice Cream%"))
#icecream = icecream.join(order_products, order_products.product_id == icecream.product_id)
#icecream = icecream.select("order_id","product_name")
#icecream = icecream.join(orders, icecream.order_id == orders.order_id)
#icecream = icecream.select("order_hour_of_day", "product_name")
## every icecream order
#icecreams = icecream.select("order_hour_of_day").count() 
## filter the database for morning and evening hours
#icecreamMorning = icecream.filter(icecream.order_hour_of_day>8).filter(icecream.order_hour_of_day<17)\
#.where(icecream.order_hour_of_day.isNotNull()) 
#icecreamEvening = icecream.filter(icecream.order_hour_of_day>17).filter(icecream.order_hour_of_day<=23)\
#.where(icecream.order_hour_of_day.isNotNull())
#
#
#from pyspark.ml.feature import VectorAssembler
#vecAssembler = VectorAssembler(inputCols=['order_hour_of_day','count'], outputCol="count")
#
#stream_df = vecAssembler.transform(icecreams)
#stream_df.show()
#from pyspark.mllib.stat import Statistics
#pval = Statistics.chiSqTest(stream_df)
#print(pval)
#




from pyspark.sql.functions import col
# make a database with the alcohol instances including features -> Day & Hour
alcohol = departments.join(products, departments.department_id == products.department_id)
alcohol = alcohol.select("product_id","department")
alcohol = alcohol.join(order_products, alcohol.product_id == order_products.product_id)
alcohol = alcohol.select("department", "order_id")
alcohol = alcohol.join(orders, alcohol.order_id == orders.order_id)
alcohol = alcohol.select("order_dow","order_hour_of_day","department")
alcohol = alcohol.filter(alcohol.department=="alcohol")

# Seperate the database to intervals
# Interval (8-17)
morningAlc = alcohol.select("order_hour_of_day").filter(alcohol.order_hour_of_day>8).filter(alcohol.order_hour_of_day<17)\
.where(alcohol.order_hour_of_day.isNotNull())
# Interval (17-23]
eveningAlc = alcohol.select("order_hour_of_day").filter(alcohol.order_hour_of_day>17).filter(alcohol.order_hour_of_day<=23)\
.where(alcohol.order_hour_of_day.isNotNull())

# Find the percentages of alcohol bought in morning/evening
alc = alcohol.filter(alcohol.department=="alcohol").count()
morningPred = morningAlc.count()/float(alc)
eveningPred = eveningAlc.count()/float(alc)


# Prepare lists for chi squared
a = alcohol.filter(alcohol.order_hour_of_day>8).filter(alcohol.order_hour_of_day<17)\
.filter(alcohol.order_dow ==6).where(alcohol.order_hour_of_day.isNotNull()).count()
b = alcohol.filter(alcohol.order_hour_of_day>17).filter(alcohol.order_hour_of_day<=23)\
.filter(alcohol.order_dow==6).where(alcohol.order_hour_of_day.isNotNull()).count()
c = alcohol.filter(alcohol.order_hour_of_day>8).filter(alcohol.order_hour_of_day<17)\
.filter(alcohol.order_dow==0).where(alcohol.order_hour_of_day.isNotNull()).count()
d = alcohol.filter(alcohol.order_hour_of_day>17).filter(alcohol.order_hour_of_day<=23)\
.filter(alcohol.order_dow==0).where(alcohol.order_hour_of_day.isNotNull()).count()
e = alcohol.filter(alcohol.order_hour_of_day>8).filter(alcohol.order_hour_of_day<17)\
.filter(alcohol.order_dow==1).where(alcohol.order_hour_of_day.isNotNull()).count()
f  = alcohol.filter(alcohol.order_hour_of_day>17).filter(alcohol.order_hour_of_day<=23)\
.filter(alcohol.order_dow==1).where(alcohol.order_hour_of_day.isNotNull()).count()

print("Morning + weekend = ",float( (a+c+e)/(a+b+c+d+e+f)),"/n","Evening + weekend = ",float( (b+d+f)/(a+b+c+d+e+f)))

print("Morning alcohol = ", morningPred, eveningPred)




