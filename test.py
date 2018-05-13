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

#order_products = order_products__prior.union(order_products__train)
#
#order_products = order_products.select("order_id","product_id")
#
#
#
order_products__prior1 = order_products__prior.selectExpr("product_id","order_id as order_id_1")
products_FP = order_products__prior1.join(orders, order_products__prior1.order_id_1 == orders.order_id)

products_FP.select("product_id","order_id","user_id")
products_FP.show()

import pyspark.sql.functions as F



transactions = products_FP.groupby("order_id").agg(F.collect_list('product_id'))
transactions = products_FP.selectExpr("collect_list(product_id) as items")


#trans = transactions.select("items").take(20)
#trans = trans.collect()
#a = [(item) for sublist in trans for item in sublist]
#a = sc.parallelize(a)
#model = FPGrowth.train(a, minSupport=0.2, numPartitions=10)
#result = model.freqItemsets().collect()

#for fi in result:
#    print(fi)

from pyspark.ml.fpm import FPGrowth

fpGrowth = FPGrowth(itemsCol="items", minSupport=0.05, minConfidence=0.6)
model = fpGrowth.fit(transactions)
model.freqItemsets.show()

model.associationRules.show()

# transform examines the input items against all the association rules and summarize the
# consequents as prediction
model.transform(transactions).show()






#import pyspark.ml.stat
#icecream = orders.join(order_products, orders.order_id == order_products.order_id)
#icecream = icecream.select("order_hour_of_day","product_id")
#icecream = icecream.join(products, icecream.product_id == products.product_id)
#icecream = icecream.select("order_hour_of_day","product_name").show()
##icecream = icecream.filter(icecream.product_name=="Ice cream").show()
#from pyspark.sql.functions import col,desc
#from pyspark.ml.stat import ChiSquareTest
#alcohol = departments.join(products, departments.department_id == products.department_id)
#alcohol = alcohol.select("product_id","department")
#alcohol = alcohol.join(order_products, alcohol.product_id == order_products.product_id)
#alcohol = alcohol.select("department", "order_id")
#alcohol = alcohol.join(orders, alcohol.order_id == orders.order_id)
#alcohol = alcohol.select("order_dow","order_hour_of_day","department")
#alcohol = alcohol.filter(alcohol.department=="alcohol")
#alcDail = alcohol.groupby("order_dow").count().sort(desc("order_dow"))
#alcWeek = alcohol.groupby("order_hour_of_day").count().sort(desc("order_hour_of_day"))
##r = ChiSquareTest.test(alcohol, "order_dow", "count")
##print(r.pValues)
##print(r.degreesOfFreedom)
##print(r.statistics)
#
#alcohol = alcohol.filter(alcohol.department=="alcohol")
#morningAlc = alcohol.select("order_hour_of_day").filter(alcohol.order_hour_of_day>8).filter(alcohol.order_hour_of_day<17)\
#.where(alcohol.order_hour_of_day.isNotNull())
#eveningAlc = alcohol.select("order_hour_of_day").filter(alcohol.order_hour_of_day>17).filter(alcohol.order_hour_of_day<=23)\
#.where(alcohol.order_hour_of_day.isNotNull())
#print( "morning alcohol is :", 100*morningAlc.count()/alcohol.select("order_hour_of_day").count(),"/n",\
#     "evening alcohol is :", 100*eveningAlc.count()/alcohol.select("order_hour_of_day").count())
