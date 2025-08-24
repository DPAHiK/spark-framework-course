from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, sum, dense_rank, when, max, row_number
from pyspark.sql.window import Window

DB_URL = "jdbc:postgresql://pagila:5432/postgres"

spark = SparkSession.builder \
    .appName("MyPySparkApp") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

properties = {
    "user": "postgres",
    "password": "123456",
    "driver": "org.postgresql.Driver" 
}

actor = film_category = spark.read.jdbc(url=DB_URL, table="actor", properties=properties)
film_actor = film_category = spark.read.jdbc(url=DB_URL, table="film_actor", properties=properties)
film = film_category = spark.read.jdbc(url=DB_URL, table="film", properties=properties)
inventory = film_category = spark.read.jdbc(url=DB_URL, table="inventory", properties=properties)
rental = film_category = spark.read.jdbc(url=DB_URL, table="rental", properties=properties)
film_category = spark.read.jdbc(url=DB_URL, table="film_category", properties=properties)
category = spark.read.jdbc(url=DB_URL, table="category", properties=properties)
payment = spark.read.jdbc(url=DB_URL, table="payment", properties=properties)
city = spark.read.jdbc(url=DB_URL, table="city", properties=properties)
address = spark.read.jdbc(url=DB_URL, table="address", properties=properties)
customer = spark.read.jdbc(url=DB_URL, table="customer", properties=properties)

print(30 * '-')
print("Вывести количество фильмов в каждой категории, отсортировать по убыванию.")
print(30 * '-')
category.select("category_id", "name")\
    .join(film_category, on="category_id")\
    .drop("category_id")\
    .groupBy("name")\
    .agg(count("name").alias("count"))\
    .orderBy(col("count").desc())\
    .show()

print(30 * '-')
print("Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.")
print(30 * '-')
actor.join(film_actor, on="actor_id")\
    .join(film, on="film_id")\
    .join(inventory, on="film_id")\
    .join(rental, on="inventory_id")\
    .select("first_name", "last_name")\
    .groupBy("first_name", "last_name")\
    .agg(count("last_name").alias("count"))\
    .orderBy(col("count").desc())\
    .limit(10).show()

print(30 * '-')
print("Вывести категорию фильмов, на которую потратили больше всего денег.")
print(30 * '-')
category.join(film_category, on="category_id")\
    .join(film, on="film_id")\
    .join(inventory, on="film_id")\
    .join(rental, on="inventory_id")\
    .join(payment, on="rental_id")\
    .groupBy("name")\
    .agg(sum("amount").alias("price")).orderBy(col('price').desc())\
    .select("name")\
    .limit(1).show()

print(30 * '-')
print("Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.")
print(30 * '-')
film.join(inventory, on="film_id", how="left")\
    .where(inventory.film_id.isNull())\
    .select(film.title)\
    .show(film.count(), truncate=False)

print(30 * '-')
print("Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.")
print(30 * '-')
window = Window.orderBy(col('count').desc())
actor.join(film_actor, on="actor_id")\
    .join(film, on="film_id")\
    .join(film_category, on="film_id")\
    .join(category, on="category_id")\
    .where(col('name') == 'Children')\
    .groupBy(actor.first_name, actor.last_name)\
    .agg(count(film.title).alias('count'))\
    .withColumn('rank', dense_rank().over(window=window))\
    .where(col('rank') <= 3)\
    .select(actor.first_name, actor.last_name, 'count')\
    .show(actor.count(), truncate=False)

print(30 * '-')
print("вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). отсортировать по количеству неактивных клиентов по убыванию.")
print(30 * '-')
city.join(address, on="city_id")\
    .join(customer, on="address_id")\
    .groupBy(city.city)\
    .agg(count(when(col('active') == 1, True)).alias('active_customers'), count(when(col('active') == 0, True)).alias('inactive_customers'))\
    .orderBy(col('inactive_customers').desc())\
    .show(city.count(), truncate=False)

print(30 * '-')
print("Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.")
print(30 * '-')
category_hours = category.join(film_category, on="category_id")\
    .join(film, on="film_id")\
    .join(inventory, on="film_id")\
    .join(rental, on="inventory_id")\
    .join(customer, on="customer_id")\
    .join(address, on="address_id")\
    .join(city, on="city_id")\
    .groupBy(category.name, city.city)\
    .agg(sum(rental.return_date - rental.rental_date).alias('sum'))\
    .select(category.name, city.city, 'sum')\
    .orderBy(col('city').asc(), col('sum').desc())

window = Window.partitionBy('city').orderBy(col('sum').desc())
category_hours.where(col('city').like('A%'))\
    .withColumn('row_number', row_number().over(window=window)).where(col('row_number') == 1)\
    .drop('row_number')\
    \
    .unionAll(category_hours.where(col('city').like('%-%'))\
              .withColumn('row_number', row_number().over(window=window))\
              .where(col('row_number') == 1).drop('row_number'))\
    .show(category_hours.count(), truncate=False)  

spark.stop()