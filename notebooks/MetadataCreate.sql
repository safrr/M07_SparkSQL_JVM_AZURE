-- Databricks notebook source
-- MAGIC %scala
-- MAGIC // set account key
-- MAGIC spark.conf.set("fs.azure.account.key.stsafrwesteurope.dfs.core.windows.net","+XeKLLgt/Lg+sgAFdQ3vzRBVZeFqUytjM/qIq1aWk5nigEtpLqdl7m0CGkpvXPyUoTl9azVrBOLyvIyV8O6byQ==");
-- MAGIC
-- MAGIC // directory paths
-- MAGIC //hotel-weather paths
-- MAGIC val hotelWeatherPath = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/hotel-weather/"
-- MAGIC val hotelWeatherSilverPath = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/hotel-weather/"
-- MAGIC //expedia paths
-- MAGIC val expediaPath = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/expedia/"
-- MAGIC val expediaSilverPath = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/expedia/"
-- MAGIC // results output paths
-- MAGIC val topHotelsMaxTemprDiff = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/gold/top_hotels_max_temp_diff/"
-- MAGIC val topBusyHotels = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/gold/top_ten_busy/"
-- MAGIC val extendedStay = s"abfss://data@stsafrwesteurope.dfs.core.windows.net/gold/visits_extended/"
-- MAGIC
-- MAGIC // remove directories
-- MAGIC dbutils.fs.rm(hotelWeatherSilverPath, true)
-- MAGIC dbutils.fs.rm(expediaSilverPath, true)
-- MAGIC
-- MAGIC //remove result output directories
-- MAGIC dbutils.fs.rm(topHotelsMaxTemprDiff, true)
-- MAGIC dbutils.fs.rm(topBusyHotels, true)
-- MAGIC dbutils.fs.rm(extendedStay, true)
-- MAGIC
-- MAGIC // read hotel-weather data (format-parquet) and write (format-delta) to hotelWeatherSilverPath
-- MAGIC  spark.read
-- MAGIC .format("parquet")
-- MAGIC .load(hotelWeatherPath)
-- MAGIC .write
-- MAGIC .format("delta")
-- MAGIC .mode("overwrite")
-- MAGIC .save(hotelWeatherSilverPath)
-- MAGIC
-- MAGIC // read expedia data (format-avro) and write (format-delta) to expediaSilverPath
-- MAGIC spark
-- MAGIC .read
-- MAGIC .format("avro")
-- MAGIC .load(expediaPath)
-- MAGIC .write
-- MAGIC .format("delta")
-- MAGIC .mode("overwrite")
-- MAGIC .save(expediaSilverPath)

-- COMMAND ----------

DROP TABLE IF EXISTS hotel_weather_silver

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // create delta table hotel_weather_silver based on data in storage account
-- MAGIC val hotelWeatherSilver = spark.sql(s"""
-- MAGIC CREATE TABLE hotel_weather_silver
-- MAGIC USING DELTA
-- MAGIC LOCATION "${hotelWeatherSilverPath}"
-- MAGIC """)

-- COMMAND ----------

DROP TABLE IF EXISTS expedia_silver

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // create delta table expedia_silver based on data in storage account
-- MAGIC val expediaSilver = spark.sql(s"""
-- MAGIC CREATE TABLE expedia_silver
-- MAGIC USING DELTA
-- MAGIC LOCATION "${expediaSilverPath}"
-- MAGIC """)

-- COMMAND ----------

-- Task_1 - Top 10 hotels with max absolute temperature difference by month.
 DROP TABLE IF EXISTS top_hotels_max_temp_diff

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql(s"""
-- MAGIC   CREATE TABLE top_hotels_max_temp_diff
-- MAGIC   USING PARQUET
-- MAGIC   LOCATION "${topHotelsMaxTemprDiff}"
-- MAGIC   AS (
-- MAGIC     SELECT ROUND((MAX(avg_tmpr_c) - MIN(avg_tmpr_c)), 2) as diff, address, id as hotel_id, year, month
-- MAGIC     FROM hotel_weather_silver
-- MAGIC     GROUP BY address, id, year, month
-- MAGIC     ORDER BY diff DESC
-- MAGIC     LIMIT 10)
-- MAGIC """)

-- COMMAND ----------

-- Task_2 - Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
DROP TABLE IF EXISTS top_busy_hotels;

CREATE OR REPLACE TEMPORARY VIEW affected_months
AS (
    SELECT EXPLODE(sequence(cast(checkin_min as date), cast(checkout_max as date), interval 1 month)) AS date,
           year(date) AS year,
           month(date) AS month
    FROM (SELECT MIN(srch_ci) AS checkin_min, MAX(srch_co) AS checkout_max
          FROM expedia_silver)
    );

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql(s"""
-- MAGIC   CREATE TABLE top_busy_hotels
-- MAGIC   USING PARQUET
-- MAGIC   LOCATION "${topBusyHotels}"
-- MAGIC   AS(
-- MAGIC     SELECT hotel_id, count(*) AS visits_count
-- MAGIC     FROM expedia_silver
-- MAGIC     JOIN affected_months ON (year(expedia_silver.srch_ci) = affected_months.year AND
-- MAGIC                             month(expedia_silver.srch_ci) = affected_months.month) OR
-- MAGIC                             (year(expedia_silver.srch_co) = affected_months.year AND
-- MAGIC                             month(expedia_silver.srch_co) = affected_months.month)
-- MAGIC     GROUP BY expedia_silver.hotel_id, affected_months.year, affected_months.month
-- MAGIC     ORDER BY visits_count DESC
-- MAGIC     LIMIT 10)
-- MAGIC  """)

-- COMMAND ----------

--  Task_3 - For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay

DROP TABLE IF EXISTS extended_stay_gold;

CREATE OR REPLACE TEMPORARY VIEW extended_stay
AS (
    SELECT id as expedia_id, hotel_id, srch_ci AS checkin, srch_co AS checkout
    FROM expedia_silver
    WHERE DATEDIFF(srch_co, srch_ci) > 7
   );

CREATE OR REPLACE TEMPORARY VIEW extended_stay_weather
AS (
  SELECT DISTINCT *
  FROM hotel_weather_silver
  JOIN extended_stay ON extended_stay.hotel_id = hotel_weather_silver.id AND
                       (hotel_weather_silver.wthr_date = extended_stay.checkin OR
                       hotel_weather_silver.wthr_date = extended_stay.checkout )
  );

CREATE OR REPLACE TEMPORARY VIEW trend
AS (
    SELECT *,
           ROUND((SELECT AVG(avg_tmpr_c)
                  FROM extended_stay_weather
                  WHERE checkout = wthr_date and esw.id = id) -
                 (SELECT AVG(avg_tmpr_c)
                  FROM extended_stay_weather
                  WHERE checkin = wthr_date and esw.id = id), 2) AS weather_trend
    FROM extended_stay_weather esw
  )

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql(s"""
-- MAGIC   CREATE TABLE extended_stay_gold
-- MAGIC   USING PARQUET
-- MAGIC   LOCATION "${extendedStay}"
-- MAGIC   AS (
-- MAGIC     SELECT expedia_id,
-- MAGIC            hotel_id,
-- MAGIC            checkin,
-- MAGIC            checkout,
-- MAGIC            DATEDIFF(checkout, checkin) AS duration,
-- MAGIC            weather_trend,
-- MAGIC            AVG(avg_tmpr_c) AS average_tmpr
-- MAGIC     FROM trend
-- MAGIC     WHERE wthr_date BETWEEN checkin AND checkout
-- MAGIC     GROUP BY expedia_id, hotel_id, checkout, checkin, weather_trend)
-- MAGIC """)

-- COMMAND ----------
