* Add your code in `src/main/` if needed
* Test your code with `src/tests/` if needed
* Modify notebooks for your needs
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```

* Launch notebooks on Databricks cluster

* Created databricks service and storage account by terraform script

![](screenshots/azure.png)

* Copied data (hotel-weather, expedia) to generated storage account

./azcopy copy "https://bd201stacc.blob.core.windows.net/m07sparksql?sv=2020-02-10&ss=bfqt&srt=sco&sp=rlx&se=2031-04-08T17:32:24Z&st=2021-04-08T09:32:24Z&spr=https&sig=5An75VCvK%2FCfuPiiWf8knAHhrMNIR%2BE37oUx3b%2FLUQc%3D" "https://stsafrwesteurope.blob.core.windows.net/data" --recursive

![](screenshots/storage.png)

* Created spark cluster

![](screenshots/cluster.png)

* Secret scope databricks
```
databricks secrets create-scope --scope <scope-name> --initial-manage-principal "users"

databricks secrets put --scope <scope-name>  --key <value>
```
## Tasks
* Task_1 - Top 10 hotels with max absolute temperature difference by month.
![](screenshots/task1.png)
![](screenshots/diff1.png)

== Parsed Logical Plan ==
'CreateTableAsSelectStatement [top_hotels_max_temp_diff], DELTA, , false, false
+- 'GlobalLimit 10
   +- 'LocalLimit 10
      +- 'Sort ['diff DESC NULLS LAST], true
         +- 'Aggregate ['address, 'id, 'year, 'month], ['ROUND(('MAX('avg_tmpr_c) - 'MIN('avg_tmpr_c)), 2) AS diff#2557, 'address, 'id AS hotel_id#2558, 'year, 'month]
            +- 'UnresolvedRelation [hotel_weather_silver], [], false

== Analyzed Logical Plan ==
num_affected_rows: bigint, num_inserted_rows: bigint
CreateTableAsSelect com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog@5cdac099, default.top_hotels_max_temp_diff, [provider=DELTA, location=], false
+- GlobalLimit 10
   +- LocalLimit 10
      +- Sort [diff#2557 DESC NULLS LAST], true
         +- Aggregate [address#2585, id#2591, year#2596, month#2597], [round((max(avg_tmpr_c#2586) - min(avg_tmpr_c#2586)), 2) AS diff#2557, address#2585, id#2591 AS hotel_id#2558, year#2596, month#2597]
            +- SubqueryAlias spark_catalog.default.hotel_weather_silver
               +- Relation default.hotel_weather_silver[address#2585,avg_tmpr_c#2586,avg_tmpr_f#2587,city#2588,country#2589,geoHash#2590,id#2591,latitude#2592,longitude#2593,name#2594,wthr_date#2595,year#2596,month#2597,day#2598] parquet

== Optimized Logical Plan ==
CreateTableAsSelect com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog@5cdac099, default.top_hotels_max_temp_diff, [provider=DELTA, location=], false
+- GlobalLimit 10
   +- LocalLimit 10
      +- Sort [diff#2557 DESC NULLS LAST], true
         +- Aggregate [address#2585, id#2591, year#2596, month#2597], [round((max(avg_tmpr_c#2586) - min(avg_tmpr_c#2586)), 2) AS diff#2557, address#2585, id#2591 AS hotel_id#2558, year#2596, month#2597]
            +- Project [address#2585, avg_tmpr_c#2586, id#2591, year#2596, month#2597]
               +- Relation default.hotel_weather_silver[address#2585,avg_tmpr_c#2586,avg_tmpr_f#2587,city#2588,country#2589,geoHash#2590,id#2591,latitude#2592,longitude#2593,name#2594,wthr_date#2595,year#2596,month#2597,day#2598] parquet

== Physical Plan ==
AtomicCreateTableAsSelect [num_affected_rows#2641L, num_inserted_rows#2642L], com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog@5cdac099, default.top_hotels_max_temp_diff, GlobalLimit 10, [provider=DELTA, location=, owner=root], [], false
+- AdaptiveSparkPlan isFinalPlan=false
   +- TakeOrderedAndProject(limit=10, orderBy=[diff#2557 DESC NULLS LAST], output=[diff#2557,address#2585,hotel_id#2558,year#2596,month#2597])
      +- HashAggregate(keys=[address#2585, id#2591, year#2596, month#2597], functions=[finalmerge_max(merge max#2620) AS max(avg_tmpr_c#2586)#2599, finalmerge_min(merge min#2622) AS min(avg_tmpr_c#2586)#2600], output=[diff#2557, address#2585, hotel_id#2558, year#2596, month#2597])
         +- Exchange hashpartitioning(address#2585, id#2591, year#2596, month#2597, 200), ENSURE_REQUIREMENTS, [id=#1814]
            +- HashAggregate(keys=[address#2585, id#2591, year#2596, month#2597], functions=[partial_max(avg_tmpr_c#2586) AS max#2620, partial_min(avg_tmpr_c#2586) AS min#2622], output=[address#2585, id#2591, year#2596, month#2597, max#2620, min#2622])
               +- FileScan parquet default.hotel_weather_silver[address#2585,avg_tmpr_c#2586,id#2591,year#2596,month#2597] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/hotel-w..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<address:string,avg_tmpr_c:double,id:string,year:int,month:int>
               
               
* Reading the file and analyzing it
* Filtering based on the criteria
* Exchange records and using aggregation function
* Selecting required columns
               
         
* Task_2 -Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
![](screenshots/task2.png)
![](screenshots/visits2.png)

               == Parsed Logical Plan ==
               'GlobalLimit 10
               +- 'LocalLimit 10
                  +- 'Sort ['visits_count DESC NULLS LAST], true
                     +- 'Aggregate ['expedia_silver.hotel_id, 'affected_months.year, 'affected_months.month], ['hotel_id, 'count(1) AS visits_count#4779]
                        +- 'Join Inner, ((('year('expedia_silver.srch_ci) = 'affected_months.year) AND ('month('expedia_silver.srch_ci) = 'affected_months.month)) OR (('year('expedia_silver.srch_co) = 'affected_months.year) AND ('month('expedia_silver.srch_co) = 'affected_months.month)))
                           :- 'UnresolvedRelation [expedia_silver], [], false
                           +- 'UnresolvedRelation [affected_months], [], false
               
               == Analyzed Logical Plan ==
               hotel_id: bigint, visits_count: bigint
               GlobalLimit 10
               +- LocalLimit 10
                  +- Sort [visits_count#4779L DESC NULLS LAST], true
                     +- Aggregate [hotel_id#4855L, year#4791, month#4792], [hotel_id#4855L, count(1) AS visits_count#4779L]
                        +- Join Inner, (((year(cast(srch_ci#4848 as date)) = year#4791) AND (month(cast(srch_ci#4848 as date)) = month#4792)) OR ((year(cast(srch_co#4849 as date)) = year#4791) AND (month(cast(srch_co#4849 as date)) = month#4792)))
                           :- SubqueryAlias spark_catalog.default.expedia_silver
                           :  +- Relation default.expedia_silver[id#4836L,date_time#4837,site_name#4838,posa_continent#4839,user_location_country#4840,user_location_region#4841,user_location_city#4842,orig_destination_distance#4843,user_id#4844,is_mobile#4845,is_package#4846,channel#4847,srch_ci#4848,srch_co#4849,srch_adults_cnt#4850,srch_children_cnt#4851,srch_rm_cnt#4852,srch_destination_id#4853,srch_destination_type_id#4854,hotel_id#4855L] parquet
                           +- SubqueryAlias affected_months
                              +- View (`affected_months`, [date#4790,year#4791,month#4792])
                                 +- Project [cast(date#4835 as date) AS date#4790, cast(year#4788 as int) AS year#4791, cast(month#4789 as int) AS month#4792]
                                    +- Project [date#4835, year(date#4835) AS year#4788, month(date#4835) AS month#4789]
                                       +- Generate explode(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC))), false, [date#4835]
                                          +- SubqueryAlias __auto_generated_subquery_name
                                             +- Aggregate [min(srch_ci#4825) AS checkin_min#4785, max(srch_co#4826) AS checkout_max#4786]
                                                +- SubqueryAlias spark_catalog.default.expedia_silver
                                                   +- Relation default.expedia_silver[id#4813L,date_time#4814,site_name#4815,posa_continent#4816,user_location_country#4817,user_location_region#4818,user_location_city#4819,orig_destination_distance#4820,user_id#4821,is_mobile#4822,is_package#4823,channel#4824,srch_ci#4825,srch_co#4826,srch_adults_cnt#4827,srch_children_cnt#4828,srch_rm_cnt#4829,srch_destination_id#4830,srch_destination_type_id#4831,hotel_id#4832L] parquet
               
               == Optimized Logical Plan ==
               GlobalLimit 10
               +- LocalLimit 10
                  +- Sort [visits_count#4779L DESC NULLS LAST], true
                     +- Project [hotel_id#4855L, count(1)#4856L AS visits_count#4779L]
                        +- AggregatePart [hotel_id#4855L, year#4791, month#4792], [finalmerge_count(merge count#4858L) AS count(1)#4856L], true
                           +- AggregatePart [hotel_id#4855L, year#4791, month#4792], [merge_count(merge count#4858L) AS count#4858L], false
                              +- Project [hotel_id#4855L, year#4791, month#4792, count#4858L]
                                 +- Join Inner, (((year(cast(srch_ci#4848 as date)) = year#4791) AND (month(cast(srch_ci#4848 as date)) = month#4792)) OR ((year(cast(srch_co#4849 as date)) = year#4791) AND (month(cast(srch_co#4849 as date)) = month#4792)))
                                    :- Project [srch_ci#4848, srch_co#4849, hotel_id#4855L]
                                    :  +- Relation default.expedia_silver[id#4836L,date_time#4837,site_name#4838,posa_continent#4839,user_location_country#4840,user_location_region#4841,user_location_city#4842,orig_destination_distance#4843,user_id#4844,is_mobile#4845,is_package#4846,channel#4847,srch_ci#4848,srch_co#4849,srch_adults_cnt#4850,srch_children_cnt#4851,srch_rm_cnt#4852,srch_destination_id#4853,srch_destination_type_id#4854,hotel_id#4855L] parquet
                                    +- AggregatePart [year#4791, month#4792], [partial_count(1) AS count#4858L], false
                                       +- Project [year(date#4835) AS year#4791, month(date#4835) AS month#4792]
                                          +- Generate explode(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC))), [0, 1], false, [date#4835]
                                             +- Filter ((size(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC)), true) > 0) AND isnotnull(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC))))
                                                +- Aggregate [min(srch_ci#4825) AS checkin_min#4785, max(srch_co#4826) AS checkout_max#4786]
                                                   +- Project [srch_ci#4825, srch_co#4826]
                                                      +- Relation default.expedia_silver[id#4813L,date_time#4814,site_name#4815,posa_continent#4816,user_location_country#4817,user_location_region#4818,user_location_city#4819,orig_destination_distance#4820,user_id#4821,is_mobile#4822,is_package#4823,channel#4824,srch_ci#4825,srch_co#4826,srch_adults_cnt#4827,srch_children_cnt#4828,srch_rm_cnt#4829,srch_destination_id#4830,srch_destination_type_id#4831,hotel_id#4832L] parquet
               
               == Physical Plan ==
               AdaptiveSparkPlan isFinalPlan=false
               +- TakeOrderedAndProject(limit=10, orderBy=[visits_count#4779L DESC NULLS LAST], output=[hotel_id#4855L,visits_count#4779L])
                  +- Project [hotel_id#4855L, count(1)#4856L AS visits_count#4779L]
                     +- HashAggregate(keys=[hotel_id#4855L, year#4791, month#4792], functions=[finalmerge_count(merge count#4858L) AS count(1)#4856L], output=[hotel_id#4855L, year#4791, month#4792, count(1)#4856L])
                        +- Exchange hashpartitioning(hotel_id#4855L, year#4791, month#4792, 200), ENSURE_REQUIREMENTS, [id=#6081]
                           +- HashAggregate(keys=[hotel_id#4855L, year#4791, month#4792], functions=[merge_count(merge count#4858L) AS count#4858L], output=[hotel_id#4855L, year#4791, month#4792, count#4858L])
                              +- Project [hotel_id#4855L, year#4791, month#4792, count#4858L]
                                 +- BroadcastNestedLoopJoin BuildRight, Inner, (((year(cast(srch_ci#4848 as date)) = year#4791) AND (month(cast(srch_ci#4848 as date)) = month#4792)) OR ((year(cast(srch_co#4849 as date)) = year#4791) AND (month(cast(srch_co#4849 as date)) = month#4792)))
                                    :- FileScan parquet default.expedia_silver[srch_ci#4848,srch_co#4849,hotel_id#4855L] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/expedia], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<srch_ci:string,srch_co:string,hotel_id:bigint>
                                    +- BroadcastExchange IdentityBroadcastMode, [id=#6076]
                                       +- HashAggregate(keys=[year#4791, month#4792], functions=[partial_count(1) AS count#4858L], output=[year#4791, month#4792, count#4858L])
                                          +- Project [year(date#4835) AS year#4791, month(date#4835) AS month#4792]
                                             +- Generate explode(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC))), false, [date#4835]
                                                +- Filter ((size(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC)), true) > 0) AND isnotnull(sequence(cast(checkin_min#4785 as date), cast(checkout_max#4786 as date), Some(INTERVAL '1' MONTH), Some(Etc/UTC))))
                                                   +- SortAggregate(key=[], functions=[finalmerge_min(merge min#4860) AS min(srch_ci#4825)#4833, finalmerge_max(merge max#4862) AS max(srch_co#4826)#4834], output=[checkin_min#4785, checkout_max#4786])
                                                      +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#6069]
                                                         +- SortAggregate(key=[], functions=[partial_min(srch_ci#4825) AS min#4860, partial_max(srch_co#4826) AS max#4862], output=[min#4860, max#4862])
                                                            +- FileScan parquet default.expedia_silver[srch_ci#4825,srch_co#4826] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/expedia], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<srch_ci:string,srch_co:string>   

* Reading the file and analyzing it
* Filtering based on the criteria
* Shuffling data
* At the same time reading data from another table and filtering based on the criteria
* Exploding records and using aggregation function with shuffling
* Doing a broadcast hash join and selecting the right columns

* Task_3 - For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.
![](screenshots/task3.png)
![](screenshots/avg3.png)

== Parsed Logical Plan ==
'Aggregate ['expedia_id, 'hotel_id, 'checkout, 'checkin, 'weather_trend], ['expedia_id, 'hotel_id, 'checkin, 'checkout, 'DATEDIFF('checkout, 'checkin) AS duration#70048, 'weather_trend, 'AVG('avg_tmpr_c) AS average_tmpr#70049]
+- 'Filter (('wthr_date >= 'checkin) AND ('wthr_date <= 'checkout))
   +- 'UnresolvedRelation [trend], [], false

== Analyzed Logical Plan ==
expedia_id: bigint, hotel_id: bigint, checkin: string, checkout: string, duration: int, weather_trend: double, average_tmpr: double
Aggregate [expedia_id#70072L, hotel_id#70073L, checkout#70075, checkin#70074, weather_trend#70076], [expedia_id#70072L, hotel_id#70073L, checkin#70074, checkout#70075, datediff(cast(checkout#70075 as date), cast(checkin#70074 as date)) AS duration#70048, weather_trend#70076, avg(avg_tmpr_c#70059) AS average_tmpr#70049]
+- Filter ((wthr_date#70068 >= checkin#70074) AND (wthr_date#70068 <= checkout#70075))
   +- SubqueryAlias trend
      +- View (`trend`, [address#70058,avg_tmpr_c#70059,avg_tmpr_f#70060,city#70061,country#70062,geoHash#70063,id#70064,latitude#70065,longitude#70066,name#70067,wthr_date#70068,year#70069,month#70070,day#70071,expedia_id#70072L,hotel_id#70073L,checkin#70074,checkout#70075,weather_trend#70076])
         +- Project [cast(address#70077 as string) AS address#70058, cast(avg_tmpr_c#70078 as double) AS avg_tmpr_c#70059, cast(avg_tmpr_f#70079 as double) AS avg_tmpr_f#70060, cast(city#70080 as string) AS city#70061, cast(country#70081 as string) AS country#70062, cast(geoHash#70082 as string) AS geoHash#70063, cast(id#70083 as string) AS id#70064, cast(latitude#70084 as double) AS latitude#70065, cast(longitude#70085 as double) AS longitude#70066, cast(name#70086 as string) AS name#70067, cast(wthr_date#70087 as string) AS wthr_date#70068, cast(year#70088 as int) AS year#70069, cast(month#70089 as int) AS month#70070, cast(day#70090 as int) AS day#70071, cast(expedia_id#70091L as bigint) AS expedia_id#70072L, cast(hotel_id#70092L as bigint) AS hotel_id#70073L, cast(checkin#70093 as string) AS checkin#70074, cast(checkout#70094 as string) AS checkout#70075, cast(weather_trend#70057 as double) AS weather_trend#70076]
            +- Project [address#70077, avg_tmpr_c#70078, avg_tmpr_f#70079, city#70080, country#70081, geoHash#70082, id#70083, latitude#70084, longitude#70085, name#70086, wthr_date#70087, year#70088, month#70089, day#70090, expedia_id#70091L, hotel_id#70092L, checkin#70093, checkout#70094, round((scalar-subquery#70055 [id#70083] - scalar-subquery#70056 [id#70083]), 2) AS weather_trend#70057]
               :  :- Aggregate [avg(avg_tmpr_c#70171) AS avg(avg_tmpr_c)#70230]
               :  :  +- Filter ((checkout#70187 = wthr_date#70180) AND (outer(id#70083) = id#70176))
               :  :     +- SubqueryAlias extended_stay_weather
               :  :        +- View (`extended_stay_weather`, [address#70170,avg_tmpr_c#70171,avg_tmpr_f#70172,city#70173,country#70174,geoHash#70175,id#70176,latitude#70177,longitude#70178,name#70179,wthr_date#70180,year#70181,month#70182,day#70183,expedia_id#70184L,hotel_id#70185L,checkin#70186,checkout#70187])
               :  :           +- Project [cast(address#70215 as string) AS address#70170, cast(avg_tmpr_c#70216 as double) AS avg_tmpr_c#70171, cast(avg_tmpr_f#70217 as double) AS avg_tmpr_f#70172, cast(city#70218 as string) AS city#70173, cast(country#70219 as string) AS country#70174, cast(geoHash#70220 as string) AS geoHash#70175, cast(id#70221 as string) AS id#70176, cast(latitude#70222 as double) AS latitude#70177, cast(longitude#70223 as double) AS longitude#70178, cast(name#70224 as string) AS name#70179, cast(wthr_date#70225 as string) AS wthr_date#70180, cast(year#70226 as int) AS year#70181, cast(month#70227 as int) AS month#70182, cast(day#70228 as int) AS day#70183, cast(expedia_id#70191L as bigint) AS expedia_id#70184L, cast(hotel_id#70192L as bigint) AS hotel_id#70185L, cast(checkin#70193 as string) AS checkin#70186, cast(checkout#70194 as string) AS checkout#70187]
               :  :              +- Distinct
               :  :                 +- Project [address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70192L, checkin#70193, checkout#70194]
               :  :                    +- Join Inner, ((hotel_id#70192L = cast(id#70221 as bigint)) AND ((wthr_date#70225 = checkin#70193) OR (wthr_date#70225 = checkout#70194)))
               :  :                       :- SubqueryAlias spark_catalog.default.hotel_weather_silver
               :  :                       :  +- Relation default.hotel_weather_silver[address#70215,avg_tmpr_c#70216,avg_tmpr_f#70217,city#70218,country#70219,geoHash#70220,id#70221,latitude#70222,longitude#70223,name#70224,wthr_date#70225,year#70226,month#70227,day#70228] parquet
               :  :                       +- SubqueryAlias extended_stay
               :  :                          +- View (`extended_stay`, [expedia_id#70191L,hotel_id#70192L,checkin#70193,checkout#70194])
               :  :                             +- Project [cast(expedia_id#70188L as bigint) AS expedia_id#70191L, cast(hotel_id#70214L as bigint) AS hotel_id#70192L, cast(checkin#70189 as string) AS checkin#70193, cast(checkout#70190 as string) AS checkout#70194]
               :  :                                +- Project [id#70195L AS expedia_id#70188L, hotel_id#70214L, srch_ci#70207 AS checkin#70189, srch_co#70208 AS checkout#70190]
               :  :                                   +- Filter (datediff(cast(srch_co#70208 as date), cast(srch_ci#70207 as date)) > 7)
               :  :                                      +- SubqueryAlias spark_catalog.default.expedia_silver
               :  :                                         +- Relation default.expedia_silver[id#70195L,date_time#70196,site_name#70197,posa_continent#70198,user_location_country#70199,user_location_region#70200,user_location_city#70201,orig_destination_distance#70202,user_id#70203,is_mobile#70204,is_package#70205,channel#70206,srch_ci#70207,srch_co#70208,srch_adults_cnt#70209,srch_children_cnt#70210,srch_rm_cnt#70211,srch_destination_id#70212,srch_destination_type_id#70213,hotel_id#70214L] parquet
               :  +- Aggregate [avg(avg_tmpr_c#70232) AS avg(avg_tmpr_c)#70291]
               :     +- Filter ((checkin#70247 = wthr_date#70241) AND (outer(id#70083) = id#70237))
               :        +- SubqueryAlias extended_stay_weather
               :           +- View (`extended_stay_weather`, [address#70231,avg_tmpr_c#70232,avg_tmpr_f#70233,city#70234,country#70235,geoHash#70236,id#70237,latitude#70238,longitude#70239,name#70240,wthr_date#70241,year#70242,month#70243,day#70244,expedia_id#70245L,hotel_id#70246L,checkin#70247,checkout#70248])
               :              +- Project [cast(address#70276 as string) AS address#70231, cast(avg_tmpr_c#70277 as double) AS avg_tmpr_c#70232, cast(avg_tmpr_f#70278 as double) AS avg_tmpr_f#70233, cast(city#70279 as string) AS city#70234, cast(country#70280 as string) AS country#70235, cast(geoHash#70281 as string) AS geoHash#70236, cast(id#70282 as string) AS id#70237, cast(latitude#70283 as double) AS latitude#70238, cast(longitude#70284 as double) AS longitude#70239, cast(name#70285 as string) AS name#70240, cast(wthr_date#70286 as string) AS wthr_date#70241, cast(year#70287 as int) AS year#70242, cast(month#70288 as int) AS month#70243, cast(day#70289 as int) AS day#70244, cast(expedia_id#70252L as bigint) AS expedia_id#70245L, cast(hotel_id#70253L as bigint) AS hotel_id#70246L, cast(checkin#70254 as string) AS checkin#70247, cast(checkout#70255 as string) AS checkout#70248]
               :                 +- Distinct
               :                    +- Project [address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70253L, checkin#70254, checkout#70255]
               :                       +- Join Inner, ((hotel_id#70253L = cast(id#70282 as bigint)) AND ((wthr_date#70286 = checkin#70254) OR (wthr_date#70286 = checkout#70255)))
               :                          :- SubqueryAlias spark_catalog.default.hotel_weather_silver
               :                          :  +- Relation default.hotel_weather_silver[address#70276,avg_tmpr_c#70277,avg_tmpr_f#70278,city#70279,country#70280,geoHash#70281,id#70282,latitude#70283,longitude#70284,name#70285,wthr_date#70286,year#70287,month#70288,day#70289] parquet
               :                          +- SubqueryAlias extended_stay
               :                             +- View (`extended_stay`, [expedia_id#70252L,hotel_id#70253L,checkin#70254,checkout#70255])
               :                                +- Project [cast(expedia_id#70249L as bigint) AS expedia_id#70252L, cast(hotel_id#70275L as bigint) AS hotel_id#70253L, cast(checkin#70250 as string) AS checkin#70254, cast(checkout#70251 as string) AS checkout#70255]
               :                                   +- Project [id#70256L AS expedia_id#70249L, hotel_id#70275L, srch_ci#70268 AS checkin#70250, srch_co#70269 AS checkout#70251]
               :                                      +- Filter (datediff(cast(srch_co#70269 as date), cast(srch_ci#70268 as date)) > 7)
               :                                         +- SubqueryAlias spark_catalog.default.expedia_silver
               :                                            +- Relation default.expedia_silver[id#70256L,date_time#70257,site_name#70258,posa_continent#70259,user_location_country#70260,user_location_region#70261,user_location_city#70262,orig_destination_distance#70263,user_id#70264,is_mobile#70265,is_package#70266,channel#70267,srch_ci#70268,srch_co#70269,srch_adults_cnt#70270,srch_children_cnt#70271,srch_rm_cnt#70272,srch_destination_id#70273,srch_destination_type_id#70274,hotel_id#70275L] parquet
               +- SubqueryAlias esw
                  +- SubqueryAlias extended_stay_weather
                     +- View (`extended_stay_weather`, [address#70077,avg_tmpr_c#70078,avg_tmpr_f#70079,city#70080,country#70081,geoHash#70082,id#70083,latitude#70084,longitude#70085,name#70086,wthr_date#70087,year#70088,month#70089,day#70090,expedia_id#70091L,hotel_id#70092L,checkin#70093,checkout#70094])
                        +- Project [cast(address#70156 as string) AS address#70077, cast(avg_tmpr_c#70157 as double) AS avg_tmpr_c#70078, cast(avg_tmpr_f#70158 as double) AS avg_tmpr_f#70079, cast(city#70159 as string) AS city#70080, cast(country#70160 as string) AS country#70081, cast(geoHash#70161 as string) AS geoHash#70082, cast(id#70162 as string) AS id#70083, cast(latitude#70163 as double) AS latitude#70084, cast(longitude#70164 as double) AS longitude#70085, cast(name#70165 as string) AS name#70086, cast(wthr_date#70166 as string) AS wthr_date#70087, cast(year#70167 as int) AS year#70088, cast(month#70168 as int) AS month#70089, cast(day#70169 as int) AS day#70090, cast(expedia_id#70098L as bigint) AS expedia_id#70091L, cast(hotel_id#70099L as bigint) AS hotel_id#70092L, cast(checkin#70100 as string) AS checkin#70093, cast(checkout#70101 as string) AS checkout#70094]
                           +- Distinct
                              +- Project [address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70099L, checkin#70100, checkout#70101]
                                 +- Join Inner, ((hotel_id#70099L = cast(id#70162 as bigint)) AND ((wthr_date#70166 = checkin#70100) OR (wthr_date#70166 = checkout#70101)))
                                    :- SubqueryAlias spark_catalog.default.hotel_weather_silver
                                    :  +- Relation default.hotel_weather_silver[address#70156,avg_tmpr_c#70157,avg_tmpr_f#70158,city#70159,country#70160,geoHash#70161,id#70162,latitude#70163,longitude#70164,name#70165,wthr_date#70166,year#70167,month#70168,day#70169] parquet
                                    +- SubqueryAlias extended_stay
                                       +- View (`extended_stay`, [expedia_id#70098L,hotel_id#70099L,checkin#70100,checkout#70101])
                                          +- Project [cast(expedia_id#70095L as bigint) AS expedia_id#70098L, cast(hotel_id#70141L as bigint) AS hotel_id#70099L, cast(checkin#70096 as string) AS checkin#70100, cast(checkout#70097 as string) AS checkout#70101]
                                             +- Project [id#70122L AS expedia_id#70095L, hotel_id#70141L, srch_ci#70134 AS checkin#70096, srch_co#70135 AS checkout#70097]
                                                +- Filter (datediff(cast(srch_co#70135 as date), cast(srch_ci#70134 as date)) > 7)
                                                   +- SubqueryAlias spark_catalog.default.expedia_silver
                                                      +- Relation default.expedia_silver[id#70122L,date_time#70123,site_name#70124,posa_continent#70125,user_location_country#70126,user_location_region#70127,user_location_city#70128,orig_destination_distance#70129,user_id#70130,is_mobile#70131,is_package#70132,channel#70133,srch_ci#70134,srch_co#70135,srch_adults_cnt#70136,srch_children_cnt#70137,srch_rm_cnt#70138,srch_destination_id#70139,srch_destination_type_id#70140,hotel_id#70141L] parquet

== Optimized Logical Plan ==
Aggregate [expedia_id#70098L, hotel_id#70141L, checkout#70101, checkin#70100, weather_trend#70076], [expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, datediff(cast(checkout#70101 as date), cast(checkin#70100 as date)) AS duration#70048, weather_trend#70076, avg(avg_tmpr_c#70157) AS average_tmpr#70049]
+- Project [avg_tmpr_c#70157, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, round((avg(avg_tmpr_c)#70230 - avg(avg_tmpr_c)#70291), 2) AS weather_trend#70076]
   +- Join LeftOuter, (id#70162 = id#70282)
      :- Project [avg_tmpr_c#70157, id#70162, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, avg(avg_tmpr_c)#70230]
      :  +- Join LeftOuter, (id#70162 = id#70221)
      :     :- Project [avg_tmpr_c#70157, id#70162, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101]
      :     :  +- AggregatePart [address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101], true
      :     :     +- AggregatePart [address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101], false
      :     :        +- Join Inner, ((((wthr_date#70166 >= checkin#70100) AND (wthr_date#70166 <= checkout#70101)) AND (hotel_id#70141L = cast(id#70162 as bigint))) AND ((wthr_date#70166 = checkin#70100) OR (wthr_date#70166 = checkout#70101)))
      :     :           :- AggregatePart [address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169], false
      :     :           :  +- Filter (isnotnull(wthr_date#70166) AND isnotnull(id#70162))
      :     :           :     +- Relation default.hotel_weather_silver[address#70156,avg_tmpr_c#70157,avg_tmpr_f#70158,city#70159,country#70160,geoHash#70161,id#70162,latitude#70163,longitude#70164,name#70165,wthr_date#70166,year#70167,month#70168,day#70169] parquet
      :     :           +- AggregatePart [expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101], false
      :     :              +- Project [id#70122L AS expedia_id#70098L, hotel_id#70141L, srch_ci#70134 AS checkin#70100, srch_co#70135 AS checkout#70101]
      :     :                 +- Filter (((isnotnull(srch_co#70135) AND isnotnull(srch_ci#70134)) AND (datediff(cast(srch_co#70135 as date), cast(srch_ci#70134 as date)) > 7)) AND isnotnull(hotel_id#70141L))
      :     :                    +- Relation default.expedia_silver[id#70122L,date_time#70123,site_name#70124,posa_continent#70125,user_location_country#70126,user_location_region#70127,user_location_city#70128,orig_destination_distance#70129,user_id#70130,is_mobile#70131,is_package#70132,channel#70133,srch_ci#70134,srch_co#70135,srch_adults_cnt#70136,srch_children_cnt#70137,srch_rm_cnt#70138,srch_destination_id#70139,srch_destination_type_id#70140,hotel_id#70141L] parquet
      :     +- Aggregate [id#70221], [avg(avg_tmpr_c#70216) AS avg(avg_tmpr_c)#70230, id#70221]
      :        +- Project [avg_tmpr_c#70216, id#70221]
      :           +- AggregatePart [address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], true
      :              +- AggregatePart [address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], false
      :                 +- Join Inner, (((checkout#70194 = wthr_date#70225) AND (hotel_id#70214L = cast(id#70221 as bigint))) AND ((wthr_date#70225 = checkin#70193) OR (wthr_date#70225 = checkout#70194)))
      :                    :- AggregatePart [address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, wthr_date#70225], false
      :                    :  +- AggregatePart [address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, wthr_date#70225], false
      :                    :     +- Filter (isnotnull(wthr_date#70225) AND isnotnull(id#70221))
      :                    :        +- Relation default.hotel_weather_silver[address#70215,avg_tmpr_c#70216,avg_tmpr_f#70217,city#70218,country#70219,geoHash#70220,id#70221,latitude#70222,longitude#70223,name#70224,wthr_date#70225,year#70226,month#70227,day#70228] parquet
      :                    +- AggregatePart [checkout#70194, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], false
      :                       +- AggregatePart [checkout#70194, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], false
      :                          +- Project [id#70195L AS expedia_id#70191L, hotel_id#70214L, srch_ci#70207 AS checkin#70193, srch_co#70208 AS checkout#70194]
      :                             +- Filter (((isnotnull(srch_co#70208) AND isnotnull(srch_ci#70207)) AND (datediff(cast(srch_co#70208 as date), cast(srch_ci#70207 as date)) > 7)) AND (isnotnull(hotel_id#70214L) AND ((srch_co#70208 = srch_ci#70207) OR isnotnull(srch_co#70208))))
      :                                +- Relation default.expedia_silver[id#70195L,date_time#70196,site_name#70197,posa_continent#70198,user_location_country#70199,user_location_region#70200,user_location_city#70201,orig_destination_distance#70202,user_id#70203,is_mobile#70204,is_package#70205,channel#70206,srch_ci#70207,srch_co#70208,srch_adults_cnt#70209,srch_children_cnt#70210,srch_rm_cnt#70211,srch_destination_id#70212,srch_destination_type_id#70213,hotel_id#70214L] parquet
      +- Aggregate [id#70282], [avg(avg_tmpr_c#70277) AS avg(avg_tmpr_c)#70291, id#70282]
         +- Project [avg_tmpr_c#70277, id#70282]
            +- AggregatePart [address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], true
               +- AggregatePart [address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], false
                  +- Join Inner, (((checkin#70254 = wthr_date#70286) AND (hotel_id#70275L = cast(id#70282 as bigint))) AND ((wthr_date#70286 = checkin#70254) OR (wthr_date#70286 = checkout#70255)))
                     :- AggregatePart [address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, wthr_date#70286], false
                     :  +- AggregatePart [address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, wthr_date#70286], false
                     :     +- Filter (isnotnull(wthr_date#70286) AND isnotnull(id#70282))
                     :        +- Relation default.hotel_weather_silver[address#70276,avg_tmpr_c#70277,avg_tmpr_f#70278,city#70279,country#70280,geoHash#70281,id#70282,latitude#70283,longitude#70284,name#70285,wthr_date#70286,year#70287,month#70288,day#70289] parquet
                     +- AggregatePart [checkin#70254, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], false
                        +- AggregatePart [checkin#70254, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], false
                           +- Project [id#70256L AS expedia_id#70252L, hotel_id#70275L, srch_ci#70268 AS checkin#70254, srch_co#70269 AS checkout#70255]
                              +- Filter (((isnotnull(srch_co#70269) AND isnotnull(srch_ci#70268)) AND (datediff(cast(srch_co#70269 as date), cast(srch_ci#70268 as date)) > 7)) AND (isnotnull(hotel_id#70275L) AND (isnotnull(srch_ci#70268) OR (srch_ci#70268 = srch_co#70269))))
                                 +- Relation default.expedia_silver[id#70256L,date_time#70257,site_name#70258,posa_continent#70259,user_location_country#70260,user_location_region#70261,user_location_city#70262,orig_destination_distance#70263,user_id#70264,is_mobile#70265,is_package#70266,channel#70267,srch_ci#70268,srch_co#70269,srch_adults_cnt#70270,srch_children_cnt#70271,srch_rm_cnt#70272,srch_destination_id#70273,srch_destination_type_id#70274,hotel_id#70275L] parquet

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[expedia_id#70098L, hotel_id#70141L, checkout#70101, checkin#70100, weather_trend#70076], functions=[finalmerge_avg(merge sum#70303, count#70304L) AS avg(avg_tmpr_c#70157)#70292], output=[expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, duration#70048, weather_trend#70076, average_tmpr#70049])
   +- Exchange hashpartitioning(expedia_id#70098L, hotel_id#70141L, checkout#70101, checkin#70100, weather_trend#70076, 200), ENSURE_REQUIREMENTS, [id=#109691]
      +- HashAggregate(keys=[expedia_id#70098L, hotel_id#70141L, checkout#70101, checkin#70100, knownfloatingpointnormalized(normalizenanandzero(weather_trend#70076)) AS weather_trend#70076], functions=[partial_avg(avg_tmpr_c#70157) AS (sum#70303, count#70304L)], output=[expedia_id#70098L, hotel_id#70141L, checkout#70101, checkin#70100, weather_trend#70076, sum#70303, count#70304L])
         +- Project [avg_tmpr_c#70157, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, round((avg(avg_tmpr_c)#70230 - avg(avg_tmpr_c)#70291), 2) AS weather_trend#70076]
            +- SortMergeJoin [id#70162], [id#70282], LeftOuter
               :- Project [avg_tmpr_c#70157, id#70162, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, avg(avg_tmpr_c)#70230]
               :  +- SortMergeJoin [id#70162], [id#70221], LeftOuter
               :     :- Sort [id#70162 ASC NULLS FIRST], false, 0
               :     :  +- Exchange hashpartitioning(id#70162, 200), ENSURE_REQUIREMENTS, [id=#109668]
               :     :     +- Project [avg_tmpr_c#70157, id#70162, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101]
               :     :        +- HashAggregate(keys=[address#70156, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70157)) AS avg_tmpr_c#70157, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70158)) AS avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, knownfloatingpointnormalized(normalizenanandzero(latitude#70163)) AS latitude#70163, knownfloatingpointnormalized(normalizenanandzero(longitude#70164)) AS longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101], functions=[], output=[address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101])
               :     :           +- Exchange hashpartitioning(address#70156, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70157)) AS avg_tmpr_c#70157, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70158)) AS avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, knownfloatingpointnormalized(normalizenanandzero(latitude#70163)) AS latitude#70163, knownfloatingpointnormalized(normalizenanandzero(longitude#70164)) AS longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101, 200), ENSURE_REQUIREMENTS, [id=#109652]
               :     :              +- HashAggregate(keys=[address#70156, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70157)) AS avg_tmpr_c#70157, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70158)) AS avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, knownfloatingpointnormalized(normalizenanandzero(latitude#70163)) AS latitude#70163, knownfloatingpointnormalized(normalizenanandzero(longitude#70164)) AS longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101], functions=[], output=[address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169, expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101])
               :     :                 +- BroadcastHashJoin [cast(id#70162 as bigint)], [hotel_id#70141L], Inner, BuildLeft, (((wthr_date#70166 >= checkin#70100) AND (wthr_date#70166 <= checkout#70101)) AND ((wthr_date#70166 = checkin#70100) OR (wthr_date#70166 = checkout#70101))), false
               :     :                    :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[6, string, true] as bigint)),false), [id=#109648]
               :     :                    :  +- HashAggregate(keys=[address#70156, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70157)) AS avg_tmpr_c#70157, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70158)) AS avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, knownfloatingpointnormalized(normalizenanandzero(latitude#70163)) AS latitude#70163, knownfloatingpointnormalized(normalizenanandzero(longitude#70164)) AS longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169], functions=[], output=[address#70156, avg_tmpr_c#70157, avg_tmpr_f#70158, city#70159, country#70160, geoHash#70161, id#70162, latitude#70163, longitude#70164, name#70165, wthr_date#70166, year#70167, month#70168, day#70169])
               :     :                    :     +- Filter (isnotnull(wthr_date#70166) AND isnotnull(id#70162))
               :     :                    :        +- FileScan parquet default.hotel_weather_silver[address#70156,avg_tmpr_c#70157,avg_tmpr_f#70158,city#70159,country#70160,geoHash#70161,id#70162,latitude#70163,longitude#70164,name#70165,wthr_date#70166,year#70167,month#70168,day#70169] Batched: true, DataFilters: [isnotnull(wthr_date#70166), isnotnull(id#70162)], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/hotel-w..., PartitionFilters: [], PushedFilters: [IsNotNull(wthr_date), IsNotNull(id)], ReadSchema: struct<address:string,avg_tmpr_c:double,avg_tmpr_f:double,city:string,country:string,geoHash:stri...
               :     :                    +- HashAggregate(keys=[expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101], functions=[], output=[expedia_id#70098L, hotel_id#70141L, checkin#70100, checkout#70101])
               :     :                       +- Project [id#70122L AS expedia_id#70098L, hotel_id#70141L, srch_ci#70134 AS checkin#70100, srch_co#70135 AS checkout#70101]
               :     :                          +- Filter (((isnotnull(srch_co#70135) AND isnotnull(srch_ci#70134)) AND (datediff(cast(srch_co#70135 as date), cast(srch_ci#70134 as date)) > 7)) AND isnotnull(hotel_id#70141L))
               :     :                             +- FileScan parquet default.expedia_silver[id#70122L,srch_ci#70134,srch_co#70135,hotel_id#70141L] Batched: true, DataFilters: [isnotnull(srch_co#70135), isnotnull(srch_ci#70134), (datediff(cast(srch_co#70135 as date), cast(..., Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/expedia], PartitionFilters: [], PushedFilters: [IsNotNull(srch_co), IsNotNull(srch_ci), IsNotNull(hotel_id)], ReadSchema: struct<id:bigint,srch_ci:string,srch_co:string,hotel_id:bigint>
               :     +- Sort [id#70221 ASC NULLS FIRST], false, 0
               :        +- HashAggregate(keys=[id#70221], functions=[finalmerge_avg(merge sum#70295, count#70296L) AS avg(avg_tmpr_c#70216)#70229], output=[avg(avg_tmpr_c)#70230, id#70221])
               :           +- Exchange hashpartitioning(id#70221, 200), ENSURE_REQUIREMENTS, [id=#109664]
               :              +- HashAggregate(keys=[id#70221], functions=[partial_avg(avg_tmpr_c#70216) AS (sum#70295, count#70296L)], output=[id#70221, sum#70295, count#70296L])
               :                 +- Project [avg_tmpr_c#70216, id#70221]
               :                    +- HashAggregate(keys=[address#70215, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70216)) AS avg_tmpr_c#70216, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70217)) AS avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, knownfloatingpointnormalized(normalizenanandzero(latitude#70222)) AS latitude#70222, knownfloatingpointnormalized(normalizenanandzero(longitude#70223)) AS longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], functions=[], output=[address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194])
               :                       +- Exchange hashpartitioning(address#70215, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70216)) AS avg_tmpr_c#70216, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70217)) AS avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, knownfloatingpointnormalized(normalizenanandzero(latitude#70222)) AS latitude#70222, knownfloatingpointnormalized(normalizenanandzero(longitude#70223)) AS longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194, 200), ENSURE_REQUIREMENTS, [id=#109659]
               :                          +- HashAggregate(keys=[address#70215, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70216)) AS avg_tmpr_c#70216, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70217)) AS avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, knownfloatingpointnormalized(normalizenanandzero(latitude#70222)) AS latitude#70222, knownfloatingpointnormalized(normalizenanandzero(longitude#70223)) AS longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], functions=[], output=[address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194])
               :                             +- BroadcastHashJoin [wthr_date#70225, cast(id#70221 as bigint)], [checkout#70194, hotel_id#70214L], Inner, BuildLeft, ((wthr_date#70225 = checkin#70193) OR (wthr_date#70225 = checkout#70194)), false
               :                                :- BroadcastExchange HashedRelationBroadcastMode(ArrayBuffer(input[10, string, true], cast(input[6, string, true] as bigint)),false), [id=#109655]
               :                                :  +- HashAggregate(keys=[address#70215, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70216)) AS avg_tmpr_c#70216, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70217)) AS avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, knownfloatingpointnormalized(normalizenanandzero(latitude#70222)) AS latitude#70222, knownfloatingpointnormalized(normalizenanandzero(longitude#70223)) AS longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, wthr_date#70225], functions=[], output=[address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, wthr_date#70225])
               :                                :     +- HashAggregate(keys=[address#70215, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70216)) AS avg_tmpr_c#70216, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70217)) AS avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, knownfloatingpointnormalized(normalizenanandzero(latitude#70222)) AS latitude#70222, knownfloatingpointnormalized(normalizenanandzero(longitude#70223)) AS longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, wthr_date#70225], functions=[], output=[address#70215, avg_tmpr_c#70216, avg_tmpr_f#70217, city#70218, country#70219, geoHash#70220, id#70221, latitude#70222, longitude#70223, name#70224, wthr_date#70225, year#70226, month#70227, day#70228, wthr_date#70225])
               :                                :        +- Filter (isnotnull(wthr_date#70225) AND isnotnull(id#70221))
               :                                :           +- FileScan parquet default.hotel_weather_silver[address#70215,avg_tmpr_c#70216,avg_tmpr_f#70217,city#70218,country#70219,geoHash#70220,id#70221,latitude#70222,longitude#70223,name#70224,wthr_date#70225,year#70226,month#70227,day#70228] Batched: true, DataFilters: [isnotnull(wthr_date#70225), isnotnull(id#70221)], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/hotel-w..., PartitionFilters: [], PushedFilters: [IsNotNull(wthr_date), IsNotNull(id)], ReadSchema: struct<address:string,avg_tmpr_c:double,avg_tmpr_f:double,city:string,country:string,geoHash:stri...
               :                                +- HashAggregate(keys=[checkout#70194, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], functions=[], output=[checkout#70194, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194])
               :                                   +- HashAggregate(keys=[checkout#70194, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194], functions=[], output=[checkout#70194, expedia_id#70191L, hotel_id#70214L, checkin#70193, checkout#70194])
               :                                      +- Project [id#70195L AS expedia_id#70191L, hotel_id#70214L, srch_ci#70207 AS checkin#70193, srch_co#70208 AS checkout#70194]
               :                                         +- Filter ((((isnotnull(srch_co#70208) AND isnotnull(srch_ci#70207)) AND (datediff(cast(srch_co#70208 as date), cast(srch_ci#70207 as date)) > 7)) AND isnotnull(hotel_id#70214L)) AND ((srch_co#70208 = srch_ci#70207) OR isnotnull(srch_co#70208)))
               :                                            +- FileScan parquet default.expedia_silver[id#70195L,srch_ci#70207,srch_co#70208,hotel_id#70214L] Batched: true, DataFilters: [isnotnull(srch_co#70208), isnotnull(srch_ci#70207), (datediff(cast(srch_co#70208 as date), cast(..., Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/expedia], PartitionFilters: [], PushedFilters: [IsNotNull(srch_co), IsNotNull(srch_ci), IsNotNull(hotel_id)], ReadSchema: struct<id:bigint,srch_ci:string,srch_co:string,hotel_id:bigint>
               +- Sort [id#70282 ASC NULLS FIRST], false, 0
                  +- HashAggregate(keys=[id#70282], functions=[finalmerge_avg(merge sum#70299, count#70300L) AS avg(avg_tmpr_c#70277)#70290], output=[avg(avg_tmpr_c)#70291, id#70282])
                     +- Exchange hashpartitioning(id#70282, 200), ENSURE_REQUIREMENTS, [id=#109682]
                        +- HashAggregate(keys=[id#70282], functions=[partial_avg(avg_tmpr_c#70277) AS (sum#70299, count#70300L)], output=[id#70282, sum#70299, count#70300L])
                           +- Project [avg_tmpr_c#70277, id#70282]
                              +- HashAggregate(keys=[address#70276, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70277)) AS avg_tmpr_c#70277, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70278)) AS avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, knownfloatingpointnormalized(normalizenanandzero(latitude#70283)) AS latitude#70283, knownfloatingpointnormalized(normalizenanandzero(longitude#70284)) AS longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], functions=[], output=[address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255])
                                 +- Exchange hashpartitioning(address#70276, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70277)) AS avg_tmpr_c#70277, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70278)) AS avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, knownfloatingpointnormalized(normalizenanandzero(latitude#70283)) AS latitude#70283, knownfloatingpointnormalized(normalizenanandzero(longitude#70284)) AS longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255, 200), ENSURE_REQUIREMENTS, [id=#109677]
                                    +- HashAggregate(keys=[address#70276, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70277)) AS avg_tmpr_c#70277, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70278)) AS avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, knownfloatingpointnormalized(normalizenanandzero(latitude#70283)) AS latitude#70283, knownfloatingpointnormalized(normalizenanandzero(longitude#70284)) AS longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], functions=[], output=[address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255])
                                       +- BroadcastHashJoin [wthr_date#70286, cast(id#70282 as bigint)], [checkin#70254, hotel_id#70275L], Inner, BuildLeft, ((wthr_date#70286 = checkin#70254) OR (wthr_date#70286 = checkout#70255)), false
                                          :- BroadcastExchange HashedRelationBroadcastMode(ArrayBuffer(input[10, string, true], cast(input[6, string, true] as bigint)),false), [id=#109673]
                                          :  +- HashAggregate(keys=[address#70276, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70277)) AS avg_tmpr_c#70277, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70278)) AS avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, knownfloatingpointnormalized(normalizenanandzero(latitude#70283)) AS latitude#70283, knownfloatingpointnormalized(normalizenanandzero(longitude#70284)) AS longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, wthr_date#70286], functions=[], output=[address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, wthr_date#70286])
                                          :     +- HashAggregate(keys=[address#70276, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_c#70277)) AS avg_tmpr_c#70277, knownfloatingpointnormalized(normalizenanandzero(avg_tmpr_f#70278)) AS avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, knownfloatingpointnormalized(normalizenanandzero(latitude#70283)) AS latitude#70283, knownfloatingpointnormalized(normalizenanandzero(longitude#70284)) AS longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, wthr_date#70286], functions=[], output=[address#70276, avg_tmpr_c#70277, avg_tmpr_f#70278, city#70279, country#70280, geoHash#70281, id#70282, latitude#70283, longitude#70284, name#70285, wthr_date#70286, year#70287, month#70288, day#70289, wthr_date#70286])
                                          :        +- Filter (isnotnull(wthr_date#70286) AND isnotnull(id#70282))
                                          :           +- FileScan parquet default.hotel_weather_silver[address#70276,avg_tmpr_c#70277,avg_tmpr_f#70278,city#70279,country#70280,geoHash#70281,id#70282,latitude#70283,longitude#70284,name#70285,wthr_date#70286,year#70287,month#70288,day#70289] Batched: true, DataFilters: [isnotnull(wthr_date#70286), isnotnull(id#70282)], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/hotel-w..., PartitionFilters: [], PushedFilters: [IsNotNull(wthr_date), IsNotNull(id)], ReadSchema: struct<address:string,avg_tmpr_c:double,avg_tmpr_f:double,city:string,country:string,geoHash:stri...
                                          +- HashAggregate(keys=[checkin#70254, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], functions=[], output=[checkin#70254, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255])
                                             +- HashAggregate(keys=[checkin#70254, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255], functions=[], output=[checkin#70254, expedia_id#70252L, hotel_id#70275L, checkin#70254, checkout#70255])
                                                +- Project [id#70256L AS expedia_id#70252L, hotel_id#70275L, srch_ci#70268 AS checkin#70254, srch_co#70269 AS checkout#70255]
                                                   +- Filter ((((isnotnull(srch_co#70269) AND isnotnull(srch_ci#70268)) AND (datediff(cast(srch_co#70269 as date), cast(srch_ci#70268 as date)) > 7)) AND isnotnull(hotel_id#70275L)) AND (isnotnull(srch_ci#70268) OR (srch_ci#70268 = srch_co#70269)))
                                                      +- FileScan parquet default.expedia_silver[id#70256L,srch_ci#70268,srch_co#70269,hotel_id#70275L] Batched: true, DataFilters: [isnotnull(srch_co#70269), isnotnull(srch_ci#70268), (datediff(cast(srch_co#70269 as date), cast(..., Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[abfss://data@stsafrwesteurope.dfs.core.windows.net/silver/expedia], PartitionFilters: [], PushedFilters: [IsNotNull(srch_co), IsNotNull(srch_ci), IsNotNull(hotel_id)], ReadSchema: struct<id:bigint,srch_ci:string,srch_co:string,hotel_id:bigint>




* Reading the file and analyzing it
* Filtering based on the criteria
* Selecting columns and doing a shuffle operation
* At the same time reading data from another table and filtering based on the criteria
* Doing a filter and explosion on some columns, then selecting certain columns
* Joining both tables and doing a shuffle for sort

![](screenshots/res1.png)
![](screenshots/res2.png)
