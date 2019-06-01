/*
How to use the na function to fill necessary data
*/
/*
The csv file looks like this
Year,Quarter,MonthNo,MonthName,MonthID,Date
2016,Q1,1,Jan,201601,1/1/2016
,,,,,1/2/2016
,,,,,1/3/2016
*/

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val dateDimSchema2 = StructType(List(StructField("Year", StringType, true),StructField("Quarter", StringType, true),StructField("MonthNo", StringType, true),StructField("MonthName", StringType, true),StructField("MonthID", StringType, true),StructField("Date", StringType, true)))


//set the file location
val dateFileLoc = "D:/ProjecT/github/apache-spark/input-data/2019_date_dim.csv"

//read the date dimention
val dateDf = spark.read.format("csv").schema(dateDimSchema2).option("header","true").load(dateFileLoc)

dateDf.show
/*
+----+-------+-------+---------+-------+---------+
|Year|Quarter|MonthNo|MonthName|MonthID|     Date|
+----+-------+-------+---------+-------+---------+
|2016|     Q1|      1|      Jan| 201601| 1/1/2016|
|null|   null|   null|     null|   null| 1/2/2016|
|null|   null|   null|     null|   null| 1/3/2016|
|null|   null|   null|     null|   null| 1/4/2016|
|null|   null|   null|     null|   null| 1/5/2016|
|null|   null|   null|     null|   null| 1/6/2016|
|null|   null|   null|     null|   null| 1/7/2016|
|null|   null|   null|     null|   null| 1/8/2016|
|null|   null|   null|     null|   null| 1/9/2016|
|null|   null|   null|     null|   null|1/10/2016|
|null|   null|   null|     null|   null|1/11/2016|
|null|   null|   null|     null|   null|1/12/2016|
|null|   null|   null|     null|   null|1/13/2016|
|null|   null|   null|     null|   null|1/14/2016|
|null|   null|   null|     null|   null|1/15/2016|
|null|   null|   null|     null|   null|1/16/2016|
|null|   null|   null|     null|   null|1/17/2016|
|null|   null|   null|     null|   null|1/18/2016|
|null|   null|   null|     null|   null|1/19/2016|
|null|   null|   null|     null|   null|1/20/2016|
+----+-------+-------+---------+-------+---------+
only showing top 20 rows
*/

val dateDfFill = dateDf.na.fill("2016",Seq("Year"))
dateDfFill.show

/*
scala> dateDfFill.show
+----+-------+-------+---------+-------+---------+
|Year|Quarter|MonthNo|MonthName|MonthID|     Date|
+----+-------+-------+---------+-------+---------+
|2016|     Q1|      1|      Jan| 201601| 1/1/2016|
|2016|   null|   null|     null|   null| 1/2/2016|
|2016|   null|   null|     null|   null| 1/3/2016|
|2016|   null|   null|     null|   null| 1/4/2016|
|2016|   null|   null|     null|   null| 1/5/2016|
|2016|   null|   null|     null|   null| 1/6/2016|
|2016|   null|   null|     null|   null| 1/7/2016|
|2016|   null|   null|     null|   null| 1/8/2016|
|2016|   null|   null|     null|   null| 1/9/2016|
|2016|   null|   null|     null|   null|1/10/2016|
|2016|   null|   null|     null|   null|1/11/2016|
|2016|   null|   null|     null|   null|1/12/2016|
|2016|   null|   null|     null|   null|1/13/2016|
|2016|   null|   null|     null|   null|1/14/2016|
|2016|   null|   null|     null|   null|1/15/2016|
|2016|   null|   null|     null|   null|1/16/2016|
|2016|   null|   null|     null|   null|1/17/2016|
|2016|   null|   null|     null|   null|1/18/2016|
|2016|   null|   null|     null|   null|1/19/2016|
|2016|   null|   null|     null|   null|1/20/2016|
+----+-------+-------+---------+-------+---------+
only showing top 20 rows
*/

//it fills only those cells which are empty
val df2019 = dateDf.na.fill("2019",Seq("Year"))
df2019.show
/*
+----+-------+-------+---------+-------+---------+
|Year|Quarter|MonthNo|MonthName|MonthID|     Date|
+----+-------+-------+---------+-------+---------+
|2016|     Q1|      1|      Jan| 201601| 1/1/2016|
|2019|   null|   null|     null|   null| 1/2/2016|
|2019|   null|   null|     null|   null| 1/3/2016|
|2019|   null|   null|     null|   null| 1/4/2016|
|2019|   null|   null|     null|   null| 1/5/2016|
|2019|   null|   null|     null|   null| 1/6/2016|
|2019|   null|   null|     null|   null| 1/7/2016|
|2019|   null|   null|     null|   null| 1/8/2016|
|2019|   null|   null|     null|   null| 1/9/2016|
|2019|   null|   null|     null|   null|1/10/2016|
|2019|   null|   null|     null|   null|1/11/2016|
|2019|   null|   null|     null|   null|1/12/2016|
|2019|   null|   null|     null|   null|1/13/2016|
|2019|   null|   null|     null|   null|1/14/2016|
|2019|   null|   null|     null|   null|1/15/2016|
|2019|   null|   null|     null|   null|1/16/2016|
|2019|   null|   null|     null|   null|1/17/2016|
|2019|   null|   null|     null|   null|1/18/2016|
|2019|   null|   null|     null|   null|1/19/2016|
|2019|   null|   null|     null|   null|1/20/2016|
+----+-------+-------+---------+-------+---------+
*/