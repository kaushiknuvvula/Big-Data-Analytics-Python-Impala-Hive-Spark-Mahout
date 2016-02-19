// spark-shell --packages com.databricks:spark-csv_2.10:1.1.0
// export PATH=$PATH:/usr/local/spark/bin
// source ~/.bashrc


val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val file = "Scorecard.csv"

sqlContext.load("com.databricks.spark.csv", Map("path" -> file, "header" -> "true")).registerTempTable("scorecard")

// **  STATE WISE ** 

// SAT Score
val dataset = sqlContext.sql("SELECT STABBR, AVG(cast(SAT_AVG as FLOAT)) SAT from scorecard WHERE Year = 2013 AND cast (SAT_AVG as FLOAT) > 0 GROUP BY STABBR ORDER BY SAT DESC").save("SATData2013_StateWise", "com.databricks.spark.csv")

// Number of Universities per State
val dataset = sqlContext.sql("SELECT STABBR, COUNT(INSTNM) AS NO_OF_COLLEGES from scorecard WHERE Year = 2013 GROUP BY STABBR ORDER BY NO_OF_COLLEGES DESC").save("Universities2013_StateWise", "com.databricks.spark.csv")


//CDR3
val dataset = sqlContext.sql("SELECT STABBR, AVG(cast(CDR3 as FLOAT)) AS COHORT from scorecard WHERE Year = 2013 AND cast(CDR3 as FLOAT) > 0 GROUP BY STABBR ORDER BY COHORT DESC").save("CDR2013_StateWise", "com.databricks.spark.csv")

// DEBT_MDN
val dataset = sqlContext.sql("SELECT STABBR, AVG(cast(DEBT_MDN as FLOAT)) AS DEBT from scorecard WHERE Year = 2013 AND cast(DEBT_MDN as FLOAT) > 0 GROUP BY STABBR ORDER BY DEBT DESC").save("Debt2013_StateWise", "com.databricks.spark.csv")

// Repayment Rate
val dataset = sqlContext.sql("SELECT STABBR, AVG(cast(RPY_3YR_RT as FLOAT)) AS REPAYMENT from scorecard WHERE Year = 2013 AND cast(RPY_3YR_RT as FLOAT) GROUP BY STABBR ORDER BY REPAYMENT DESC").save("Repayment2013_StateWise", "com.databricks.spark.csv")

