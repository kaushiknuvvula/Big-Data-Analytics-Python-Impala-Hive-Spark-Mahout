// spark-shell --packages com.databricks:spark-csv_2.10:1.1.0
// export PATH=$PATH:/usr/local/spark/bin
// source ~/.bashrc


val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val file = "Scorecard.csv"

sqlContext.load("com.databricks.spark.csv", Map("path" -> file, "header" -> "true")).registerTempTable("scorecard")

// ** Trend Analysis

// SAT Score
val dataset = sqlContext.sql("SELECT UPPER(INSTNM), AVG(cast(SAT_AVG as FLOAT)) AS SAT from scorecard WHERE cast (SAT_AVG as FLOAT) > 0 GROUP BY UPPER(INSTNM) ORDER BY SAT DESC LIMIT 20")save("Top20Universities_SAT_AVG", "com.databricks.spark.csv")

val dataset = sqlContext.sql("SELECT OPEID, AVG(cast(SAT_AVG as FLOAT)) AS SAT from scorecard WHERE cast (SAT_AVG as FLOAT) > 0 GROUP BY OPEID ORDER BY SAT DESC LIMIT 20")save("Top20Universities_SAT_AVG", "com.databricks.spark.csv")

//val dataset = sqlContext.sql("SELECT UPPER(INSTNM), Year, cast(SAT_AVG as FLOAT) SAT from scorecard WHERE cast (SAT_AVG as FLOAT) > 0 AND UPPER(INSTNM) IN ('CALIFORNIA INSTITUTE OF TECHNOLOGY','FRANKLIN W. OLIN COLLEGE OF ENGINEERING','HARVARD UNIVERSITY','HARVEY MUDD COLLEGE','MASSACHUSETTS INSTITUTE OF TECHNOLOGY','YALE UNIVERSITY','PRINCETON UNIVERSITY','FRANKLIN W OLIN COLLEGE OF ENGINEERING','POMONA COLLEGE','COLUMBIA UNIVERSITY IN THE CITY OF NEW YORK','STANFORD UNIVERSITY','DARTMOUTH COLLEGE','WASHINGTON UNIVERSITY IN ST LOUIS','SWARTHMORE COLLEGE','DUKE UNIVERSITY','UNIVERSITY OF CHICAGO','AMHERST COLLEGE','RICE UNIVERSITY','WILLIAMS COLLEGE','UNIVERSITY OF PENNSYLVANIA') AND cast(Year as FLOAT) > 2000")save("Trend_SAT_AVG", "com.databricks.spark.csv")

val dataset = sqlContext.sql("SELECT UPPER(INSTNM), Year, cast(SAT_AVG as FLOAT) SAT from scorecard WHERE cast (SAT_AVG as FLOAT) > 0 AND OPEID IN ('00113100','03946300','00215500','00117100','00217800','00142600','00262700','00117300','00270700','00130500','00257300','00252000','00337000','00292000','00177400','00211500','00360400','00222900','00337800','00173900') AND cast(Year as FLOAT) > 2000")save("Trend_SAT_AVG1", "com.databricks.spark.csv")


// CDR3
val dataset = sqlContext.sql("SELECT UPPER(INSTNM), Year, cast(CDR3 as FLOAT) COHORT from scorecard WHERE cast(CDR3 as FLOAT) > 0 AND OPEID IN ('00113100','03946300','00215500','00117100','00217800','00142600','00262700','00117300','00270700','00130500','00257300','00252000','00337000','00292000','00177400','00211500','00360400','00222900','00337800','00173900') AND cast(Year as FLOAT) > 2000")save("Trend_CDR3", "com.databricks.spark.csv")

// DEBT_MDN
val dataset = sqlContext.sql("SELECT UPPER(INSTNM), Year, cast(DEBT_MDN as FLOAT) DEBT from scorecard WHERE cast(DEBT_MDN as FLOAT) > 0 AND OPEID IN ('00113100','03946300','00215500','00117100','00217800','00142600','00262700','00117300','00270700','00130500','00257300','00252000','00337000','00292000','00177400','00211500','00360400','00222900','00337800','00173900') AND cast(Year as FLOAT) > 2000")save("Trend_DEBT", "com.databricks.spark.csv")

// Repayment Rate
val dataset = sqlContext.sql("SELECT UPPER(INSTNM), Year, cast(RPY_3YR_RT as FLOAT) REPAYMENT from scorecard WHERE cast(RPY_3YR_RT as FLOAT) > 0 AND OPEID IN ('00113100','03946300','00215500','00117100','00217800','00142600','00262700','00117300','00270700','00130500','00257300','00252000','00337000','00292000','00177400','00211500','00360400','00222900','00337800','00173900') AND cast(Year as FLOAT) > 2000")save("Trend_REPAYMENT", "com.databricks.spark.csv")


// Schools per Year
val dataset = sqlContext.sql("SELECT Year, Count(*) AS NumberOfSchools from scorecard GROUP BY Year ORDER BY Year")save("Trend_SchoolsPerYear", "com.databricks.spark.csv")

// Schools per State per Year
val dataset = sqlContext.sql("SELECT Year, STABBR, Count(INSTNM) AS NUMBER_OF_UNIVERSITIES from scorecard WHERE STABBR IN ('CA','TX','NY','FL','PA','OH','IL','MO','MI','NC','MA','GA','TN','VA','NJ','IN','MN','PR','OK','AZ') GROUP BY Year, STABBR ORDER BY Year")save("Trend_StatewiseUniversities", "com.databricks.spark.csv")

// Trend in number of colleges based on Public/Private for Profit/Private for Non Profit
val dataset = sqlContext.sql("SELECT Year, CONTROL, Count(INSTNM) AS NUMBER_OF_UNIVERSITIES from scorecard GROUP BY Year, CONTROL ORDER BY Year")save("Trend_CONTROLwiseUniversities", "com.databricks.spark.csv")

// Trend in receiving PELL Grants based on Public/Private for Profit/Private for Non Profit
//val dataset = sqlContext.sql("SELECT Year, CONTROL, SUM(cast(PCTPELL as FLOAT)) AS PELL_GRANT from scorecard WHERE Year > 2008 AND cast(PCTPELL as FLOAT) > 0 GROUP BY Year, CONTROL ORDER BY Year")save("Trend_PellGrantsPerYear", "com.databricks.spark.csv")

val dataset = sqlContext.sql("SELECT Year, UPPER(INSTNM), SUM(cast(PCTPELL as FLOAT)) AS PELL_GRANT from scorecard WHERE Year > 2008 AND cast(PCTPELL as FLOAT) > 0 AND OPEID IN ('00113100','03946300','00215500','00117100','00217800','00142600','00262700','00117300','00270700','00130500','00257300','00252000','00337000','00292000','00177400','00211500','00360400','00222900','00337800','00173900') GROUP BY Year, UPPER(INSTNM) ORDER BY Year")save("Trend_PellGrantsPerYear", "com.databricks.spark.csv")

//
