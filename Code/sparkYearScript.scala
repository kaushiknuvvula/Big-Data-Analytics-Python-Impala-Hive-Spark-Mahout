// spark-shell --packages com.databricks:spark-csv_2.10:1.1.0
// export PATH=$PATH:/usr/local/spark/bin
// source ~/.bashrc


val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val file = "Scorecard.csv"

sqlContext.load("com.databricks.spark.csv", Map("path" -> file, "header" -> "true")).registerTempTable("scorecard")

// Sample Results
val dataset = sqlContext.sql("SELECT INSTNM,TUITIONFEE_IN, SAT_AVG, ADM_RATE, AVGFACSAL, RPY_3YR_RT from scorecard WHERE Year = 2013 AND INSTNM IN ('California Institute of Technology', 'Massachusetts Institute of Technology', 'Harvard University', 'Princeton University', 'Stanford University')")


// SAT Score
val dataset = sqlContext.sql("SELECT INSTNM, cast(SAT_AVG as FLOAT) SAT from scorecard WHERE Year = 2013 AND cast (SAT_AVG as FLOAT) > 0 ORDER BY SAT DESC").save("SATData2013", "com.databricks.spark.csv")


// Admission Rate
val dataset = sqlContext.sql("SELECT INSTNM, cast(ADM_RATE as FLOAT) AS ADMISSION_RATE from scorecard WHERE Year = 2013 AND cast(ADM_RATE as FLOAT) > 0 ORDER BY ADMISSION_RATE").save("AdmissionData2013", "com.databricks.spark.csv")


//Tution Fees
val dataset = sqlContext.sql("SELECT INSTNM, cast(TUITIONFEE_IN AS FLOAT) TUITIONFEE from scorecard WHERE Year = 2013 AND cast(TUITIONFEE_IN AS FLOAT) > 0 ORDER BY TUITIONFEE DESC").save("TutionFees2013", "com.databricks.spark.csv")

// Average Faculty Salary 
val dataset = sqlContext.sql("SELECT INSTNM, cast(AVGFACSAL AS FLOAT) FACULTY_SALARY from scorecard WHERE Year = 2013 AND cast(AVGFACSAL AS FLOAT) > 0 ORDER BY FACULTY_SALARY DESC").save("FacultySalary2013", "com.databricks.spark.csv")

// Earnings
val dataset = sqlContext.sql("SELECT INSTNM, cast(pct75_earn_wne_p10 AS FLOAT) EARNINGS from scorecard WHERE Year = 2011 AND cast(pct75_earn_wne_p10 AS FLOAT) > 0 AND PREDDEG like 'Predominantly bachelo%degree granting' ORDER BY EARNINGS DESC").save("Earnings2013", "com.databricks.spark.csv")

// Repayment Rate
val dataset = sqlContext.sql("SELECT INSTNM, cast(RPY_3YR_RT AS FLOAT) REPAYMENT from scorecard WHERE Year = 2013 AND cast(RPY_3YR_RT AS FLOAT) > 0 AND PREDDEG like 'Predominantly bachelo%degree granting' ORDER BY REPAYMENT DESC").save("Repayment2013", "com.databricks.spark.csv")

exit()
