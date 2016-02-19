
-- SAT_AVG
select INSTNM, SAT_AVG from scorecard 
where year = '2013' and SAT_AVG != 'NULL'
order by cast(SAT_AVG as int) desc limit 20;

-- ADM_RATE
select INSTNM, ADM_RATE from scorecard 
where year = '2013' and ADM_RATE != 'NULL' 
order by ADM_RATE desc limit 20;

-- TUITIONFEE_IN
select INSTNM, TUITIONFEE_IN from scorecard 
where year = '2013' and TUITIONFEE_IN != 'NULL' 
order by cast(TUITIONFEE_IN as int) desc limit 20;

-- AVGFACSAL
select INSTNM, AVGFACSAL from scorecard 
where year = '2013' and AVGFACSAL != 'NULL' 
order by cast(AVGFACSAL as int) desc limit 20;

-- RPY_3YR_RT
select INSTNM, RPY_3YR_RT from scorecard 
where year = '2013' and RPY_3YR_RT != 'NULL' and RPY_3YR_RT != 'PrivacySuppressed'
order by RPY_3YR_RT desc limit 20;
