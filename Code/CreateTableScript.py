import re

with open('Scorecard.csv') as f:
	line = f.readline()
line = re.sub(',',' STRING, ', line)
line = line.strip()
line = "CREATE TABLE IF NOT EXISTS scorecard (" + line + " STRING)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"

f = open('scorecard.sql', 'w')
f.write(line)
f.close()  
