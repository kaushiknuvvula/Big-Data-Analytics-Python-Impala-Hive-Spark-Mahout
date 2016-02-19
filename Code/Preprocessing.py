import csv
fw = open('Scorecard.csv','w')
Id = 0
for year in range(1996,2014):
	f = open('MERGED' + str(year) + '_PP.csv')
	count = 0
	for row in csv.reader(f):
		if count > 0:
			row.append(str(year))
			result = ','.join(row)
			fw.write(result + '\n')
		count += 1
	print str(year) + ' finished with ' + str(count) + ' lines'

fw.close()
