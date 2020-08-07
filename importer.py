import MySQLdb
import pathlib
import csv

db = MySQLdb.connect(user="root", passwd="WSBDDmysqlrootytooty",db="wsbdd")
c = db.cursor()

files = ['amex.csv', 'nasdaq.csv', 'nyse.csv']

curdir = pathlib.Path(__file__).parent.absolute()

listed = []

for fname in files:
    with open(str(curdir) + '/exchanges/'+ fname) as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)
        
        for row in reader:
            if row[0] not in listed:
                listed.append(row[0])
                c.execute("INSERT INTO symbols (symbol, company, sector, industry) VALUES (%s, %s, %s, %s)", (row[0], row[1], row[5], row[6]))

db.commit()