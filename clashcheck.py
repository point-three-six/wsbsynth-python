import pathlib
import csv

files = ['amex.csv', 'nasdaq.csv', 'nyse.csv']

curdir = pathlib.Path(__file__).parent.absolute()

symbols = []
companies = []
collisions = []

for fname in files:
    with open(str(curdir) + '/exchanges/'+ fname) as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)
        
        for row in reader:
            if row[0] not in symbols:
                symbols.append(row[0])
                companies.append(row[1])


with open(str(curdir) + '/exchanges/english.csv') as f:
    reader = csv.reader(f, delimiter=',')

    for row in reader:
        if row[0].upper() in symbols:
            company = companies[symbols.index(row[0].upper())]
            collisions.append((row[0], company))

with open(str(curdir) + '/collisions.csv', 'w') as f:
    for collision, company in collisions:
        f.write("\""+collision.upper() + "\",\"" + company + "\",\"\"")
        f.write('\n')