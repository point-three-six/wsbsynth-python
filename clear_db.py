import MySQLdb
import pathlib
import csv

db = MySQLdb.connect(user="root", passwd="WSBDDmysqlrootytooty",db="wsbdd")

c = db.cursor()
c.execute("DELETE FROM comments WHERE date <= DATE_SUB(NOW(), INTERVAL 1 DAY)")
db.commit()