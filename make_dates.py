import random
from calendar import monthrange

dlist = []

for x in range(100):
	year = random.randint(1980,2015)
	month = random.choice([1,2,3,4,10,11,12])
	day = random.randint(1,monthrange(year,month)[1])
	hour = random.choice([0,6,12,18])

	dlist.append("%04d%02d%02d%02d" % (year,month,day,hour))

with open("dates.txt","w") as f:
	for d in dlist:
		f.write("a b %s\n" % (d,))