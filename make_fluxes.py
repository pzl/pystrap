import random
import os
from calendar import monthrange

dlist = []

def get_season(year,month):
	return "%s%s" % (y-1,y) if month < 7 else "%s%s" % (y,y+1)

for y in range(1980,2016):
	for m in [1,2,3,4,5,9,10,11,12]:
		season = get_season(y,m)
		if not os.path.isdir("fluxes/%s" % (season,)):
			os.makedirs("fluxes/%s" % (season,))
		for d in range(1,monthrange(y,m)[1] + 1):
			with open("fluxes/%s/%4d%02d%02d00.txt" % (season,y,m,d),"w") as f:
				f.write("%s\n" % (random.uniform(-30,30)))

