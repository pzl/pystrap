#!/usr/bin/env python2.7

import os, sys
import math
import random
import datetime, time
import argparse
from multiprocessing import Pool, cpu_count

import numpy
import netCDF4

###### Command line options
verbose=False
replacement=False
samples=52

def arguments():
	global verbose
	global replacement
	global samples
	parser = argparse.ArgumentParser(description="bootstraps some research stuff")
	parser.add_argument('-v','--verbose',action='store_true')
	parser.add_argument('-n','--cases',action='store',type=int,default=10000)
	parser.add_argument('-r','--replacement',action='store_true')
	parser.add_argument('-s','--samples',action='store',type=int,default=52)

	args = parser.parse_args()
	verbose = args.verbose
	replacement = args.replacement
	samples = args.samples

	answer = raw_input("%d cases of %d samples, with%s replacement. Continue? Y/n:  " % ( args.cases, samples, 'out' if not replacement else '' )).lower()
	if answer == '' or answer == 'y' or answer == 'yes':
		return args.cases
	else:
		sys.exit()


def load_inits():
	load_dates()
	load_climo_vt()

date_list = []
def load_dates():
	global date_list
	with open("dates.txt","r") as f:
		date_list = f.read().splitlines()
	date_list = map(clean_dates, date_list) # remove columns
	log("Process %d loaded date list",os.getpid())

climo = None
def load_climo_vt():
	global climo
	climo = netCDF4.Dataset("vt.mean.anl.nc")
	log("Process %d loaded climo",os.getpid())


def clean_dates(line):
	return line.split(' ')[2]


def spawn_cases(n):
	log("Spawning %d processes",cpu_count()*2)
	pool = Pool( cpu_count()*2, load_inits ) # load the date list into each process at the start
	pool.map(single_sample, range(n))

def single_sample(i):
	global replacement
	global samples
	rand_dates = get_dates(samples,replacement)
	log("date list: %s",rand_dates)
	averages = []
	for date_offset in range(-30,31): # [a,b)
		offset_dates = make_offset(rand_dates, date_offset)
		log("%d for offset %d, dates: %s", os.getpid(), date_offset, offset_dates)
		fluxes = numpy.array(map(calc_flux, offset_dates))
		averages.append(fluxes.mean())
	with open("sample-%06d.txt" % (i,),"w") as f:
		for line in averages:
			f.write("%s\n" % (line,))

	log("iteration %d has averages: %s",i,averages)

def get_dates(N=52, replace=False):
	global date_list
	numpy.random.seed( int(str(time.time()-int(time.time())).split('.')[1][:7]) + os.getpid() )
	return numpy.random.choice(date_list, size=N, replace=replace)

def make_offset(dates, offset):
	return [ datetime.datetime.strptime(d,"%Y%m%d%H") + datetime.timedelta(days=offset) for d in dates ]

def calc_flux(date):
	global climo
	season = find_season(date) # get season in file path format, waiting for t/v replacement
	doy = int(date.strftime("%j")) # climo time is indexed by day of year 1-366

	log("calculating flux. Season: %s, DOY: %s",season,doy)

	# get climo indexes, at lev 100, and  45 <= lat <= 75
	climo_lev = numpy.where( climo.variables['lev'][:] == 100 )[0][0]
	climo_lat_indices = numpy.where(
							numpy.logical_and(
								climo.variables['lat'][:] >= 45,
								climo.variables['lat'][:] <= 75
							)
						)[0]

	log("got climo indices to use. DOY: %s, lev: %s, lats: %s",doy,climo_lev,climo_lat_indices)

	log("Accessing climo data")
	# Access the climo data we need
	filtered_climo = climo.variables['vt'][doy, climo_lev, climo_lat_indices]

	log("calculating weighted climo mean")

	# use cos(lat) as weighted average for the climo data
	lats = climo.variables['lat'][climo_lat_indices]
	lat_rads = lats * (math.atan(1.0)/45.0)
	lat_cos = numpy.cos(lat_rads)
	weighted_mean_climo = numpy.average( filtered_climo, axis=None,  weights=lat_cos)

	log("accessing merid wind and temp data at: %s and %s","/langlab_rit/hattard/merra2/%s" % (season % ("v",),),"/langlab_rit/hattard/merra2/%s" % (season % ("t",),))

	meridional_wind = netCDF4.Dataset("/langlab_rit/hattard/merra2/%s" % (season % ("v",),))
	temp = netCDF4.Dataset("/langlab_rit/hattard/merra2/%s" % (season % ("t",),))


	# V time index is YYYYMMDDHH, we are going to look for hours 0,6,12,18
	time_fmt = "%04d%02d%02d%%02d" % (date.year,date.month,date.day)
	log("Looking for time_FMT %s",time_fmt)
	vt_time_indices = numpy.where(
						numpy.logical_and(
							meridional_wind.variables['time'][:] >= int(time_fmt % (0,)),
							meridional_wind.variables['time'][:] <= int(time_fmt % (18,))
						)
					)[0]
	vt_lev = numpy.where( meridional_wind.variables['lev'][:] == 100 )[0][0]
	vt_lat_indices = numpy.where(
						numpy.logical_and(
							meridional_wind.variables['lat'][:] >= 45,
							meridional_wind.variables['lat'][:] <= 75
						)
					)[0]

	log("vt_time_indices: %s",vt_time_indices)
	log("vt_lev: %s",vt_lev)
	log("vt_lat_indices: %s",vt_lat_indices)


	# Read V and T data at the times, lev 100, lat range, and all lons
	v = meridional_wind.variables['v'][vt_time_indices, vt_lev, vt_lat_indices, :]
	t = temp.variables['t'][vt_time_indices, vt_lev, vt_lat_indices, :]

	log("performing v and t averages")
	# averaged across times
	v_time_avg = numpy.average(v,0)
	t_time_avg = numpy.average(t,0)

	# also averaged across long
	v_tlon_avg = numpy.average(v_time_avg,1)
	t_tlon_avg = numpy.average(t_time_avg,1)

	log("Subtracting averages")

	# subtract the lon averages like so: http://stackoverflow.com/questions/33303348/numpy-subtract-add-1d-array-from-2d-array
	v_zon_anomaly = v_time_avg - v_tlon_avg[:,None]
	t_zon_anomaly = t_time_avg - t_tlon_avg[:,None]

	# multiply individual elements
	vt_zon_anomaly = v_zon_anomaly * t_zon_anomaly

	# average across lons again
	vt_zon_anomaly_lon_avg = numpy.average(vt_zon_anomaly,1)

	log("Getting weighted zon anom")

	# get weighted mean for lats for vt zon anomaly
	vt_lats = meridional_wind.variables['lat'][vt_lat_indices]
	vt_lats_rads = vt_lats * (math.atan(1.0)/45.0)
	vt_lats_cos = numpy.cos(vt_lats_rads)
	weighted_mean_vt_zon_anomaly = numpy.average( vt_zon_anomaly_lon_avg, axis=None, weights=vt_lats_cos )

	heat_flux_anom = weighted_mean_vt_zon_anomaly - weighted_mean_climo

	log("for date %s, got value %s",date,heat_flux_anom)

	return heat_flux_anom



def find_season(date):
	name = "%s%s" % (date.year-1,date.year) if date.month < 7 else "%s%s" % (date.year,date.year+1)
	return "%s/%%s.%s.nc" % (name,name)

def log(s,*args):
	global verbose
	if verbose:
		print(s % args)

if __name__ == '__main__':
	n = arguments()
	spawn_cases(n)