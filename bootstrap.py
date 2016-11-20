#!/usr/bin/env python2.7

import os, sys
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
	climo = netCDF4.Dataset("vt.mean.anl.nc").
	log("Process %d loaded climo",os.getpid())


def clean_dates(line):
	return line.split(' ')[0]


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
		fluxes = map(calc_flux, offset_dates)
		averages.append( sum(fluxes)/len(fluxes) )

	log("iteration %d has averages: %s",i,averages)

def get_dates(N=52, replace=False):
	global date_list
	numpy.random.seed( int(str(time.time()-int(time.time())).split('.')[1][:7]) + os.getpid() )
	return numpy.random.choice(date_list, size=N, replace=replace)

def make_offset(dates, offset):
	return [ datetime.datetime.strptime(d,"%Y%m%d%H") + datetime.timedelta(days=offset) for d in dates ]

def calc_flux(date):
	season = find_season(date)
	"""
	meridional_wind = None
	with open(season % ("v",)) as f:
		meridional_wind = f.read()

	temp = None
	with open(season % ("t",)) as f:
		temp = f.read()
	"""
	return date.day


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