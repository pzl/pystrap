#!/usr/bin/env python2.7

import os, sys
import math
import random
import glob
import datetime, time
import shutil
import argparse
from multiprocessing import Pool, cpu_count

import numpy

###### Command line options
verbose=False
replacement=False
samples=52
percentile=2.0


def arguments():
	global verbose, replacement, samples, percentile
	parser = argparse.ArgumentParser(description="bootstraps some research stuff")
	parser.add_argument('-v','--verbose',action='store_true',help="print a lot of extra crap")
	parser.add_argument('-n','--cases',action='store',type=int,default=10000,help="how many iterations to run (default 10000)")
	parser.add_argument('-r','--replacement',action='store_true',help='WITH replacement')
	parser.add_argument('-s','--samples',action='store',type=int,default=52,help="How many dates are selected")
	parser.add_argument('-p','--percentile',action='store',type=float,default=2.0,help="What percentile to fetch (grabs X and 100-X). Default is 2.0, which will fetch percentiles 2.0 and 98.0, writing to 96.txt")

	args = parser.parse_args()
	verbose = args.verbose
	replacement = args.replacement
	samples = args.samples
	percentile = args.percentile

	answer = raw_input("%d cases of %d samples, with%s replacement. Continue? Y/n:  " % ( args.cases, samples, 'out' if not replacement else '' )).lower()
	if answer == '' or answer == 'y' or answer == 'yes':
		return args.cases
	else:
		sys.exit()


def load_inits():
	load_dates()
	load_fluxes()

date_list = []
def load_dates():
	global date_list
	with open("dates.txt","r") as f:
		date_list = f.read().splitlines()
	date_list = map(clean_dates, date_list) # remove columns
	log("Loaded date list")

def clean_dates(line):
	return line.split(' ')[2]


fluxes={}
def load_fluxes():
	global fluxes
	flux_list = glob.iglob('fluxes/*/*.txt')
	for flux in flux_list:
		with open(flux,"r") as file:
			fluxes[os.path.splitext(os.path.basename(flux))[0]] = numpy.float(file.read())


def load_averages():
	samples = []
	sample_list = glob.iglob("samples/*.txt")
	for sample in sample_list:
		with open(sample,"r") as f:
			samples.append(map(float,f.read().splitlines()))
	return numpy.array(samples)


def spawn_cases(n):
	log("Spawning %d processes",cpu_count()*2)
	pool = Pool( cpu_count()*2 )
	pool.map(single_sample, range(n))
	#map(single_sample,range(n))

def single_sample(i):
	global replacement, samples
	rand_dates = get_dates(samples,replacement)
	log("date list: %s",rand_dates)
	averages = []
	for date_offset in range(-30,31): # [a,b)
		offset_dates = make_offset(rand_dates, date_offset)
		log("%d for offset %d, dates: %s", os.getpid(), date_offset, offset_dates)
		fluxes = numpy.array(map(get_flux, offset_dates))
		log("got fluxes %s",fluxes)
		log("with mean: %s",fluxes.mean())
		averages.append(fluxes.mean())
	log("List of averages: %s",averages)
	if not os.path.isdir("samples"):
		os.mkdir("samples")
	with open("samples/%06d.txt" % (i,),"w") as f:
		for line in averages:
			f.write("%s\n" % (line,))

	log("iteration %d has averages: %s",i,averages)

def get_dates(N=52, replace=False):
	global date_list
	numpy.random.seed( int(str(time.time()-int(time.time())).split('.')[1][:7]) + os.getpid() )
	return numpy.random.choice(date_list, size=N, replace=replace)

def make_offset(dates, offset):
	try:
		off = [ datetime.datetime.strptime(d,"%Y%m%d%H") + datetime.timedelta(days=offset) for d in dates ]
	except ValueError:
		print("Failed with dates: %s and offset: %s" % (dates,offset))
		raise

	return off


def get_flux(date):
	global fluxes
	return fluxes["%4d%02d%02d00" % (date.year,date.month,date.day)]


def find_season(date):
	name = "%s%s" % (date.year-1,date.year) if date.month < 7 else "%s%s" % (date.year,date.year+1)
	return "fluxes/%s/%4d%02d%02d00.txt" % (name,date.year,date.month,date.day)

def log(s,*args):
	global verbose
	if verbose:
		print(s % args)


def get_percentiles(n, avgs):
	global percentile
	ordered = numpy.sort(avgs.T)
	perc_small = int(percentile/100 * n)
	perc_large = int((100-percentile)/100 * n)

	percentiles = [ (x[perc_small],x[perc_large]) for x in ordered ]
	return percentiles


def write_percentiles(percentiles):
	global percentile
	if not os.path.isdir("output"):
		os.mkdir("output")
	final_name = "output/%.2f.txt" % ( 100-2.0*percentile,)
	print("Writing result to %s" % (final_name))
	with open(final_name,"w") as f:
		for p in percentiles:
			f.write("%s %s\n" % (p[0],p[1]))


def clean_up_averages():
	shutil.rmtree("samples")

if __name__ == '__main__':
	start = time.time()
	n = arguments()
	load_inits()
	spawn_cases(n)
	average_finish = time.time()
	print('flux averages calculated in %s' % (average_finish-start,))

	avgs = load_averages()
	perc = get_percentiles(n,avgs)
	write_percentiles(perc)

	clean_up_averages()