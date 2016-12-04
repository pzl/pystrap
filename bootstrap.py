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
datefile=None
fluxdir=None
output=None
offset_range=[0,0]


def arguments():
	global verbose, replacement, samples, percentile, offset_range, datefile, fluxdir, output
	parser = argparse.ArgumentParser(description="bootstraps some research stuff")
	parser.add_argument('-v','--verbose',action='store_true',help="print a lot of extra crap")
	parser.add_argument('-n','--iterations',action='store',type=int,default=10000,help="how many iterations to run (default 10000)")
	parser.add_argument('-r','--replacement',action='store_true',help='WITH replacement')
	parser.add_argument('-s','--samples',action='store',type=int,default=52,help="How many dates are selected (default 52)")
	parser.add_argument('-p','--percentile',action='store',type=float,default=2.0,help="What percentile to fetch (grabs X and 100-X). Default is 2.0, which will fetch percentiles 2.0 and 98.0")
	parser.add_argument('datefile',metavar="DATELIST",help="file containing list of YYYYMMDDHH dates to select from. Dates must be in second space-separated column of file. First two cols ignored.")
	parser.add_argument('-b','--begin',action='store',type=int,default=-30,help="Beginning offset, in days from chosen sample date, inclusive (default -30)")
	parser.add_argument('-e','--end',action='store',type=int,default=30,help="Ending offset, in days from chosen sample date, inclusive (default 30).")
	parser.add_argument('fluxdir',metavar="FLUXDIR",help="directory containing the pre-computed heat flux averages")
	parser.add_argument('output',metavar="OUTPUT",help="output file")

	args = parser.parse_args()
	verbose = args.verbose
	replacement = args.replacement
	samples = args.samples
	percentile = args.percentile
	offset_range = [args.begin,args.end+1]
	datefile = args.datefile
	fluxdir = args.fluxdir
	output = args.output

	prompt = "{n:d} iterations of {date:d} dates, with{repl:s} replacement. From {begin:d} to {end:d}. Continue? Y/n:  ".format(
			n=args.iterations,
			date=samples,
			repl='out' if not replacement else '',
			begin=args.begin,
			end=args.end)
	answer = raw_input(prompt).lower()
	if answer == '' or answer == 'y' or answer == 'yes':
		return args.iterations
	else:
		sys.exit()


def load_inits():
	load_dates()
	load_fluxes()

date_list = []
def load_dates():
	global datefile, date_list
	with open(datefile,"r") as f:
		date_list = f.read().splitlines()
	date_list = map(clean_dates, date_list) # remove columns
	log("Loaded date list")

def clean_dates(line):
	return line.split(' ')[2]


fluxes={}
def load_fluxes():
	global fluxes, fluxdir
	flux_list = glob.iglob("%s/*/*.txt" % (fluxdir,))
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


def spawn_iterations(n):
	log("Spawning %d processes",cpu_count()*2)
	pool = Pool( cpu_count()*2 )
	pool.map(single_sample, range(n))
	#map(single_sample,range(n))

def single_sample(i):
	global replacement, samples, offset_range
	rand_dates = get_dates(samples,replacement)
	log("date list: %s",rand_dates)
	averages = []
	for offset in range(*offset_range): # [a,b)
		offset_dates = make_offset(rand_dates, offset)
		log("%d for offset %d, dates: %s", os.getpid(), offset, offset_dates)
		fluxes = numpy.array(map(get_flux, offset_dates))
		log("got fluxes %s",fluxes)
		log("with mean: %s",fluxes.mean())
		averages.append(fluxes.mean())
	log("List of averages: %s",averages)
	try:
		os.mkdir("samples")
	except OSError:
		pass # folder already exists. good!
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
	global percentile, output
	print("Writing result to %s" % (output,))
	with open(output,"w") as f:
		for p in percentiles:
			f.write("%s %s\n" % (p[0],p[1]))


def clean_up_averages():
	shutil.rmtree("samples")

if __name__ == '__main__':
	start = time.time()
	n = arguments()
	load_inits()
	spawn_iterations(n)
	average_finish = time.time()
	print('flux averages calculated in %s' % (average_finish-start,))

	avgs = load_averages()
	perc = get_percentiles(n,avgs)
	write_percentiles(perc)

	clean_up_averages()