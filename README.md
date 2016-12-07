pystrap
=======

This is a really narrow use-case program to perform bootstrapping on a specific data set and organization. If you're not sure if this program is for you, it isn't.

Requirements
------------

- Python (2.x or 3.x will work)
- Numpy

Running
--------

`python bootstrap.py [OPTIONS] DATELIST FLUXDIR OUTPUT`

### Options

    -h, --help            show this help message and exit
    -v, --verbose         print a lot of extra crap
    -n ITERATIONS,        how many iterations to run (default 10000)
      --iterations ITERATIONS  
    -r, --replacement     enable replacement (when given, selections are made WITH replacement)
    -s SAMPLES,           How many dates are selected (default 52)
      --samples SAMPLES 
    -p PERCENTILE,        What percentile to fetch (grabs X and 100-X). Default is 2.0, which will fetch percentiles 2.0 and 98.0
      --percentile PERCENTILE               
    -b BEGIN,             Beginning offset, in days from chosen sample date, inclusive (default -30)
      --begin BEGIN
    -e END,               Ending offset, in days from chosen sample date, inclusive (default 30)
      --end END     

### Arguments

- **DATELIST**: file containing list of YYYYMMDDHH dates, one per line, to select from.
- **FLUXDIR**: directory containing the pre-computed heat flux averages
- **OUTPUT**: output file

### Examples

`python bootstrap.py dates.txt /path/to/heatflux/ myoutput.txt`

This will run 10,000 iterations of 52 dates chosen from `dates.txt`, _without_ replacement, from -30 to +30 days, and will display the 2nd and 98th percentile as output.


---

`python bootstrap.py -n 1000 -b -25 -e 25 -s 200 dates.txt /path/to/heatflux/ 200.txt`

This will run 1,000 iterations of 200 dates chosen from `dates.txt`, again _without_ replacement, from -25 to +25 days, and will display the 2nd and 98th percentile as output.

---

`python bootstrap.py -s 50 -r -p 5 dates.txt /path/to/heatflux/ fifth_percentile.txt`

This will run 10,000 iterations of 50 dates chosen from `dates.txt`, **with** replacement, from -30 to +30 days, and will display the 5th and 95th percentiles as output.