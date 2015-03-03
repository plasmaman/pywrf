# pywrf
Python code for running wrf

To copy the key fields from namelist.wps in the root directory, type:
python exe.py cmd=cp

To submit a WPS run, type:
python exe.py cmd=sub stages=wps startdate=2002121800 commit=yes

Omit the commit if you just want to test. Replace 'wps' with 'run' for real and wrf.

To copy directories and create the script file that will be run type:
python exe.py cmd=setup stages=wps anatime=2002121800

Then you can look at the script file, in this case /work/kolstad/wrf/bspl/c1/2002121800/go.sh.
