
from wrflib import WrfSubmitter, WrfJob
from datetime import datetime, timedelta
import os, sys
from config import SETTINGS

# Copy the most important fields to namelists in templates directory
def copygeo():
	fields = (
		'parent_id',
		'parent_grid_ratio',
		'i_parent_start',
		'j_parent_start',
		'e_we',
		'e_sn',
		'map_proj',
		'ref_lat',
		'truelat1',
		'truelat2',
		'ref_lon',
		'stand_lon',
	)
	d = dict()
	f = open('namelist.wps', 'r')
	for a in f.readlines():
		for b in fields:
			if not b in d and b in a:
				d[b] = a
				break
	f.close()
	for filename in (
		'templates/namelist.wps',
		'templates/namelist.input',
	):
		keys = d.keys()
		lines = []
		f = open(filename, 'r')
		for a in f.readlines():
			for k in keys:
				if k in a:
					lines.append(d[k])
					keys.remove(k)
					a = None
					break
			if a is not None:
				lines.append(a)
		f.close()
		f = open(filename, 'w')
		for line in lines:
			f.write(line)
		f.close()
	






def main():

	# Put file arguments in a dictionary:
	kw = dict([arg.split('=') for arg in sys.argv[1:]])

	# Check what we're asked to do:
	if not 'cmd' in kw:
		sys.exit('Please supply a command (cmd)!')
	
	# Do some basic tasks:
	if kw['cmd'] in ('cp','copygeo',):
		copygeo()

	# Or move on to more complicated things
	else:

		# Copy this before reading the arguments
		settings = SETTINGS.copy()

		# Extract the experiment id and the config:
		cwd = os.getcwd()
		(settings['expid'], settings['config'],) = cwd.split("/")[-2:]
		settings['scriptdir'] = cwd

		# Stages to run:
		try:
			settings['stages'] = [s.strip() for s in kw['stages'].split(',')]
		except:
			#settings['stages'] = ('ungrib','geogrid','metgrid','real','wrf',)
			settings['stages'] = ('all',)

		# The duration has to be set 
		try:
			duration_h = settings['duration_h']
		except:
			try:
				duration_h = kw['duration_h']
			except:
				duration_h = 24
		settings['duration'] = timedelta(hours = int(duration_h))

		# Set up:
		if kw['cmd'] in ('setup',):

			# Set the anatime:
			try:
				anatime = datetime.strptime(kw['anatime'], '%Y%m%d%H')
			except:
				sys.exit('Please supply a valid analysis time (anatime=YYYYMMDDHH)!')

			job = WrfJob(
				anatime = anatime,
				**settings
			)
			job.setup()

		# Submit
		elif kw['cmd'] in ('sub','submit',):

			# Set the start date:
			try:
				startdate = datetime.strptime(kw['startdate'], '%Y%m%d%H')
			except:
				sys.exit('Please supply a valid start date (startdate=YYYYMMDDHH)!')

			# Set the end date (defaults to start date):
			try:
				enddate = datetime.strptime(kw['enddate'], '%Y%m%d%H')
			except:
				enddate = startdate

			commit = None
			for k in ('c','commit',):
				try:
					if kw[k].lower() in ('true','yes',):
						commit = True
						break
				except:
					pass
				if commit is None:
					try:
						if kw[k].isdigit():
							commit = bool(int(kw[k]))
							break
					except:
						pass
			if commit is None:
				commit = False

			# Loop through all the new model runs:
			anatime = startdate
			while anatime <= enddate:
				s = WrfSubmitter(
					anatime = anatime,
					commit = commit,
					**settings
				)
				s.submit()
				anatime += settings['duration']




if __name__ == "__main__":
	main()

