
from string import Template
import sys, os, shutil
from datetime import datetime, date, timedelta
import glob

def tdhours(td): 
	return (td.days * 24. + td.seconds / 3600.)

class WrfBase(object):

	def __init__(self, **kw):
		#print kw
		self.props = kw.copy()
		self.cmds = []

	def add_cmd(self, cmd):
		#print '# %s'%cmd
		self.cmds.append(cmd)

	def get(self, key):
		try:
			return self.props[key]
		except:
			val = None
		# These are default values:
		if key == 'workdir':
			val = '/work/kolstad/wrf/%s/%s/%s' %(
				self.get('expid'), 
				self.get('config'),
				self.get('anatime_fmt')
			)
		elif key == 'rundir':
			val = '%s/run' %self.get('workdir')
		elif key == 'wpsdir':
			val = '%s/WPS' %self.get('workdir')
		elif key == 'wrfpath':
			val = '/work/apps/WRF/%s-cray' %self.get('wrf_version')
		elif key == 'wpspath':
			val = '/work/apps/WPS/%s-cray' %self.get('wrf_version')
		elif key == 'wrftmpdir':
			val = '%s/%s' %(self.get('wrftemplatebasedir'), self.get('wrf_version'))
		#elif key == 'duration_h':
		#	td = self.get('duration')
		elif key == 'scriptfile':
			val = '%s/go.sh' %self.get('workdir')	
		elif key == 'anatime_fmt':
			val = self.get('anatime').strftime('%Y%m%d%H')
		elif key == 'basedatadir':
			val = '/work/shared/bjerknes/kolstad/data'
		elif key == 'update_anatime':
			val = True

		if val is None:
			sys.exit("Invalid key: %s..." %key)
		self.props[key] = val
		return val

	def print_props(self):
		for k, v in self.props.items():
			print k, v

	def sub(self, **kw):
		d = kw['d']
		# Loop through defaults:
		for k,v in self.DEFAULTS.items():
			try:
				a = d[k]
			except:
				d[k] = v
		sub = dict()
		for k,v in d.items():
			try:
				fmt = self.FMT[k]
			except:
				fmt = '%s'
			s = fmt%v
			sub[k] = self._get_line(s, k)
		print sub
		try:
			s = open('templates/%s' %kw['fn']).read()
			t = Template(s)
			c = t.substitute(sub)
			os.system('rm -f %s' %kw['targetfile']) 
			f = open(kw['targetfile'], 'w')
			f.write(c)
			f.close()
			#print c
		except:
			import traceback
			traceback.print_exc()

	def _get_line(self, s, k):
		return s

	def log(self, s):
		out = '%s - LOG - %s' %(datetime.now().strftime('%H:%M:%S'), s)
		print out


class WrfSubmitter(WrfBase):
	
	DEFAULTS = {
	}
	
	FMT = {
	}

	def __init__(self, **kw):
		super(WrfSubmitter, self).__init__(**kw)

	def submit(self):
		d = dict()
		# Copy required keys:
		for k in ('scriptdir','duration_h',):
			d[k] = self.get(k)
		d['anatime'] = self.get('anatime_fmt')
		d['job_name'] = 'wrf_%s_%s_%s' %(
			self.get('expid'),
			self.get('config'),
			d['anatime']
		)
		d['duration_h'] = self.get('duration_h')
		d['scriptfile'] = self.get('scriptfile')
		# Fill in the template
		for stage in self.get('stages'):
			dd = d.copy()
			dd['ncpu'] = self.get('ncpu')[stage]
			dd['walltime'] = self.get('walltime')[stage]
			dd['stages'] = stage
			dd['job_name'] += '_%s'%stage
			t = 'pbs_%s.sh' %stage
			self.sub(targetfile = t, fn = 'pbs.sh', d = dd)
			if self.get('commit'):
				self.log('Committing to queue...')
				os.system('qsub %s' %t)
	

class WrfJob(WrfBase):

	DEFAULTS = {
		'start_minute': 0,
		'start_second': 0,
		'end_minute': 0,
		'end_second': 0
	}

	NOEXPAND = (
		'max_dom',
		'run_days',
		'run_hours',
		'interval_seconds',
		'geog_data_path',
	)

	FMT = {
		'start_date': "'%s'",
		'end_date': "'%s'"
	}

	def __init__(self, **kw):
		super(WrfJob, self).__init__(**kw)

	def _get_line(self, s, k):
		expand = (not k in self.NOEXPAND)
		return (self._expand(s) if expand else s)

	def setup(self):

		self.log("Setting up WPS/WRF run...")

		#self.print_props()

		# Translate any aliases:
		stages = []
		for stg in self.get('stages'):
			if stg == 'all':
				stages.extend(['clean','geogrid','ungrib','metgrid','real','wrf'])
			elif stg == 'wps':
				stages.extend(['geogrid','ungrib','metgrid'])
			elif stg == 'run':
				stages.extend(['real','wrf'])
			else:
				stages.append(stg)

		wps = ('ungrib' in stages or 'geogrid' in stages or 'metgrid' in stages)
		run = ('real' in stages or 'wrf' in stages)

		# Make the directory on /work
		wd = self.get('workdir')
		if 'clean' in stages:
			self.log('Deleting work dir: %s...'%wd)
			os.system('rm -rf %s' %wd)
		self.log('Creating work dir: %s...'%wd)
		os.system('mkdir -p %s'%wd)
			
		# Our list of commands:
		cmds = [
			'module load netcdf'	
		]
		cmds.append('echo "Time started:"')
		cmds.append('date')

		# Create a dictionary used for the namelists:
		d = dict()
		# Copy required keys:
		for k in ('max_dom','geog_data_path',):
			d[k] = self.get(k)
		# Set times:
		at = self.get('anatime')
		d['start_year'] = at.year
		d['start_month'] = at.month
		d['start_day'] = at.day
		d['start_hour'] = at.hour
		d['run_days'] = self.get('duration').days
		d['run_hours'] = int(self.get('duration').seconds/3600)
		last = at + self.get('duration')
		d['end_year'] = last.year
		d['end_month'] = last.month
		d['end_day'] = last.day
		d['end_hour'] = last.hour
		# For namelist.wps:
		d['start_date'] = at.strftime('%Y-%m-%d_%H:%M:%S')
		d['end_date'] = last.strftime('%Y-%m-%d_%H:%M:%S')
		d['interval_seconds'] = self.get('interval_h')*3600
		#print d
			
		wpsdir = '%s/WPS' %wd
		rundir = '%s/run/' %wd

		# Do the WPS part first:
		if wps:
			
			# Load the WPS module:
			cmds.append('module load WPS/%s' %self.get('wrf_version'))

			# Our working directory:
			self.log('Copying WPS template dir to: %s...' %wpsdir)
			if os.path.exists(wpsdir):
				os.system('cp -r %s/WPS/* %s' %(self.get('wrftmpdir'), wpsdir))
			else:
				os.system('cp -r %s/WPS/ %s' %(self.get('wrftmpdir'), wpsdir))
			cmds.append('cd %s' %wpsdir)
			
			# Write namelist:
			self.log('Creating namelist.wps...')
			self.sub(targetfile = '%s/namelist.wps' %wpsdir, fn = 'namelist.wps', d = d)
			
			# Copy these if they exist?
			files_to_copy = {
				'GEOGRID.TBL': 'geogrid',
				'METGRID.TBL': 'metgrid',
				'Vtable.%s'%self.get('datasource'): 'Vtable'
			}
			for k,v in files_to_copy.items():
				fn2 = '%s/files/%s' %(self.get('scriptdir'), k)
				if os.path.exists(fn2):
					self.log('Copying %s...' %fn2)
					cmds.append('cp %s %s/%s' %(fn2, wpsdir, v))

			if 'ungrib' in stages:
				cmds.append('module load cdo')
				# Copy data files
				targetdir = '%s/bdata' %wpsdir
				datasource = self.get('datasource')
				cmds.append('mkdir -p %s' %targetdir)
				dt = at
				td = timedelta(hours=self.get('interval_h'))
				filenames = []
				while dt <= last:
					if datasource == 'fnl':
						filenames.append('%s/%s%fnl_%s' %(
							self.get('basedatadir'), 
							datasource,
							dt.strftime('%Y%m%d_%H_%M')
						))
					elif datasource == 'gfs':
						filenames.append('%s/%s/gfs.t%sz.pgrb2f%02d' %(
							self.get('basedatadir'), 
							datasource,
							at.strftime('%H'),
							int(tdhours(dt-at))
						))
						#fn = 'gfs_4_%s_%03d.grb2'%(
						#	at.strftime('%Y%m%d_%H%M'),
						#	int(tdhours(dt-at))
						#)
					elif datasource == 'erai':
						if self.get('update_anatime'):
							diff = dt.hour % 12
							anatime = dt - timedelta(hours = diff)
						else:
							anatime = at
							diff = int(tdhours(dt-anatime))
						filenames.append('%s/%s/%s/erai_%s_%02d.grb'%(
							self.get('basedatadir'), 
							datasource, 
							anatime.strftime('%Y/%m%d/%H'),
							anatime.strftime('%Y%m%d%H'),
							diff
						))
					dt += td
				for fn in filenames:
					filename = os.path.basename(fn)
					#path = '%s/%s' %(self.get('datadir'), fn)
					# Try to use CDO to strip the grid.
					# Not supported for GFS files, it seems.
					#try:
						#sys.exit('Selection of bounding box not supported for GFS files')
						#box = self.get('lonlatbox')
						#self.log('Using CDO on %s...' %fn)
						#cmds.append('cp %s %s/%s_orig' %(fn, targetdir, fn))
						#cmds.append('cdo sellonlatbox,%s %s/%s_orig %s/%s'%(
						#	','.join(['%s'%s for s in box]),
						#	targetdir, fn,
						#	targetdir, fn,
						#))
					#except:
					self.log('Linking to %s...' %filename)
					cmds.append('ln -sf %s %s/%s' %(fn, targetdir, filename))
				cmds.append('rm -rf %s/*_orig' %targetdir)
				# Link to data files:
				cmds.append('csh ./link_grib.csh %s/*' %targetdir)
				# Run ungrib:
				cmds.append('aprun -B ungrib.exe')

				# Now convert from ECMWF hybrid levels:
				if self.get('datasource') == 'erai':
					cmds.append('aprun -B calc_ecmwf_p.exe')


			# Run geogrid:
			if 'geogrid' in stages:
				self.run_geogrid(cmds)

			# Run metgrid:
			if 'metgrid' in stages:
				#self.run_geogrid(cmds)
				cmds.append('aprun -B metgrid.exe')

		# Now run wrf:
		if run:

			# Load the WRF module:
			cmds.append('module load WRF/%s' %self.get('wrf_version'))

			# Our working directory:
			#os.system('rm -rf %s' %(rundir))
			if not os.path.exists(rundir):
				self.log('Copying WRF template dir to: %s...' %rundir)
				os.system('cp -r %s/run/ %s' %(self.get('wrftmpdir'), rundir))
			cmds.append('cd %s' %rundir)
			self.log('Creating namelist.input...')
			self.sub(targetfile = '%s/namelist.input' %rundir, fn = 'namelist.input', d = d)

			
			if 'real' in stages:
				cmds.append('ln -sf ../WPS/met_em.d0* .')
				cmds.append('aprun -B real.exe')

			if 'wrf' in stages:
				cmds.append('aprun -B wrf.exe')

		cmds.append('echo "Time stopped:"')
		cmds.append('date')

		# Write the commands to a script file:
		self.log('Writing to script file: %s...' %self.get('scriptfile'))
		f = open('%s' %self.get('scriptfile'), 'w')
		for cmd in cmds:
			print cmd
			f.write(cmd+'\n')
		f.close()

	def run_geogrid(self, cmds):
		# Check if we have to run geogrid:
		dir = '%s/%s/%s' %(self.get('geofilebasedir'), self.get('expid'), self.get('config'))
		files = glob.glob('%s/geo_em*' %dir)
		if len(files) < self.get('max_dom'):
			# Run geogrid
			cmds.append('aprun -B geogrid.exe')
			cmds.append('mkdir -p %s' %dir)
			cmds.append('cp geo_em*.nc %s/' %dir)
		else:
			for f in files:
				cmds.append('ln -sf %s .' %f) 

	def _expand(self, s):
		return ", ".join([s for j in range(self.get('max_dom'))])


