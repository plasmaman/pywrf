
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
		elif key == 'post_output_dir':
			val = '%s/%s/%s/%s' %(
				self.get('gribfilebasedir'),
				self.get('expid'), 
				self.get('config'),
				self.get('anatime_fmt')
			)
		elif key == 'bdata_file_prefix':
			val = self.get('datasource')
		elif key == 'post_prefix':
			val = '%s/WRFPRS_' %self.get('post_output_dir')
		elif key == 'rundir':
			val = '%s/run' %self.get('workdir')
		elif key == 'wpsdir':
			val = '%s/WPS' %self.get('workdir')
		elif key == 'postdir':
			val = '%s/upp' %self.get('workdir')
		elif key == 'unipost_home':
			val = '/work/apps/upp/%s-pgi' %self.get('upp_version')
		elif key == 'wps_version':
			val = self.get('wrf_version')
		elif key == 'wrfpath':
			v = self.get('wrf_version')
			val = '/work/apps/WRF/%s-%s' %(v, ('cray' if v=='3.5.1' else 'pgi'))
		elif key == 'wpspath':
			v = self.get('wps_version')
			val = '/work/apps/WPS/%s-%s' %(v, ('cray' if v=='3.5.1' else 'pgi'))
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
		elif key == 'post_first':
			val = 0
			if self.get('is_restart_run'):
				val = self.get('restart_fhr') + 1
		elif key == 'post_last':
			val = int(self.get('duration_h'))
		elif key == 'post_interval':
			val = 1
		elif key == 'post_domains':
			#val = ' '.join(['d%02d'%j for j in range(1,int(self.get('max_dom'))+1)])
			val = ['d%02d'%j for j in range(1,int(self.get('max_dom'))+1)]
		elif key == 'is_restart_run':
			val = False

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
		'unipost_home',
		#'wrfout_dir',
		'wrfpath',
		'postdir',
		'wrfout_filename',
		'time_suffix',
		'grib_filename',
		'wrf_parm_filename',
		'fhr',
		'startdate',
		'domain',
		'is_restart_run',
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

	def get_time_suffix(self, fhr):
		dt = self.get('anatime') + timedelta(hours=fhr)
		return dt.strftime('%Y-%m-%d_%H:00:00')

	def get_wrfout_filename(self, fhr, domain):
		# See if we've specified this first:
		try:
			outdir = self.get('wrfout_dir')
		except:
			if self.get('is_restart_run') and fhr<=self.get('restart_fhr'):
				outdir = self.get('restartdir')
			else:
				outdir = self.get('rundir')
		fn = '%s/wrfout_%s_%s' %(
			outdir,
			domain,
			self.get_time_suffix(fhr)
		)
		return fn

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
		post = ('post' in stages)

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
		
		# anatime is always needed
		at = self.get('anatime')

		rundir = self.get('rundir')
		wpsdir = self.get('wpsdir')
		postdir = self.get('postdir')

		# Create a dictionary used for the namelists:
		d = dict()
		if wps or run:
			for k in ('max_dom','geog_data_path',):
				d[k] = self.get(k)
			# If restart run, change start time:
			if self.get('is_restart_run'):
				td = timedelta(hours = self.get('restart_fhr'))
				dt = at + td 
				duration = self.get('duration') - td
				d['is_restart_run'] = '.true.'
			else:
				duration = self.get('duration')
				dt = at
				d['is_restart_run'] = '.false.'
			# Set times:
			d['start_year'] = dt.year
			d['start_month'] = dt.month
			d['start_day'] = dt.day
			d['start_hour'] = dt.hour
			d['run_days'] = duration.days
			d['run_hours'] = int(duration.seconds/3600)
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
			
		# Do the WPS part first:
		if wps:
			
			# Load the WPS module:
			cmds.append('module load WPS/%s' %self.get('wps_version'))

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
						filenames.append('%s/%s/%s/%s_%s_%02d.grb'%(
							self.get('basedatadir'), 
							datasource,
							anatime.strftime('%Y/%m%d/%H'),
							self.get('bdata_file_prefix'), 
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
			# Link to restart files and boundary files if applicable
			if self.get('is_restart_run'):
				src = self.get('restartdir')
				#for pref in ('wrfrst','wrfbdy','wrfinput','wrffdda',):
				for pref in ('wrfbdy','wrfinput','wrffdda',):
					cmds.append('ln -sf %s/%s* .'%(src, pref))
				# Copy just the one restart file:
				td = timedelta(hours = self.get('restart_fhr'))
				dt = at + td 
				# Important to copy this, not link
				cmds.append('rm -f wrfrst_d*')
				cmds.append('cp %s/wrfrst_d*_%s* .'%(src, dt.strftime('%Y-%m-%d_%H')))
			self.log('Creating namelist.input...')
			self.sub(
				targetfile = '%s/namelist.input' %rundir, 
				fn = 'namelist.input', 
				d = d
			)

			
			if 'real' in stages:
				cmds.append('ln -sf ../WPS/met_em.d0* .')
				cmds.append('aprun -B real.exe')

			if 'wrf' in stages:
				cmds.append('aprun -B wrf.exe')
		
		# Now run upp:
		if post:

			# Load the WRF module:
			cmds.append('module load upp/%s' %self.get('upp_version'))

			# Our working directory:
			#os.system('rm -rf %s' %(rundir))
			if not os.path.exists(postdir):
				os.system('mkdir %s' %(postdir))
			cmds.append('cd %s' %postdir)
			cmds.append('cp %s/files/postcntrl.xml .' %self.get('scriptdir'))
			outdir = self.get('post_output_dir')
			cmds.append('mkdir -p %s' %outdir)

			# Common dictionary
			d['wrfpath'] = self.get('wrfpath')
			d['postdir'] = postdir
			d['unipost_home'] = self.get('unipost_home')
			d['startdate'] = at.strftime('%Y%m%d%H')

			for domain in self.get('post_domains'):

				d['domain'] = domain

				# Fixed fields first:
				fn = '%s/WRFPRS_%s_fixed.grb' %(outdir, domain)
				print 'Fixed file:',fn
				if not os.path.exists(fn):
					print 'Does not exist...'
					fhr = self.get('post_first')
					d['wrfout_filename'] = self.get_wrfout_filename(fhr, domain)
					print 'Looking for file:',d['wrfout_filename']
					if os.path.exists(d['wrfout_filename']):
						#d['wrfout_dir'] = self.get_wrfout_dir(fhr)
						d['time_suffix'] = self.get_time_suffix(fhr)
						d['wrf_parm_filename'] = '%s/files/wrf_cntrl.parm.fixed'%self.get('scriptdir')
						d['fhr'] = '%02d'%fhr
						d['grib_filename'] = fn
						scriptfile = 'run_unipost_fixed_%s'%domain
						self.sub(
							targetfile = '%s/%s' %(postdir, scriptfile), 
							fn = 'run_unipost', 
							d = d
						)
						cmds.append('ksh %s' %scriptfile)

				# Loop for each forecast hour
				for fhr in range(
					self.get('post_first'), 
					self.get('post_last')+1,
					self.get('post_interval')
				):
					print 'fhr:',fhr
					fn = '%s/WRFPRS_%s_%03d.grb' %(outdir, domain, fhr)
					if os.path.exists(fn):
						print 'Ok, file exists:',fn
					else:
						d['wrfout_filename'] = self.get_wrfout_filename(fhr, domain)
						print d['wrfout_filename']
						if os.path.exists(d['wrfout_filename']):
							#d['wrfout_dir'] = self.get_wrfout_dir(fhr)
							d['time_suffix'] = self.get_time_suffix(fhr)
							d['wrf_parm_filename'] = '%s/files/wrf_cntrl.parm'%self.get('scriptdir')
							d['fhr'] = '%02d'%fhr
							d['grib_filename'] = fn
							scriptfile = 'run_unipost_%s_%03d'%(domain, fhr)
							self.sub(
								targetfile = '%s/%s' %(postdir, scriptfile), 
								fn = 'run_unipost', 
								d = d
							)
							cmds.append('ksh %s' %scriptfile)

		#print d
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


