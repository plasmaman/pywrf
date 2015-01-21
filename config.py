
# These are the general settings:
SETTINGS = {
	'wrf_version': '3.5.1',
	'max_dom': 2,
	'datasource': 'erai',
	'interval_h': 3,
	'duration_h': 3,
	#'lonlatbox': [-100,100,20,90], # lon1,lon2,lat1,lat2
	#'datadir': '/work/shared/bjerknes/kolstad/data/fnl',
	#'datadir': '/work/shared/bjerknes/kolstad/data/gfs',
	'geog_data_path': '/work/shared/bjerknes/kolstad/geog',
	'geofilebasedir': '/work/shared/bjerknes/kolstad/geofiles',
	'wrftemplatebasedir': '/work/shared/bjerknes/kolstad/wrftmp',
	'ncpu': {
		'all': 16,
		'run': 128,
		'real': 48,
		'wps': 16,
		'ungrib': 1,
		'geogrid': 32,
		'metgrid': 32
	},
	'walltime': {
		'all': '00:15:00',
		#'run': '00:59:59',
		'run': '00:10:00',
		'real': '00:05:00',
		'wps': '00:05:00',
		'ungrib': '00:05:00',
		'metgrid': '00:10:00',
		'geogrid': '00:05:00'
	}
}


