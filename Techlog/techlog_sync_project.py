from math import *
from TechlogMath import *
from operator import *
import sys
if sys.version_info[0]==3:
    from six.moves import range

PI     = 3.14159265358979323846
PIO2   = 1.57079632679489661923
PIO4   = 7.85398163397448309616E-1
SQRT2  = 1.41421356237309504880
SQRTH  = 7.07106781186547524401E-1
E      = exp(1)
LN2    = log(2)
LN10   = log(10)
LOG2E  = 1.4426950408889634073599
LOG10E = 1.0 / LN10
MissingValue = -9999
def iif(condition, trueResult=MissingValue, falseResult=MissingValue):
	if condition:
		return trueResult
	else:
		return falseResult

#Declarations
#The dictionary of parameters v2.0
#name,bname,type,family,measurement,unit,value,mode,description,group,min,max,list,enable,iscombocheckbox,isused
parameterDict = {}
try:
	if Parameter:
		pass
except NameError:
	class Parameter:
		def __init__(self, **d):
			pass
#Type:Folder
#Mode:In
#Description:
#isFolderPath:True
GAMMA_PROJECT_PATH = "C:\\Users\\dIcEmAN\\Docker\\petrotool"
parameterDict.update({'GAMMA_PROJECT_PATH' : Parameter(name='GAMMA_PROJECT_PATH',bname='',type='Folder',family='',measurement='',unit='',value='C:\\Users\\dIcEmAN\\Docker\\petrotool',mode='In',description='',group='',min='',max='',list='',enable='True',iscombocheckbox='False',isused='True')})
#Type:String
#Mode:In
#Description:
#List:No/Yes
profile = "No"
parameterDict.update({'profile' : Parameter(name='profile',bname='',type='String',family='',measurement='',unit='',value='No',mode='In',description='',group='',min='',max='',list='No/Yes',enable='True',iscombocheckbox='False',isused='True')})

__doc__ = """Import entire Gamma project to the current Techlog project."""
__author__ = """Anton O"""
__date__ = """2021-08-09"""
__version__ = """1.0"""
__pyVersion__ = """3"""
__group__ = """"""
__suffix__ = """"""
__prefix__ = """"""
__applyMode__ = """0"""
__awiEngine__ = """v1"""
__layoutTemplateMode__ = """"""
__includeMissingValues__ = """True"""
__keepPreviouslyComputedValues__ = """True"""
__areInputDisplayed__ = """True"""
__useMultiWellLayout__ = """True"""
__idForHelp__ = """"""
__executionGranularity__ = """full"""
#DeclarationsEnd
import sys
import cProfile

sys.path.append(GAMMA_PROJECT_PATH)

import numpy as np
import re
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.domain.Log import BasicLog
from components.petrophysics.volumetric_model import interpolate_to_common_reference
from components.domain.MarkersSet import MarkersSet
from settings import DEFAULT_DEPTH_UNITS, DEFAULT_MARKERS_NAME

forbidden_characters = ''.join(db.forbiddenCharacterList(0))
re_forbidden_characters = re.compile(f'[{forbidden_characters}]+')
profile = profile == 'Yes'

def newDatasetName(well, dataset):
	t_dataset_raw = re_forbidden_characters.sub('_', dataset)
	return db.datasetNewName(well, t_dataset_raw)

def main():
	db.progressBarShow()
	# delete all wells
	# for well in db.wellList():
	# 	db.wellDelete(well)
	p = Project()
	db.wellCreate('QUANTI')
	for prop, value in p.meta.items():
		db.wellPropertyChange('QUANTI', prop, str(value))
	wells = p.list_wells()
	for n_well, g_well in enumerate(wells):
		db.progressBarSetValue(float(n_well + 1) / len(wells) * 100, information='Data retrieving')
		t_well = re_forbidden_characters.sub('_', g_well)
		db.wellDelete(t_well)
		db.wellCreate(t_well)
		w = Well(g_well)
		for prop, value in w.meta.items():
			if isinstance(value, dict) and 'value' in value:
				db.wellPropertyChange(t_well, prop, value['value'], value.get('units', ''), value.get('description', ''))
			else:
				db.wellPropertyChange(t_well, prop, str(value))
		for dataset in w.datasets:
			ds = WellDataset(w, dataset)
			logs = []
			for var in ds.log_list:
				log = BasicLog(ds.id, var)
				if 'main_depth' in log.meta.tags:
					continue
				ref, g_log_data = log.values.T
				if dataset == DEFAULT_MARKERS_NAME and log.meta.type == 'MarkersLog':
					zonation_dataset = newDatasetName(t_well, var)
					db.datasetCreate(t_well, zonation_dataset, 'DEPTH', 'Measured Depth', DEFAULT_DEPTH_UNITS, list(ref))
					db.datasetTypeIDChange(t_well, zonation_dataset, 1)
					family = 'Zone Name'
					ms = MarkersSet(var)
					index_name = {v: k for k, v in ms.markers.items()}
					zone_name = list(map(index_name.__getitem__, g_log_data))
					db.variableSave(t_well, zonation_dataset, 'Zone Name', 'Zone Name', 'unitless', zone_name)
					for prop, value in log.meta.asdict().items():
						db.variablePropertyChange(t_well, zonation_dataset, 'Zone Name', prop, str(value))
				else:
					logs.append(log)
			if logs:
				logs = interpolate_to_common_reference(logs)
				t_dataset = newDatasetName(t_well, dataset)
				ref = list(logs[0].values[:, 0])
				db.datasetCreate(t_well, t_dataset, 'DEPTH', 'Measured Depth', DEFAULT_DEPTH_UNITS, ref)
				for prop, value in ds.meta.items():
					if isinstance(value, dict) and 'value' in value:
						db.datasetPropertyChange(t_well, t_dataset, prop, value['value'], value.get('units', ''), value.get('description', ''))
					else:
						db.datasetPropertyChange(t_well, t_dataset, prop, str(value))
				for log in logs:
					family = log.meta.family if hasattr(log.meta, 'family') else ''
					units = log.meta.units if hasattr(log.meta, 'units') else ''
					log_data = log.values[:, 1]
					log_data[np.isnan(log_data)] = MissingValue
					t_log = re_forbidden_characters.sub('_', log.name)
					db.variableSave(t_well, t_dataset, t_log, family, units, list(log_data))
					for prop, value in log.meta.asdict().items():
						db.variablePropertyChange(t_well, t_dataset, t_log, prop, str(value))
	db.progressBarHide()

if profile:
	pr = cProfile.Profile()
	pr.enable()

main()

if profile:
	pr.disable()
	pr.print_stats(sort='cumtime')
