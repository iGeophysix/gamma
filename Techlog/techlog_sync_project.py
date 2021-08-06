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

__doc__ = """Import entire Gamma project to the current Techlog project."""
__author__ = """Anton O"""
__date__ = """2021-08-02"""
__version__ = """1.0"""
__pyVersion__ = """3"""
__group__ = """"""
__suffix__ = """"""
__prefix__ = """"""
__applyMode__ = """0"""
__awiEngine__ = """v2"""
__layoutTemplateMode__ = """"""
__includeMissingValues__ = """True"""
__keepPreviouslyComputedValues__ = """True"""
__areInputDisplayed__ = """True"""
__useMultiWellLayout__ = """True"""
__idForHelp__ = """"""
__executionGranularity__ = """full"""
#DeclarationsEnd
import sys

sys.path.append(GAMMA_PROJECT_PATH)

import numpy as np
import re
import json
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.domain.Log import BasicLog

forbidden_characters = ''.join(db.forbiddenCharacterList(0))
re_forbidden_characters = re.compile(f'[{forbidden_characters}]+')

# delete all wells
# for well in db.wellList():
# 	db.wellDelete(well)

p = Project()
db.wellCreate('QUANTI')
for prop, value in p.meta.items():
	db.wellPropertyChange('QUANTI', prop, str(value))
for well in p.list_wells():
	db.wellDelete(well)
	db.wellCreate(well)
	w = Well(well)
	for prop, value in w.meta.items():
		if isinstance(value, dict) and 'value' in value:
			db.wellPropertyChange(well, prop, value['value'], value.get('units', ''), value.get('description', ''))
		else:
			db.wellPropertyChange(well, prop, str(value))
	t_dataset_list = {}
	for dataset in w.datasets:
		ds = WellDataset(w, dataset)
		for var in ds.log_list:
			log = BasicLog(ds.id, var)
			ref, g_log_data = log.values.T
			ref = tuple(ref)
			key = (dataset, ref)
			if key in t_dataset_list:
				t_dataset = t_dataset_list[key]
			else:
				t_dataset_raw = re_forbidden_characters.sub('_', dataset)
				t_dataset = db.datasetNewName(well, t_dataset_raw)
				db.datasetCreate(well, t_dataset, 'DEPTH', 'Measured Depth', 'm', ref)
				t_dataset_list[key] = t_dataset
				for prop, value in ds.info.items():
					if isinstance(value, dict) and 'value' in value:
						db.datasetPropertyChange(well, t_dataset, prop, value['value'], value.get('units', ''), value.get('description', ''))
					else:
						db.datasetPropertyChange(well, t_dataset, prop, str(value))
			t_log_data = list(np.nan_to_num(g_log_data, nan=MissingValue))
			db.variableSave(well, t_dataset, log.name, log.meta.family, log.meta.units, t_log_data)
			for prop, value in log.meta.asdict().items():
				db.variablePropertyChange(well, t_dataset, log.name, prop, str(value))
