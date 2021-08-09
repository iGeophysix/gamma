# -*- coding: utf-8 -*-
from __future__ import division
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
GAMMA_PROJECT_PATH = u"C:\\Users\\dIcEmAN\\Docker\\petrotool"
parameterDict.update({'GAMMA_PROJECT_PATH' : Parameter(name='GAMMA_PROJECT_PATH',bname='',type='Folder',family='',measurement='',unit='',value='C:\\Users\\dIcEmAN\\Docker\\petrotool',mode='In',description='',group='',min='',max='',list='',enable='True',iscombocheckbox='False',isused='True')})
#DeclarationsEnd
import sys

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

def newDatasetName(well, dataset):
	t_dataset_raw = re_forbidden_characters.sub('_', dataset)
	return db.datasetNewName(well, t_dataset_raw)

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
	for dataset in w.datasets:
		ds = WellDataset(w, dataset)
		logs = []
		for var in ds.log_list:
			log = BasicLog(ds.id, var)
			if 'main_depth' in log.meta.tags:
				continue
			ref, g_log_data = log.values.T
			g_log_data[np.isnan(g_log_data)] = MissingValue
			if dataset == DEFAULT_MARKERS_NAME and log.meta.type == 'MarkersLog':
				zonation_dataset = newDatasetName(well, var)
				db.datasetCreate(well, zonation_dataset, 'DEPTH', 'Measured Depth', DEFAULT_DEPTH_UNITS, list(ref))
				db.datasetTypeIDChange(well, zonation_dataset, 1)
				family = 'Zone Name'
				ms = MarkersSet(var)
				index_name = {v: k for k, v in ms.markers.items()}
				zone_name = list(map(lambda marker_index: index_name[marker_index], g_log_data))
				db.variableSave(well, zonation_dataset, 'Zone Name', 'Zone Name', 'unitless', zone_name)
				for prop, value in log.meta.asdict().items():
					db.variablePropertyChange(well, zonation_dataset, 'Zone Name', prop, str(value))
			else:
				logs.append(log)
		if logs:
			logs = interpolate_to_common_reference(logs)
			t_dataset = newDatasetName(well, dataset)
			ref = list(logs[0].values[:, 0])
			db.datasetCreate(well, t_dataset, 'DEPTH', 'Measured Depth', DEFAULT_DEPTH_UNITS, ref)
			for prop, value in ds.info.items():
				if isinstance(value, dict) and 'value' in value:
					db.datasetPropertyChange(well, t_dataset, prop, value['value'], value.get('units', ''), value.get('description', ''))
				else:
					db.datasetPropertyChange(well, t_dataset, prop, str(value))
			for log in logs:
				family = log.meta.family if hasattr(log.meta, 'family') else ''
				units = log.meta.units if hasattr(log.meta, 'units') else ''
				log_data = log.values[:, 1]
				log_data[np.isnan(log_data)] = MissingValue
				db.variableSave(well, t_dataset, log.name, family, units, list(log_data))
				for prop, value in log.meta.asdict().items():
					db.variablePropertyChange(well, t_dataset, log.name, prop, str(value))

__doc__ = """Import entire Gamma project to the current Techlog project."""
__author__ = """Anton O"""
__date__ = """2021-08-09"""
__version__ = """1.0"""
__pyVersion__ = """3"""
__group__ = """"""
__suffix__ = """"""
__prefix__ = """"""
__applyMode__ = """0"""
__layoutTemplateMode__ = """"""
__includeMissingValues__ = """True"""
__keepPreviouslyComputedValues__ = """True"""
__areInputDisplayed__ = """True"""
__useMultiWellLayout__ = """True"""
__idForHelp__ = """"""
__executionGranularity__ = """full"""