from dataclasses import dataclass

from components.database.RedisStorage import RedisStorage
from components.domain.Well import get_well_by_id
from components.domain.WellDataset import WellDataset
from settings import DEFAULT_MARKERS_NAME


@dataclass
class MarkersSet:
    name: str
    markers: dict
    _sequence_max: int

    def __init__(self, name):
        self.name = name

        if self.exists():
            self._load()
        else:
            self.markers = {None: None}
            self._sequence_max = 0

    def exists(self):
        _s = RedisStorage()
        return _s.check_markerset_exists(self.name)

    def append(self, name, silent=True):
        '''
        Append new marker to the MarkerSet
        :param name: name of the new marker
        :param silent: raise exception if marker already exists in MarkerSet
        :return:
        '''
        if name is None:
            return
        elif not name in self.markers:
            self.markers[name] = self._sequence_max
            self._sequence_max += 1
        elif not silent:
            raise Exception(f"Marker {name} already exists in markers set {self.name}")

    def remove(self, name, silent=True):
        '''
        Removes marker from the MarkerSet and delete it from all wells
        :param name: name of marker to remove from the set
        :param silent: raise exception if marker doesn't exist in MarkerSet
        :return:
        '''
        if name in self.markers:
            for well_id in self.well_ids:
                w = get_well_by_id(well_id)
                if w.exists():
                    markerset = WellDataset(w, DEFAULT_MARKERS_NAME)
                    markerset.delete_log(self.name)
            del self.markers[name]
        elif not silent:
            raise Exception(f"Marker {name} wasn't found in markers set {self.name}")

    def add_well(self, well_id, silent=True):
        """
        Add well_id to well_ids index of the MarkerSet
        :param well_id: well id
        :param silent: if True and well_id is already in the index then raise an exception
        :return:
        """
        w = get_well_by_id(well_id)
        if not w.exists():
            raise Exception(f"Well id {well_id} was not found in the project")
        if not well_id in self.well_ids:
            self.well_ids.append(well_id)
        elif not silent:
            raise Exception(f"Well id {well_id} was already found in the index of MarkerSet {self.name}")

    def delete_well(self, well_id, silent=True):
        """
        Delete well_id from well_ids index of the MarkerSet and remove MarkerLog from the well
        :param well_id: well id
        :param silent: if True and well_id is already in the index then raise an exception
        :return:
        """
        w = get_well_by_id(well_id)
        if not w.exists():
            raise Exception(f"Well id {well_id} was not found in the project")
        if well_id in self.well_ids:
            markerset = WellDataset(w, DEFAULT_MARKERS_NAME)
            markerset.delete_log(self.name)
            del self.well_ids[well_id]
        elif not silent:
            raise Exception(f"Well id {well_id} was not found in the index of MarkerSet {self.name}")

    def asdict(self) -> dict:
        """
        Serialize as dict
        :return:
        """
        return {
            'name': self.name,
            'markers': self.markers,
            '_sequence_max': self._sequence_max
        }

    def __getitem__(self, item):
        return self.markers[item] if item is not None else None

    def save(self):
        _s = RedisStorage()
        _s.set_markerset_by_name(self.asdict())

    def _load(self):
        _s = RedisStorage()
        data = _s.get_markerset_by_name(self.name)
        self.markers = data['markers']
        self._sequence_max = data['_sequence_max']

    @property
    def well_ids(self):
        """
        Get well ids having this MarkerSet
        :return:
        """
        _s = RedisStorage()
        return _s.markerset_well_ids(self.name)
