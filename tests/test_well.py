import os
import time
import unittest

from storage import Storage
from well import Well
from well import WellDataset, WellDataset2, WellDatasetColumns

PATH_TO_TEST_DATA = os.path.join('test_data')


class TestWell(unittest.TestCase):
    def setUp(self) -> None:
        _s = Storage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_well(self):
        w = Well("test_well", new=True)
        assert w.info == {}
        w.delete()
        assert str(w) == "test_well"

    def test_create_well_and_read_las_files(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        assert 'one' in well.datasets
        dataset.delete()
        assert 'one' not in well.datasets
        well.delete()


class TestWellDataset(unittest.TestCase):
    def setUp(self) -> None:
        _s = Storage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_and_delete_datasets(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        dataset = WellDataset(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        assert 'one' in well.datasets
        dataset = WellDataset(well, "two")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        dataset = WellDataset(well, "one")
        dataset.delete()
        assert 'one' not in well.datasets
        assert 'two' in well.datasets

    def test_dataset_get_data(self):
        ref_depth = 2000.0880000
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        data = dataset.get_data(start=ref_depth - 0.001, end=ref_depth + 0.001)
        true_answer = {'GR': 46.731338501, 'SP': 63.879390717, 'DTC': 143.6020813, 'PEF': 6.3070640564, 'ROP': 38.207931519, 'RXO': -999.25, 'CALI': 18.639200211,
                       'DRHO': 0.0151574211, 'NPHI': 0.5864710212, 'RDEP': 0.4988202751, 'RHOB': 1.8031704426, 'RMED': 0.4965194166, 'RSHA': -999.25, 'X_LOC': 437627.5625,
                       'Y_LOC': 6470980.0, 'Z_LOC': -1974.846802, 'DEPTH_MD': 2000.0880127, 'MUDWEIGHT': 0.1366020888, 'FORCE_2020_LITHOFACIES_LITHOLOGY': 30000.0,
                       'FORCE_2020_LITHOFACIES_CONFIDENCE': 1.0}
        assert data[ref_depth] == true_answer

    def test_dataset_info(self):
        wellname = 'new_test_well'
        dataset_name = 'new_test_dataset'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.register()
        info = {"step": 0.5}
        dataset.info = info
        assert dataset.info == info
        info_2 = {"step": 0.8}
        dataset.info = info_2
        assert dataset.info == info_2
        well.delete()

    def test_dataset_insert_row(self):
        wellname = 'random_well'
        dataset_name = 'insert_row'
        reference = 450
        reference_2 = 800
        row = {"GR": 87.81237987, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        row_2 = {"GR": 97.2, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.register()
        dataset.info = {"step": 0.5}

        dataset.insert(reference, row)
        assert dataset.get_data(start=reference, end=reference) == {reference: row}
        dataset.insert(reference, row_2)
        assert dataset.get_data(start=reference, end=reference) == {reference: row_2}
        assert dataset.get_data(logs=["GR", "PS"], start=reference, end=reference) == {reference: {"GR": 97.2, "PS": -0.234235555667, }}
        assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}

        dataset.insert(reference_2, row)
        assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}
        dataset.insert(reference_2, row_2)
        assert dataset.get_data(start=reference_2, end=reference_2) == {reference_2: row_2}
        assert dataset.get_data(logs=["GR", "PS"], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, "PS": -0.234235555667, }}
        assert dataset.get_data(logs=["GR", ], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, }}

    def test_add_one_new_log(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        mnemonic = "NEW_CURVE"
        new_curve = {
            198: {mnemonic: 0},
            198.0164: {mnemonic: 1},
            198.1684: {mnemonic: 2},
            198.4724: {mnemonic: 3},
            198.5: {mnemonic: 4},
        }
        new_curve_meta = {mnemonic: {"unit": "ohm.m", "descr": mnemonic, "value": "", "mnemonic": mnemonic, "original_mnemonic": mnemonic}, }
        dataset.add_curve(new_curve, new_curve_meta)

        assert dataset.get_data(logs=[mnemonic, ], start=198, end=198)[198] == new_curve[198]
        dataset.delete_curve(mnemonic)

    def test_add_three_new_logs(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "two")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        mnemonic_1 = "NEW_CURVE_1"
        mnemonic_2 = "NEW_CURVE_2"
        mnemonic_3 = "NEW_CURVE_3"
        new_curve = {
            198: {mnemonic_1: "A0", mnemonic_2: "B0", mnemonic_3: "C0"},
            198.0164: {mnemonic_1: "A1", mnemonic_2: "B1", mnemonic_3: "C1"},
            198.1684: {mnemonic_1: "A2", mnemonic_2: "B2", mnemonic_3: "C2"},
            198.4724: {mnemonic_1: "A3", mnemonic_2: "B3", mnemonic_3: "C3"},
            198.5: {mnemonic_1: "A4", mnemonic_2: "B4", mnemonic_3: "C4"},
        }
        new_curve_meta = {
            mnemonic: {"unit": "ohm.m", "descr": mnemonic, "value": "", "mnemonic": mnemonic, "original_mnemonic": mnemonic} for mnemonic in [mnemonic_1, mnemonic_2, mnemonic_3]
        }
        dataset.add_curve(new_curve, new_curve_meta)

        assert dataset.get_data(logs=[mnemonic_1, ], start=198, end=198)[198][mnemonic_1] == new_curve[198][mnemonic_1]
        dataset.delete_curve(mnemonic_1)
        curves = dataset.info['Curves']
        assert mnemonic_2 in curves and mnemonic_3 in curves

    def test_get_data_time(self):
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        start = time.time()
        data = dataset.get_data()
        end = time.time()
        assert end - start < 1


class TestWellDatasetColumns(unittest.TestCase):
    def setUp(self) -> None:
        _s = Storage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_and_delete_datasets(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        dataset = WellDatasetColumns(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        assert 'one' in well.datasets
        dataset = WellDatasetColumns(well, "two")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        dataset = WellDatasetColumns(well, "one")
        dataset.delete()
        assert 'one' not in well.datasets
        assert 'two' in well.datasets

    def test_dataset_get_data(self):
        ref_depth = 2000.0880000
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDatasetColumns(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        data = dataset.get_data(start=ref_depth - 0.001, end=ref_depth + 0.001)
        true_answer = {'GR': 46.731338501, 'SP': 63.879390717, 'DTC': 143.6020813, 'PEF': 6.3070640564, 'ROP': 38.207931519, 'RXO': None, 'CALI': 18.639200211,
                       'DRHO': 0.0151574211, 'NPHI': 0.5864710212, 'RDEP': 0.4988202751, 'RHOB': 1.8031704426, 'RMED': 0.4965194166, 'RSHA': None, 'X_LOC': 437627.5625,
                       'Y_LOC': 6470980.0, 'Z_LOC': -1974.846802, 'DEPTH_MD': 2000.0880127, 'MUDWEIGHT': 0.1366020888, 'FORCE_2020_LITHOFACIES_LITHOLOGY': 30000.0,
                       'FORCE_2020_LITHOFACIES_CONFIDENCE': 1.0}
        for key in true_answer.keys():
            assert data[ref_depth][key] == true_answer[key]

    def test_get_data_time(self):
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDatasetColumns(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        start = time.time()
        data = dataset.get_data()
        end = time.time()
        assert end - start < 1

    def test_dataset_insert_row(self):
        wellname = 'random_well'
        dataset_name = 'insert_row'
        reference = 450
        reference_2 = 800
        row = {"GR": 87.81237987, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        row_2 = {"GR": 97.2, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        well = Well(wellname, new=True)
        dataset = WellDatasetColumns(well, dataset_name)
        dataset.register()
        dataset.add_log("GR", float)
        dataset.add_log("PS", float)
        dataset.add_log("LITHO", int)
        dataset.add_log("STRING", str)

        dataset.insert({reference: row})
        assert dataset.get_data(start=reference, end=reference) == {reference: row}
        dataset.insert({reference: row_2})
        assert dataset.get_data(start=reference, end=reference) == {reference: row_2}
        assert dataset.get_data(logs=["GR", "PS"], start=reference, end=reference) == {reference: {"GR": 97.2, "PS": -0.234235555667, }}
        assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}

        dataset.insert({reference_2: row})
        assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}
        dataset.insert({reference_2: row_2})
        assert dataset.get_data(start=reference_2, end=reference_2) == {reference_2: row_2}
        assert dataset.get_data(logs=["GR", "PS"], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, "PS": -0.234235555667, }}
        assert dataset.get_data(logs=["GR", ], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, }}


class TestWellDataset2(unittest.TestCase):
    def setUp(self) -> None:
        _s = Storage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_and_delete_datasets(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        dataset = WellDataset2(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        assert 'one' in well.datasets
        dataset = WellDataset2(well, "two")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        dataset = WellDataset2(well, "one")
        dataset.delete()
        assert 'one' not in well.datasets
        assert 'two' in well.datasets
#
#     def test_dataset_info(self):
#         wellname = 'new_test_well'
#         dataset_name = 'new_test_dataset'
#         well = Well(wellname, new=True)
#         dataset = WellDataset2(well, dataset_name)
#         dataset.register()
#         info = {"step": 0.5}
#         dataset.info = info
#         assert dataset.info == info
#         info_2 = {"step": 0.8}
#         dataset.info = info_2
#         assert dataset.info == info_2
#         well.delete()
#
#     def test_dataset_get_data(self):
#         ref_depth = 2000.0880000
#         wellname = '15_9-13'
#         dataset_name = 'one'
#         well = Well(wellname, new=True)
#         dataset = WellDataset2(well, dataset_name)
#         dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
#         data = dataset.get_data(start=ref_depth - 0.001, end=ref_depth + 0.001)
#         true_answer = {'FORCE_2020_LITHOFACIES_CONFIDENCE': {'2000.088': 1.0}, 'FORCE_2020_LITHOFACIES_LITHOLOGY': {'2000.088': 30000.0}, 'CALI': {'2000.088': 18.639200211},
#                        'MUDWEIGHT': {'2000.088': 0.1366020888}, 'ROP': {'2000.088': 38.207931519}, 'RDEP': {'2000.088': 0.4988202751}, 'RSHA': {'2000.088': -999.25},
#                        'RMED': {'2000.088': 0.4965194166}, 'RXO': {'2000.088': -999.25}, 'SP': {'2000.088': 63.879390717}, 'DTC': {'2000.088': 143.6020813},
#                        'NPHI': {'2000.088': 0.5864710212}, 'PEF': {'2000.088': 6.3070640564}, 'GR': {'2000.088': 46.731338501}, 'RHOB': {'2000.088': 1.8031704426},
#                        'DRHO': {'2000.088': 0.0151574211}, 'DEPTH_MD': {'2000.088': 2000.0880127}, 'X_LOC': {'2000.088': 437627.5625}, 'Y_LOC': {'2000.088': 6470980.0},
#                        'Z_LOC': {'2000.088': -1974.846802}}
#         assert data == true_answer
#         assert 1 == 1
#
#     def test_dataset_insert_row(self):
#         wellname = 'random_well'
#         dataset_name = 'insert_row'
#         reference = 450
#         reference_2 = 800
#         row = {"GR": 87.81237987, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
#         row_2 = {"GR": 97.2, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
#         well = Well(wellname, new=True)
#         dataset = WellDataset2(well, dataset_name)
#         dataset.register()
#         dataset.info = {"step": 0.5}
#
#         dataset.insert(reference, row)
#         assert dataset.get_data(start=reference, end=reference) == {reference: row}
#         dataset.insert(reference, row_2)
#         assert dataset.get_data(start=reference, end=reference) == {reference: row_2}
#         assert dataset.get_data(logs=["GR", "PS"], start=reference, end=reference) == {reference: {"GR": 97.2, "PS": -0.234235555667, }}
#         assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}
#
#         dataset.insert(reference_2, row)
#         assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}
#         dataset.insert(reference_2, row_2)
#         assert dataset.get_data(start=reference_2, end=reference_2) == {reference_2: row_2}
#         assert dataset.get_data(logs=["GR", "PS"], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, "PS": -0.234235555667, }}
#         assert dataset.get_data(logs=["GR", ], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, }}
#
#     def test_add_one_new_log(self):
#         f = '7_1-2 S.las'
#         wellname = f.replace(".las", "")
#         well = Well(wellname, new=True)
#         dataset = WellDataset2(well, "one")
#         dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
#         mnemonic = "NEW_CURVE"
#         new_curve = {
#             198: {mnemonic: 0},
#             198.0164: {mnemonic: 1},
#             198.1684: {mnemonic: 2},
#             198.4724: {mnemonic: 3},
#             198.5: {mnemonic: 4},
#         }
#         new_curve_meta = {mnemonic: {"unit": "ohm.m", "descr": mnemonic, "value": "", "mnemonic": mnemonic, "original_mnemonic": mnemonic}, }
#         dataset.add_curve(new_curve, new_curve_meta)
#
#         assert dataset.get_data(logs=[mnemonic, ], start=198, end=198)[198] == new_curve[198]
#         dataset.delete_curve(mnemonic)
#
#     def test_add_three_new_logs(self):
#         f = '7_1-2 S.las'
#         wellname = f.replace(".las", "")
#         well = Well(wellname, new=True)
#         dataset = WellDataset2(well, "two")
#         dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
#         mnemonic_1 = "NEW_CURVE_1"
#         mnemonic_2 = "NEW_CURVE_2"
#         mnemonic_3 = "NEW_CURVE_3"
#         new_curve = {
#             198: {mnemonic_1: "A0", mnemonic_2: "B0", mnemonic_3: "C0"},
#             198.0164: {mnemonic_1: "A1", mnemonic_2: "B1", mnemonic_3: "C1"},
#             198.1684: {mnemonic_1: "A2", mnemonic_2: "B2", mnemonic_3: "C2"},
#             198.4724: {mnemonic_1: "A3", mnemonic_2: "B3", mnemonic_3: "C3"},
#             198.5: {mnemonic_1: "A4", mnemonic_2: "B4", mnemonic_3: "C4"},
#         }
#         new_curve_meta = {
#             mnemonic: {"unit": "ohm.m", "descr": mnemonic, "value": "", "mnemonic": mnemonic, "original_mnemonic": mnemonic} for mnemonic in [mnemonic_1, mnemonic_2, mnemonic_3]
#         }
#         dataset.add_curve(new_curve, new_curve_meta)
#
#         assert dataset.get_data(logs=[mnemonic_1, ], start=198, end=198)[198][mnemonic_1] == new_curve[198][mnemonic_1]
#         dataset.delete_curve(mnemonic_1)
#         curves = dataset.info['Curves']
#         assert mnemonic_2 in curves and mnemonic_3 in curves
