import logging
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor

import celery_conf
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from components.importexport.FamilyProperties import FamilyProperties
from settings import LOGGING_LEVEL


class LogReconstructionNode(EngineNode):
    '''
    This EngineNodes restores missing logs in wells basing on ML algorithms trained on other wells in the project
    '''

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        return 1

    logger = logging.getLogger(__name__)
    logger.setLevel(LOGGING_LEVEL)

    @classmethod
    def __check_well_is_valid_for_training_and_get_output_units(cls, ds, well_name, log_family_to_predict, log_families_to_train):
        log_to_predict_units = None
        # check if well has good target log
        target_logs = ds.get_log_list(family=log_family_to_predict)
        good_target_log = None
        for target_log in target_logs:
            l = BasicLog(ds.id, target_log)
            if not 'reconstructed' in l.meta.tags:
                good_target_log = target_log
                # TODO: get better solution once we define default units for each family
                log_to_predict_units = l.meta.units
                break

        if not good_target_log:
            cls.logger.debug(f"Well {well_name} doesn't have log of family {log_family_to_predict} for train")
            return False, None

        # check if well has good feature logs
        feature_families = {fam: ds.get_log_list(family=fam) for fam in log_families_to_train}
        good_feature_logs = {fam: None for fam in log_families_to_train}
        for family, feature_logs in feature_families.items():
            for feature_log in feature_logs:
                l = BasicLog(ds.id, feature_log)
                if not 'reconstructed' in l.meta.tags:
                    good_feature_logs[l.meta.family] = feature_log

        if not all(good_feature_logs.values()):
            cls.logger.debug(f"Well {well_name} doesn't have log of family {log_families_to_train} for train")
            return False, None

        return True, log_to_predict_units

    @classmethod
    def _fit(cls, well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs=None):
        well_names_to_train = []
        max_count_wells_to_train = int(len(well_names) * percent_of_wells_to_train)
        for well_name in well_names:
            well = Well(well_name)
            ds = WellDataset(well, 'LQC')
            well_is_valid, units = cls.__check_well_is_valid_for_training_and_get_output_units(ds, well_name, log_family_to_predict, log_families_to_train)
            if not well_is_valid:
                continue

            well_names_to_train.append(well_name)

            if len(well_names_to_train) >= max_count_wells_to_train:
                break

        if not well_names_to_train:
            cls.logger.info(f"Not found wells to restore synthetic {log_family_to_predict} on {log_families_to_train}")
            return None, None

        # get train data
        log_data = pd.DataFrame()
        required_families = [*log_families_to_train, *[log_family_to_predict, ]]
        for well_name in well_names_to_train:
            well = Well(well_name)
            well_dataset = WellDataset(well, 'LQC')
            logs_in_well = []
            for log_id in well_dataset.log_list:
                log = BasicLog(well_dataset.id, log_id)
                if hasattr(log.meta, 'family') \
                        and log.meta.family in required_families \
                        and not 'reconstructed' in log.meta.tags:
                    logs_in_well.append(log)

            # interpolate to common reference
            interp_logs = cls._prepare_data(logs_in_well)
            log_data = pd.concat((log_data, interp_logs), axis=0, ignore_index=True)

        # train
        try:
            features = log_data[log_families_to_train]
            target = log_data[log_family_to_predict]

            model = CatBoostRegressor(**model_kwargs)  # looses a lot of details
            model.fit(X=features, y=target)
        except Exception as exc:
            cls.logger.critical(f"Unexpected failure: well_names_to_train={well_names_to_train},\n"
                                f"log_families_to_train = {log_families_to_train}\n"
                                f"log_family_to_predict = {log_family_to_predict}")
            raise exc
        return model, units

    @staticmethod
    def _predict(model, input_logs):
        df = LogReconstructionNode._prepare_data(input_logs)
        features = df[[log.meta.family for log in input_logs]]
        try:
            result = model.predict(features)
            return np.vstack((df['__depth'], result)).T
        except:
            LogReconstructionNode.logger.warning(f"Something went wrong in this dataset:{[(log.dataset_id, log.name) for log in input_logs]}")
            raise

    @staticmethod
    def _prepare_data(logs):

        # get min step
        step = np.nanmin([log.meta['basic_statistics']['avg_step'] for log in logs])

        # define top and bottom of the spliced log
        min_depth = np.nanmin([log.meta['basic_statistics']['min_depth'] for log in logs])
        max_depth = np.nanmax([log.meta['basic_statistics']['max_depth'] for log in logs])
        new_md = np.arange(min_depth, max_depth + step, step)

        interp_logs = defaultdict(np.array)
        interp_logs['__depth'] = new_md
        for log in logs:
            interp_logs[log.meta.family] = log.interpolate(new_md)[:, 1]
        df = pd.DataFrame(interp_logs).dropna()
        return df

    @staticmethod
    def _valid_log(log, log_families_to_train) -> bool:
        return hasattr(log.meta, 'family') and \
               log.meta.family in log_families_to_train \
               and 'reconstructed' not in log.meta.tags

    @staticmethod
    def _predicted_log(log, log_family_to_predict) -> bool:
        return hasattr(log.meta, 'family') and \
               log.meta.family in log_family_to_predict \
               and 'reconstructed' in log.meta.tags

    @classmethod
    def run_for_item(cls, well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs):
        """Run log reconstruction for family"""
        # train
        model, log_to_predict_units = cls._fit(well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs)
        if model is None:
            raise ValueError("Cannot train model on this data")

        FAMILY_PROPERTIES = FamilyProperties()
        for well_name in well_names:
            # predict
            well = Well(well_name)
            well_dataset = WellDataset(well, 'LQC')
            input_logs = []
            required_log_families = list(log_families_to_train)
            for log_id in well_dataset.log_list:
                log = BasicLog(well_dataset.id, log_id)
                if cls._valid_log(log, log_families_to_train):
                    input_logs.append(log)
                    required_log_families.remove(log.meta.family)

            if required_log_families:
                cls.logger.info(f"Well {well_name} misses log of families {required_log_families} to predict {log_family_to_predict}")
                return

            if not input_logs:
                cls.logger.info(f"Well {well_name} misses log of families {log_families_to_train} to predict {log_family_to_predict}")
                return

            family_meta = FAMILY_PROPERTIES[log_family_to_predict]

            new_log = BasicLog(well_dataset.id, f"{family_meta.get('mnemonic', log_family_to_predict)}_SYNTH")

            new_log.values = cls._predict(model, input_logs)
            new_log.meta.family = log_family_to_predict
            new_log.meta.name = f"{family_meta.get('mnemonic', log_family_to_predict)}_SYNTH"
            new_log.meta.units = log_to_predict_units
            new_log.meta.add_tags('reconstructed')
            cls.write_history(log=new_log, input_logs=input_logs, parameters={"algorithm": "Catboost model"})
            new_log.save()
            cls.logger.debug(f'Created synthetic {log_family_to_predict} in well {well_name}')

    @classmethod
    def item_hash(cls, *args) -> tuple[str, bool]:
        """Get item hash"""
        well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs = args
        log_hashes = []
        valid = True
        for well_name in well_names:
            well = Well(well_name)
            well_dataset = WellDataset(well, 'LQC')
            well_already_has_predicted_log = False
            for log_id in well_dataset.log_list:
                log = BasicLog(well_dataset.id, log_id)
                if cls._valid_log(log, log_families_to_train):
                    log_hashes.append(log.meta.data_hash)
                if cls._predicted_log(log, log_family_to_predict):
                    well_already_has_predicted_log = True
            valid = valid and well_already_has_predicted_log

        item_hash_input = {
            "log_hashes": tuple(sorted(log_hashes)),
            "log_families_to_train": log_families_to_train,
            "log_family_to_predict": log_family_to_predict,
            "percent_of_wells_to_train": percent_of_wells_to_train,
            "model_kwargs": model_kwargs
        }
        out_hash = cls.item_md5(item_hash_input)

        return out_hash, valid

    @classmethod
    def run(cls, **kwargs):
        """Launch node calculation"""

        cases_to_predict = kwargs.values()

        p = Project()
        well_names = list(p.list_wells().keys())

        # create ML model
        tasks = []
        hashes = []
        cache_hits = 0
        cache = EngineNodeCache(cls)
        for case_to_predict in cases_to_predict:
            log_families_to_train = case_to_predict.get('log_families_to_train')
            log_families_to_predict = case_to_predict.get('log_families_to_predict')
            model_kwargs = case_to_predict.get('model_kwargs', None)
            percent_of_wells_to_train = case_to_predict.get('percent_of_wells_to_train', 0.2)

            for log_family_to_predict in log_families_to_predict:
                # get wells having log families to train and predict in LQC
                item_hash, item_hash_is_valid = cls.item_hash(well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs)
                hashes.append(item_hash)
                if item_hash_is_valid and item_hash in cache:
                    cache_hits += 1
                    continue

                tasks.append(celery_conf.app.send_task('tasks.async_log_reconstruction',
                                                       (well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs)
                                                       )
                             )

        cache.set(hashes)
        cls.logger.info(f'Node: {cls.name()}: cache hits:{cache_hits} / misses: {len(tasks)}')
        cls.track_progress(tasks, cached=cache_hits)

    @classmethod
    def write_history(cls, **kwargs):
        """write history to the log"""
        log = kwargs['log']
        input_logs = kwargs['input_logs']
        parameters = kwargs['parameters']

        log.meta.append_history({'node': cls.name(),
                                 'node_version': cls.version(),
                                 'timestamp': datetime.now().isoformat(),
                                 'parent_logs': [(log.dataset_id, log.name) for log in input_logs],
                                 'parameters': parameters
                                 })


if __name__ == '__main__':
    node = LogReconstructionNode()
    parameters = {
        "0": {
            "log_families_to_train": [
                "Gamma Ray",
                "Bulk Density"
            ],
            "log_families_to_predict": [
                "Thermal Neutron Porosity"
            ],
            "model_kwargs": {
                "iterations": 50,
                "depth": 12,
                "learning_rate": 0.1,
                "loss_function": "MAPE",
                "allow_writing_files": False,
                "logging_level": "Silent"
            }
        },
        "1": {
            "log_families_to_train": [
                "Bulk Density",
                "Thermal Neutron Porosity"
            ],
            "log_families_to_predict": [
                "Gamma Ray"
            ],
            "model_kwargs": {
                "iterations": 50,
                "depth": 12,
                "learning_rate": 0.1,
                "loss_function": "MAPE",
                "allow_writing_files": False,
                "logging_level": "Silent"
            }
        },
        "2": {
            "log_families_to_train": [
                "Gamma Ray",
                "Thermal Neutron Porosity"
            ],
            "log_families_to_predict": [
                "Bulk Density"
            ],
            "model_kwargs": {
                "iterations": 50,
                "depth": 12,
                "learning_rate": 0.1,
                "loss_function": "MAPE",
                "allow_writing_files": False,
                "logging_level": "Silent"
            }
        }
    }
    node.run(**parameters)
