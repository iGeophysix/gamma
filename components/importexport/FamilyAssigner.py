import copy
import dataclasses
import hashlib
import json
import logging
import pickle
import re
from typing import Optional, Iterable, Union

from celery_conf import app as celery_app
from components.database.RedisStorage import RedisStorage, FAMILY_ASSIGNER_TABLE_NAME
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from components.importexport.UnitsSystem import UnitsSystem

GARBAGE_TOKENS = {'DL', 'SL', 'SIG', 'RAW', 'BP', 'ALT', 'ECO', 'DH', 'NORM'}

EXCLUDED_DATASETS = ['LQC', 'Survey']
MAX_RELIABILITY_EXACT_MATCHING = 1
MAX_RELIABILITY_PATTERN_MATCHING = 0.8
MAX_RELIABILITY_NLP_MATCHING = 0.7


class NLPParser:
    '''
    Family recongition uning Natural Language Processing.
    '''

    def __init__(self):
        self.db = []  # ({N-grams}, family), ...

    def mnemonic_tokenize(self, s: str) -> list:
        '''
        Split mnemonic to clean up garbage.
        '''
        return [p for p in re.split(r'[_\W]+', s) if p and p not in GARBAGE_TOKENS]

    def ngram(self, s: str) -> set:
        '''
        Create 2-letters N-grams with sorted letters (order insensitivity)
        '''
        ngs = set()
        for n in range(len(s) - 1):
            ng = s[n: n + 2]
            if ng.isalpha():  # symbols permutation is allowed for letters only
                ng = ''.join(sorted(ng))
            ngs.add(ng)
        return ngs

    def learn(self, mnemonic: str, data) -> None:
        '''
        Learn another one mnemonic-family pair.
        '''
        ng = set()
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng.update(self.ngram(token))
        if ng:
            self.db.append((ng, data))

    def match(self, mnemonic: str) -> list:
        '''
        Returns list of possible mnemonc's data variants with rank: [(match_rank, mnemonc data), ...]
        '''
        best_rank = 0
        best_variants = []
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng = self.ngram(token)
            # search
            for ng_ref, data in self.db:
                match_rank = len(ng.intersection(ng_ref)) / len(ng.union(ng_ref))  # rank is (number of matched N-grams) / (all N-grams)
                if match_rank > best_rank / 2:
                    best_variants.append((match_rank, data))
                    if match_rank > best_rank:
                        best_rank = match_rank
        best_variants.sort(reverse=True)
        # 100 best variants max that have match_rank > best_rank / 2
        return [best_variants[n] for n in range(min(100, len(best_variants))) if best_variants[n][0] > best_rank / 2]


class FamilyAssigner:
    '''
    Main family assigner class.
    '''
    class DataBase:
        '''
        Storage for company-specific rules.
        '''

        def __init__(self):
            self.precise_match = {}  # { mnemonic: (family, dimension), ... }
            self.re_match = {}  # { re.Pattern: (family, dimension), ... }
            self.nlpp = NLPParser()

    @dataclasses.dataclass
    class MnemonicInfo:
        '''
        Storage for mnemonic's properties
        '''
        reliability: float = dataclasses.field(init=False, default=0)
        family: str
        dimension: str
        company: str
        optional_properties: dict

        @staticmethod
        def _compare(a, b) -> int:
            '''
            Two values comparator with None support
            :return: 0 if a == b
            :return: negative value if a < b
            :return: positive value if a > b
            '''
            if a is None:
                if b is None:
                    return 0  # None vs None
                else:
                    return -1  # None vs not None
            elif b is None:
                return 1  # not None vs None
            elif a == b:
                return 0
            elif a > b:
                return 1
            else:
                return -1

        def __lt__(self, other):
            a_values = dataclasses.astuple(self)
            b_values = dataclasses.astuple(other)
            for a, b in zip(a_values, b_values):
                if isinstance(a, dict) and isinstance(b, dict):  # for optional_properties field
                    all_keys = set(a.keys()) | set(b.keys())
                    if 'tool' in all_keys:
                        all_keys.remove('tool')
                        all_keys = ['tool'] + sorted(all_keys)  # sorts by tool first
                    else:
                        all_keys = sorted(all_keys)
                    for key in all_keys:
                        dif = self._compare(a.get(key), b.get(key))
                        if dif == 0:
                            continue
                        else:
                            return dif < 0
                else:
                    dif = self._compare(a, b)
                    if dif == 0:
                        continue
                    else:
                        return dif < 0
            return False

    def __init__(self):
        '''
        Loads family assigning rules.
        '''
        s = RedisStorage()
        assert s.object_exists(FAMILY_ASSIGNER_TABLE_NAME), 'Common data is absent'
        self.db = pickle.loads(s.object_get(FAMILY_ASSIGNER_TABLE_NAME))
        self.units = UnitsSystem()

    def _nlp_assign_family(self, mnemonic: str, company: str = None) -> Optional[Iterable[tuple[float, MnemonicInfo]]]:
        '''
        Get family using NLP.
        '''
        company_result = {}
        for cc in [company] if company is not None else self.db:
            db = self.db[cc]
            company_result[cc] = db.nlpp.match(mnemonic)
        if company_result:
            if company is not None:
                return company_result[company]
            else:
                return company_result
        return None

    def assign_family(self, mnemonic: str, unit: str, one_best: bool = True, company: str = None) -> Union[MnemonicInfo, Iterable[MnemonicInfo], None]:
        '''
        Returns MnemonicInfo with company, log family, dimension, detection reliability for the mnemonic and unit.
        :param mnemonic: str, log name
        :param unit: str, log unit or None
        :param one_best: bool, return only one best variant (True) or all varians (False)
        :param company: str, filter returned variants by service company name or get them all (None)
        :return: if one_best=True, MnemonicInfo with corresponding information or None if no match found
        :return: if one_best=False, list of MnemonicInfo
        '''
        results = []
        if mnemonic:
            mnemonic = mnemonic.upper()
            for cc in [company] if company is not None else self.db:
                db = self.db[cc]

                # exact matching
                mnemonic_info = db.precise_match.get(mnemonic)
                if mnemonic_info is not None:
                    res = copy.deepcopy(mnemonic_info)
                    # reliability for a shortest 1-letter mnemonic is MAX_RANK_EXACT_MATCHING * 0.1, reliability for +INF-letter mnemonic is MAX_RANK_EXACT_MATCHING
                    res.reliability = MAX_RELIABILITY_EXACT_MATCHING * (1 - 0.9 / len(mnemonic) ** 2)
                    results.append(res)

                # pattern matching
                for re_pattern in db.re_match:
                    if re_pattern.fullmatch(mnemonic):
                        res = copy.deepcopy(db.re_match[re_pattern])
                        res.reliability = MAX_RELIABILITY_PATTERN_MATCHING
                        results.append(res)

                # NLP
                nlp_res = self._nlp_assign_family(mnemonic, cc)
                if nlp_res is not None:
                    for reliability, family_info in nlp_res:
                        res = copy.deepcopy(family_info)
                        res.reliability = reliability * MAX_RELIABILITY_NLP_MATCHING  # decrease maximum NLP confidence
                        results.append(res)

        # filter results by unit
        if unit is not None and self.units.known_unit(unit):
            required_unit_dimention = self.units.unit_dimension(unit)
            results = list(filter(lambda mnemonic_info: mnemonic_info.dimension == required_unit_dimention, results))  # convertable units have equal unit dimension

        # remove family duplicates
        results.sort(reverse=True)  # high reliability first
        uniq_family_results = []
        for mnemonic_info in results:
            already_have = None
            for unic_mnemonic_info in uniq_family_results:
                if mnemonic_info.family == unic_mnemonic_info.family:  # do we already have this family in results?
                    already_have = unic_mnemonic_info
                    break
            if already_have is not None:
                # combine two variants
                # increase reliability by a part of the second variant reliability, drop second source variant
                already_have.reliability = min(1, already_have.reliability + mnemonic_info.reliability / len(results))
                if already_have.company != mnemonic_info.company:
                    already_have.company = None  # no particular source company
                if 'tool' in already_have.optional_properties and already_have.optional_properties['tool'] != mnemonic_info.optional_properties.get('tool'):
                    del already_have.optional_properties['tool']  # no particular logging tool
            else:
                uniq_family_results.append(mnemonic_info)  # just add the variant

        uniq_family_results.sort(reverse=True)  # high reliability first
        if one_best:
            return uniq_family_results[0] if uniq_family_results else None
        else:
            return uniq_family_results

    def assign_families(self, mnemonic_unit: Iterable[tuple[str, str]]) -> dict:
        '''
        Batch family assigning by set of mnemonics and units.
        Assumes that logs are mostly produced by a one service company.
        :param mnemonic_unit: list of (mnemonic, unit) pairs
        :return: {mnemonic: MnemonicInfo or None} for all mnemonic_unit pairs.
        '''
        all_company_mnemonic_info = {m: self.assign_family(m, u, one_best=False) for m, u in mnemonic_unit}
        companies = []
        for mnemonic_infos in all_company_mnemonic_info.values():
            if mnemonic_infos is not None:
                for mnemonic_info in mnemonic_infos:
                    companies.append(mnemonic_info.company)
        company_count = {cc: companies.count(cc) for cc in set(companies)}
        for mnemonic, mis in all_company_mnemonic_info.items():
            if mis:
                # sort adjusting reliability by company share
                smis = sorted(mis, key=lambda mi: (mi.reliability + (company_count[mi.company] / len(companies) / 10), mi.family), reverse=True)
                all_company_mnemonic_info[mnemonic] = smis[0]  # update with the best variant
            else:
                all_company_mnemonic_info[mnemonic] = None
        return all_company_mnemonic_info


class FamilyAssignerNode(EngineNode):
    """
    This engine node wraps FamilyAssigner algorithm
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        return 1

    class Meta:
        name = 'Family Assigner'
        input = {
            "type": BasicLog,
        }
        output = {
            "type": BasicLog,
        }

    @classmethod
    def validate_input(cls, log: BasicLog):
        """
        Check if input is valid for this. If all is OK - return None, else raise an exception
        :param log:
        """
        assert isinstance(log, BasicLog), "log must be instance of BasicLog class"

    @classmethod
    def run_for_item(cls, **kwargs):
        """
        Recognize log family in well datasets
        :param wellname:
        :return:
        """
        wellname = kwargs['wellname']
        w = Well(wellname)
        fa = FamilyAssigner()

        for datasetname in w.datasets:
            if datasetname in EXCLUDED_DATASETS:
                continue
            wd = WellDataset(w, datasetname)
            for log_name in wd.log_list:
                log = BasicLog(wd.id, log_name)
                if 'raw' not in log.meta.tags:
                    continue
                result = fa.assign_family(log.name, log.meta.units)
                if result is not None:
                    if 'tags' in result.optional_properties:
                        op_tags = result.optional_properties.pop('tags')
                        log.meta.add_tags(*op_tags)
                    log.meta.family = result.family
                    log.meta.family_assigner = {
                        'reliability': result.reliability,
                        'unit_class': result.dimension,
                        'logging_company': result.company
                    } | result.optional_properties
                else:
                    log.meta.family = log.meta.family_assigner = None
                log.save()

    @classmethod
    def item_hash(cls, well_name) -> tuple[str, bool]:
        """Get item hash to use in cache"""
        w = Well(well_name)
        log_hashes = []
        valid = True
        for ds_id in w.datasets:
            if ds_id in EXCLUDED_DATASETS:
                continue

            ds = WellDataset(w, ds_id)
            for log_id in ds.log_list:
                log = BasicLog(ds.id, log_id)
                necessary_meta = {k: log.meta[k] for k in ['name', 'description', 'units', ]}
                log_hashes.append(hashlib.md5(json.dumps(necessary_meta).encode()).hexdigest())
                if not hasattr(log.meta, 'family'):
                    valid = False

        log_hashes = sorted(log_hashes)
        well_hash = hashlib.md5(json.dumps(log_hashes).encode()).hexdigest()
        return well_hash, valid

    @classmethod
    def run(cls, **kwargs):
        """
        Run calculations
        """
        p = Project()
        tasks = []
        cache = EngineNodeCache(cls)
        hashes = []
        cache_hits = 0
        for well_name in p.list_wells():
            item_hash, valid = cls.item_hash(well_name)
            hashes.append(item_hash)
            if valid and item_hash in cache:
                cache_hits += 1
                continue
            tasks.append(celery_app.send_task('tasks.async_recognize_family', (well_name,)))

        cache.set(hashes)
        cls.logger.info(f'Node: {cls.name()}: cache hits:{cache_hits} / misses: {len(tasks)}')
        cls.track_progress(tasks, cached=cache_hits)
