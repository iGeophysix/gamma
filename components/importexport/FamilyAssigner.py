import logging
import pickle
import re
import copy
from typing import Optional, Iterable, Union
import dataclasses

from celery_conf import app as celery_app, wait_till_completes
from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.engine.engine_node import EngineNode
from components.importexport.UnitsSystem import UnitsSystem

GARBAGE_TOKENS = {'DL', 'SL', 'SIG', 'RAW', 'BP', 'ALT', 'ECO', 'DH', 'NORM'}

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

    def match(self, mnemonic: str) -> tuple:
        '''
        Return [list of possible mnemonc's data variants], match rank.
        '''
        best_rank = 0
        best_variants = []
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng = self.ngram(token)
            # search
            for ng_ref, data in self.db:
                match_rank = len(ng.intersection(ng_ref)) / len(ng.union(ng_ref))  # rank is (number of matched N-grams) / (all N-grams)
                if match_rank > 0:
                    if match_rank > best_rank:
                        best_rank = match_rank
                        best_variants = [data]
                    elif match_rank == best_rank:
                        best_variants.append(data)
        return best_variants, best_rank


class FamilyAssigner:
    '''
    Main family assigner class.
    '''
    TABLE_NAME = "FamilyAssigner"

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
        tool: str

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
                dif = self._compare(a, b)
                if dif < 0:
                    return True
                elif dif > 0:
                    return False
            return False

    def __init__(self):
        '''
        Loads family assigning rules.
        '''
        self.units = UnitsSystem()
        s = RedisStorage()
        self.db = pickle.loads(s.object_get(self.TABLE_NAME))

    def _nlp_assign_family(self, mnemonic: str, company: str = None) -> Optional[MnemonicInfo]:
        '''
        Get family using NLP.
        '''
        company_result = {}
        for cc in [company] if company is not None else self.db:
            db = self.db[cc]
            family_infos, rank = db.nlpp.match(mnemonic)
            company_result[cc] = [(fi, rank) for fi in family_infos] if family_infos else None
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
                    res = copy.copy(mnemonic_info)
                    # reliability for a shortest 1-letter mnemonic is MAX_RANK_EXACT_MATCHING * 0.1, reliability for +INF-letter mnemonic is MAX_RANK_EXACT_MATCHING
                    res.reliability = MAX_RELIABILITY_EXACT_MATCHING * (1 - 0.9 / len(mnemonic) ** 2)
                    results.append(res)

                # pattern matching
                for re_pattern in db.re_match:
                    if re_pattern.fullmatch(mnemonic):
                        res = copy.copy(db.re_match[re_pattern])
                        res.reliability = MAX_RELIABILITY_PATTERN_MATCHING
                        results.append(res)

                # NLP
                nlp_res = self._nlp_assign_family(mnemonic, cc)
                if nlp_res is not None:
                    for family_info, reliability in nlp_res:
                        res = copy.copy(family_info)
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
                if already_have.tool != mnemonic_info.tool:
                    already_have.tool = None  # no particular source tool
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
    def run(cls):
        """
        Run calculations
        """
        p = Project()
        tasks = []
        for well_name in p.list_wells():
            well = Well(well_name)
            for dataset_name in well.datasets:
                tasks.append(celery_app.send_task('tasks.async_recognize_family', (well_name, [dataset_name, ],)))

        wait_till_completes(tasks)
