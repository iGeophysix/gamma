import json
import logging
import os
import pickle
import re
import time
import copy
from typing import Optional, Iterable
from dataclasses import dataclass, field

from celery_conf import app as celery_app, wait_till_completes
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.engine_node import EngineNode

FAMASS_RULES = os.path.join(os.path.dirname(__file__), 'rules', 'FamilyAssignment.json')
FAMASS_RULES_CACHE = os.path.join(os.path.dirname(FAMASS_RULES), 'FamilyAssignerDB.pickle')
TOOLS = os.path.join(os.path.dirname(__file__), 'rules', 'LoggingTools.json')
GARBAGE_TOKENS = {'DL', 'RT', 'SL', 'SIG', 'RAW', 'BP', 'ALT', 'ECO', 'DH', 'NORM'}

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

    class DataBase:
        '''
        Storage for company-specific rules.
        '''

        def __init__(self):
            self.precise_match = {}  # { mnemonic: (family, dimension), ... }
            self.re_match = {}  # { re.Pattern: (family, dimension), ... }
            self.nlpp = NLPParser()

    @dataclass
    class MnemonicInfo:
        '''
        Storage for mnemonic's properties
        '''
        company: str
        family: str
        dimension: str
        tool: str
        reliability: float = field(init=False)

        def __repr__(self):
            return str(vars(self))

    def __init__(self):
        '''
        Loads Energystics family assigning rules.
        '''
        self.db = {}  # {company: FamilyAssigner.DataBase}
        retries = 5
        if os.path.exists(FAMASS_RULES_CACHE) and os.path.getmtime(FAMASS_RULES_CACHE) >= os.path.getmtime(FAMASS_RULES):
            while retries:
                try:
                    with open(FAMASS_RULES_CACHE, 'rb') as f:
                        self.db = pickle.load(f)
                except pickle.UnpicklingError:
                    retries -= 1
                    time.sleep(1)
                    continue
                else:
                    break
        else:
            self.recreate_family_database()

    def recreate_family_database(self) -> None:
        with open(FAMASS_RULES, 'r') as f:
            fa_rules = json.load(f)
        with open(TOOLS, 'r') as f:
            tools = json.load(f)
        for company, rules in fa_rules.items():
            cdb = self.db[company] = FamilyAssigner.DataBase()  # initialize company FamilyAssigner db
            for mnemonics, dimension, family in rules:
                for mnemonic in mnemonics:
                    tool = tools.get(company, {}).get(mnemonic)     # source logging tool for mnemonic
                    mnemonic_info = FamilyAssigner.MnemonicInfo(company, family, dimension, tool)
                    if '*' in mnemonic or '?' in mnemonic:
                        re_mask = mnemonic.replace('*', '.*').replace('?', '.')
                        re_pattern = re.compile(re_mask, re.IGNORECASE)
                        cdb.re_match[re_pattern] = mnemonic_info
                    else:
                        cdb.precise_match[mnemonic] = mnemonic_info
                    cdb.nlpp.learn(mnemonic, mnemonic_info)
        with open(FAMASS_RULES_CACHE, 'wb') as f:
            pickle.dump(self.db, f)

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

    def assign_family(self, mnemonic: str, one_best: bool = True, company: str = None) -> Optional[MnemonicInfo]:
        '''
        Returns MnemonicInfo with company, log family, dimension, detection reliability for the mnemonic using Energystics rules.
        Gives all the variants if one_best=False.
        company limits log dictionary to the particular service company.
        '''
        if mnemonic:
            mnemonic = mnemonic.upper()
            results = []
            for cc in [company] if company is not None else self.db:
                db = self.db[cc]
                res = None

                # exact matching
                mnemonic_info = db.precise_match.get(mnemonic)
                if mnemonic_info is not None:
                    res = copy.copy(mnemonic_info)
                    # reliability for a shortest 1-letter mnemonic is MAX_RANK_EXACT_MATCHING * 0.1, reliability for +INF-letter mnemonic is MAX_RANK_EXACT_MATCHING
                    res.reliability = MAX_RELIABILITY_EXACT_MATCHING * (1 - 0.9 / len(mnemonic) ** 2)

                # pattern matching
                if res is None:
                    for re_pattern in db.re_match:
                        if re_pattern.fullmatch(mnemonic):
                            res = copy.copy(db.re_match[re_pattern])
                            res.reliability = MAX_RELIABILITY_PATTERN_MATCHING
                            break

                # NLP
                if res is None:
                    nlp_res = self._nlp_assign_family(mnemonic, cc)
                    if nlp_res is not None:
                        family_info, reliability = nlp_res[0]
                        res = copy.copy(family_info)
                        res.reliability = reliability * MAX_RELIABILITY_NLP_MATCHING     # decrease maximum NLP confidence

                if res is not None:
                    results.append(res)

            if results:
                if company is not None:
                    return results[0]   # we have only one result
                else:
                    # remove family duplicates
                    results.sort(key=lambda mnemonic_info: mnemonic_info.reliability, reverse=True)
                    uniq_family_results = []
                    for mnemonic_info in results:
                        already_have = None
                        for unic_mnemonic_info in uniq_family_results:
                            if mnemonic_info.family == unic_mnemonic_info.family:   # do we already have this family in results?
                                already_have = unic_mnemonic_info
                                break
                        if already_have is not None:
                            # combine two variants
                            # increase reliability by a part of the second variant reliability, drop second source variant
                            already_have.reliability = min(1, already_have.reliability + mnemonic_info.reliability / len(results))
                        else:
                            uniq_family_results.append(mnemonic_info)  # just add the variant

                    uniq_family_results.sort(key=lambda mnemonic_info: mnemonic_info.reliability, reverse=True)
                    if one_best:
                        return uniq_family_results[0]
                    else:
                        return uniq_family_results
        return None

    def assign_families(self, mnemonics: Iterable[str]) -> dict:
        '''
        Returns {mnemonic: MnemonicInfo} for the mnemonic using Energystics rules.
        Assumes that logs are mostly produced by a one service company.
        '''
        all_company_mnemonic_info = {m: self.assign_family(m, one_best=False) for m in mnemonics}
        companies = []
        for mnemonic_infos in all_company_mnemonic_info.values():
            if mnemonic_infos is not None:
                for mnemonic_info in mnemonic_infos:
                    companies.append(mnemonic_info.company)
        company_count = {cc: companies.count(cc) for cc in set(companies)}
        for mnemonic, mis in all_company_mnemonic_info.items():
            if mis is not None:
                # sort starting from the highest rank, then by company share
                smis = sorted(mis, key=lambda mi: (mi.reliability, company_count[mi.company]), reverse=True)
                all_company_mnemonic_info[mnemonic] = smis[0]  # update with the best variant
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
