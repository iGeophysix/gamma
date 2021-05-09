import json
import logging
import operator
import os
import pickle
import re

from celery_conf import app as celery_app, wait_till_completes
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.engine_node import EngineNode

FAMASS_RULES = os.path.join(os.path.dirname(__file__), 'rules', 'FamilyAssignment.json')
FAMASS_RULES_CACHE = os.path.join(os.path.dirname(FAMASS_RULES), 'FamilyAssignerDB.pickle')
GARBAGE_TOKENS = {'DL', 'RT', 'SL', 'SIG', 'RAW', 'BP', 'ALT', 'ECO', 'DH', 'NORM'}

MAX_RANK_EXACT_MATCHING = 1
MAX_RANK_PATTERN_MATCHING = 0.8
MAX_RANK_NLP_MATCHING = 0.7


class NLPParser:
    '''
    Family recongition uning Natural Language Processing.
    '''

    def __init__(self):
        self.db = []  # ({N-grams}, family), ...

    def mnemonic_tokenize(self, s):
        '''
        Split mnemonic to clean up garbage.
        '''
        return [p for p in re.split(r'[_\W]+', s) if p and p not in GARBAGE_TOKENS]

    def ngram(self, s):
        '''
        Create 2-letters N-grams with sorted letters (order insensitivity)
        '''
        ngs = set()
        for n in range(len(s) - 1):
            ng = s[n: n + 2]
            if ng.isalpha():    # symbols permutation is allowed for letters only
                ng = ''.join(sorted(ng))
            ngs.add(ng)
        return ngs

    def learn(self, mnemonic, family):
        '''
        Learn another one mnemonic-family pair.
        '''
        ng = set()
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng.update(self.ngram(token))
        if ng:
            self.db.append((ng, family))

    def family_variants(self, mnemonic):
        '''
        Return [list of possible families by mnemonic], math rank.
        '''
        best_rank = 0
        best_families = []
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng = self.ngram(token)
            # search
            for ng_ref, family in self.db:
                match_rank = len(ng.intersection(ng_ref)) / len(ng.union(ng_ref))  # rank is (number of matched N-grams) / (all N-grams)
                if match_rank > 0:
                    if match_rank > best_rank:
                        best_rank = match_rank
                        best_families = [family]
                    elif match_rank == best_rank:
                        best_families.append(family)
        return best_families, best_rank


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

    def __init__(self):
        '''
        Loads Energystics family assigning rules.
        '''
        self.db = {}  # {company: FamilyAssigner.DataBase}
        if os.path.exists(FAMASS_RULES_CACHE) and os.path.getmtime(FAMASS_RULES_CACHE) >= os.path.getmtime(FAMASS_RULES):
            with open(FAMASS_RULES_CACHE, 'rb') as f:
                self.db = pickle.load(f)
        else:
            self.recreate_family_database()

    def recreate_family_database(self):
        with open(FAMASS_RULES, 'r') as f:
            fa_rules = json.load(f)
        for company, rules in fa_rules.items():
            cdb = self.db[company] = FamilyAssigner.DataBase()  # initialize company FamilyAssigner db
            for mnemonics, dimension, family in rules:
                for mnemonic in mnemonics:
                    mnemonic_info = (family, dimension)
                    if '*' in mnemonic or '?' in mnemonic:
                        re_mask = mnemonic.replace('*', '.*').replace('?', '.')
                        re_pattern = re.compile(re_mask, re.IGNORECASE)
                        cdb.re_match[re_pattern] = mnemonic_info
                    else:
                        cdb.precise_match[mnemonic] = mnemonic_info
                    cdb.nlpp.learn(mnemonic, mnemonic_info)
        with open(FAMASS_RULES_CACHE, 'wb') as f:
            pickle.dump(self.db, f)

    def _nlp_assign_family(self, mnemonic, company=None):
        '''
        Get family using NLP.
        '''
        company_result = {}
        for cc in [company] if company is not None else self.db:
            db = self.db[cc]
            family_infos, rank = db.nlpp.family_variants(mnemonic)
            company_result[cc] = [(*fi, rank) for fi in family_infos] if family_infos else None
        if company_result:
            if company is not None:
                return company_result[company]
            else:
                return company_result
        return None

    def assign_family(self, mnemonic, one_best=True, company=None):
        '''
        Returns a (log family, dimension, detection reliability) for the mnemonic using Energystics rules.
        Gives all the variants if one_best=False.
        company limits log dictionary to a particuler service company.
        '''
        if mnemonic:
            mnemonic = mnemonic.upper()
            company_result = {}
            for cc in [company] if company is not None else self.db:
                db = self.db[cc]
                res = None

                # exact matching
                mnemonic_info = db.precise_match.get(mnemonic)
                if mnemonic_info is not None:
                    res = *mnemonic_info, MAX_RANK_EXACT_MATCHING * (1 - 0.9 / len(mnemonic)**2)   # rank for a shortest 1-letter mnemonic is MAX_RANK_EXACT_MATCHING * 0.1, rank for +INF-letter mnemonic is MAX_RANK_EXACT_MATCHING

                # pattern matching
                if res is None:
                    for re_pattern, mnemonic_info in db.re_match.items():
                        if re_pattern.fullmatch(mnemonic):
                            res = *mnemonic_info, MAX_RANK_PATTERN_MATCHING

                # NLP
                if res is None:
                    nlp_res = self._nlp_assign_family(mnemonic, cc)
                    if nlp_res is not None:
                        # res = [(mnemonic_info[0], mnemonic_info[1], mnemonic_info[2] * 0.7) for mnemonic_info in nlp_res]   # decrease maximum NLP confidence  # TODO: use all the variants
                        family, dimension, rank = nlp_res[0]
                        res = (family, dimension, rank * MAX_RANK_NLP_MATCHING)  # decrease maximum NLP confidence

                if res is not None:
                    company_result[cc] = res

            if company_result:
                if company is not None:
                    return company_result.get(company)
                else:
                    # remove family duplicates
                    uniq_family_results = {}
                    for company, mnemonic_info in company_result.items():
                        found_same_family_at_cc = None
                        for unic_company, unic_mnemonic_info in uniq_family_results.items():
                            if mnemonic_info[0] == unic_mnemonic_info[0]:  # compare by family
                                found_same_family_at_cc = unic_company
                                break
                        if found_same_family_at_cc is not None:
                            unic_mnemonic_info = uniq_family_results[found_same_family_at_cc]
                            newRank = min(1, unic_mnemonic_info[2] + mnemonic_info[2] / len(company_result))  # increase rank by a part of the second source rank
                            uniq_family_results[found_same_family_at_cc] = (unic_mnemonic_info[0], unic_mnemonic_info[1], newRank)  # update rank, drop second source variant
                        else:
                            uniq_family_results[company] = mnemonic_info  # just add the variant

                    uniq_family_results = dict(sorted(uniq_family_results.items(), key=lambda pair: pair[1][2], reverse=True))     # sort dictionary results by rank
                    if one_best:
                        return next(iter(uniq_family_results.values()))
                    else:
                        return uniq_family_results
        return None

    def assign_families(self, mnemonics):
        '''
        Returns {mnemonic: (log family, dimension, detection reliability), ...} for the mnemonic using Energystics rules.
        Assumes that logs are mostly produced by a one service company.
        '''
        all_company_mnemonic_info = {m: self.assign_family(m, one_best=False) for m in mnemonics}
        companies = []
        for company_info in all_company_mnemonic_info.values():
            if company_info is not None:
                companies += company_info.keys()
        company_count = {cc: companies.count(cc) for cc in set(companies)}
        for mnemonic, mis in all_company_mnemonic_info.items():
            if mis is not None:
                smis = sorted(mis.items(), key=lambda cc_info: (cc_info[1][2], company_count[cc_info[0]]),
                              reverse=True)  # sort starting from the highest rank, then by company share
                all_company_mnemonic_info[mnemonic] = smis[0][1]  # update with the best variant
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
