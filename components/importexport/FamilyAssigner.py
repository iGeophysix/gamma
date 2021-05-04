import json
import operator
import os
import re

import logging

from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode

FAMASS_RULES = os.path.join(os.path.dirname(__file__), 'rules', 'FamilyAssignment.json')
GARBAGE_TOKENS = {'DL', 'RT', 'SL', 'SIG', 'RAW', 'BP', 'ALT', 'ECO', 'DH', 'NORM'}


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
        return {''.join(sorted(s[n: n + 2])) for n in range(len(s) - 1)}

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
        # define search N-gram set
        ng = set()
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng.update(self.ngram(token))
        # search
        best_rank = 0
        best_families = []
        for ng_ref, family in self.db:
            match_rank = len(ng.intersection(ng_ref)) / max(len(ng), len(ng_ref))  # rank is (number of matched N-grams) / (all N-grams)
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

    def _nlp_assign_family(self, mnemonic, company=None):
        '''
        Get family using NLP.
        '''
        company_result = {}
        for cc in [company] if company is not None else self.db:
            db = self.db[cc]
            family_infos, rank = db.nlpp.family_variants(mnemonic)
            company_result[cc] = [(*fi, rank) for fi in family_infos]
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
        mnemonic = mnemonic.upper()
        company_result = {}
        for cc in [company] if company is not None else self.db:
            db = self.db[cc]
            res = None

            # exact matching
            mnemonic_info = db.precise_match.get(mnemonic)
            if mnemonic_info is not None:
                res = *mnemonic_info, 0.9

            # pattern matching
            if res is None:
                for re_pattern, mnemonic_info in db.re_match.items():
                    if re_pattern.fullmatch(mnemonic):
                        res = *mnemonic_info, 0.8

            # NLP
            if res is None:
                nlp_res = self._nlp_assign_family(mnemonic, cc)
                if nlp_res is not None:
                    # res = [(mnemonic_info[0], mnemonic_info[1], mnemonic_info[2] * 0.7) for mnemonic_info in nlp_res]   # decrease maximum NLP confidence  # TODO: use all the variants
                    mnemonic_info = nlp_res[0]
                    res = (mnemonic_info[0], mnemonic_info[1], mnemonic_info[2] * 0.7)  # decrease maximum NLP confidence

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

                if one_best:
                    return sorted(uniq_family_results.values(), key=operator.itemgetter(2), reverse=True)[0]  # sorted by rank
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
    logger = logging.getLogger("ShaleVolumeLinearMethodNode")
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
        :param log: BasicLog, log to process
        :return: BasicLog, log with assigned family
        """
        p = Project()
        fa = FamilyAssigner()
        for well_name in p.list_wells():
            well = Well(well_name)
            for dataset_name in well.datasets:
                dataset = WellDataset(well, dataset_name)
                for log_id in dataset.log_list:
                    log = BasicLog(dataset.id, log_id)
                    try:
                        cls.validate_input(log)
                    except Exception as exc:
                        cls.logger.error(f"Validation error in FamilyAssignerNode on {well_name}-{dataset_name}-{log.name}. {repr(exc)}")
                        continue


                    mnemonic = log.name
                    log.meta.family = fa.assign_family(mnemonic, one_best=True)[0]
                    log.save()


