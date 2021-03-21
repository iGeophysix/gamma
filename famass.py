import os
import re
import unittest
# import importexport.las as las
# import project
import operator

ENERGYSICS_CHANNEL_CLASS_PATH = os.path.join(os.path.dirname(__file__), 'PWLS v3.0 Logs.txt')
GARBAGE_TOKENS = {'DL', 'RT', 'SL', 'SIG', 'RAW', 'BP', 'ALT', 'ECO', 'DH', 'NORM'}


class NLPParser:
    '''
    Family recongition uning Natural Language Processing.
    '''

    def __init__(self):
        self.db = []    # ({N-grams}, family), ...

    def mnemonic_tokenize(self, s):
        '''
        Split mnemonic to clean up garbage.
        '''
        return [p for p in s.split('_') if p and p not in GARBAGE_TOKENS]

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

    def family(self, mnemonic):
        '''
        Detect family by mnemonic.
        '''
        # token_rank = []
        ng = set()
        for token in self.mnemonic_tokenize(mnemonic.upper()):
            ng.update(self.ngram(token))
        ranks = [(len(ng.intersection(ng_ref)) / max(len(ng), len(ng_ref)), family) for ng_ref, family in self.db]
        best_rank = 0
        best_family = None
        for r, f in ranks:
            if r > best_rank:
                best_rank = r
                best_family = f
        # if best_rank > 0:
        #     token_rank.append((best_rank, best_family))
        # if token_rank:
        #     token_rank.sort(key=operator.itemgetter(0), reverse=True)   # assign family by the most recognized token (part_of_the_mnemonic)
        #     return *token_rank[0][1], token_rank[0][0]     # family, rank
        # else:
        #     return None
        return best_family, best_rank


class FamilyAssigner:
    '''
    Main family assigner class.
    '''
    class DataBase:
        '''
        Storage for company-specific rules.
        '''
        def __init__(self):
            self.precise_match = {}  # { mnemonic: (family, units_class), ... }
            self.re_match = {}       # { re.Pattern: (family, units_class), ... }
            self.nlpp = NLPParser()

    def __init__(self):
        '''
        Loads Energystics family assigning rules.
        '''
        self.db = {}    # {company_code: FamilyAssigner.DataBase}

        with open(ENERGYSICS_CHANNEL_CLASS_PATH, 'r') as f:
            assert f.readline().rstrip('\n').split('\t') == ['Company Code', 'Curve Mnemonic', 'PWLS v3 Property', 'Curve Unit Quantity Class', 'LIS Curve Mnemonic', 'Curve Description'], 'Unexpected table header'
            for row in f.readlines():
                entry = row.rstrip('\n').split('\t')
                assert len(entry) == 6, 'Incorrect data line "{}"'.format(entry)
                mnemonic = entry[1].lower()
                mnemonic_alt = entry[4].lower()
                for mnemonic in set((mnemonic, mnemonic_alt)):
                    if mnemonic:
                        company_code = int(entry[0])
                        cdb = self.db.get(company_code)
                        if cdb is None:
                            cdb = self.db[company_code] = FamilyAssigner.DataBase()
                        family = self._capitalize(entry[2])
                        unit_class = entry[3]
                        mnemonic_info = (family, unit_class)
                        if '*' in mnemonic or '?' in mnemonic:
                            re_mask = mnemonic.replace('*', '.*').replace('?', '.')
                            re_pattern = re.compile(re_mask, re.IGNORECASE)
                            cdb.re_match[re_pattern] = mnemonic_info
                        else:
                            cdb.precise_match[mnemonic] = mnemonic_info
                        cdb.nlpp.learn(mnemonic, mnemonic_info)

    def _capitalize(self, s):
        '''
        Makes the first letter of all separate words capital.
        '''
        return ' '.join(map(str.capitalize, s.split(' ')))

    def _nlp_assign_family(self, mnemonic, company_code=None):
        '''
        Get family using NLP.
        '''
        company_result = {}
        for cc in [company_code] if company_code is not None else self.db:
            db = self.db[cc]
            family_info, rank = db.nlpp.family(mnemonic)
            if family_info is not None:
                company_result[cc] = (*family_info, rank)
        if company_result:
            if company_code is not None:
                return company_result[company_code]
            else:
                return company_result
        return None

    def assign_family(self, mnemonic, one_best=True, company_code=None):
        '''
        Returns a (log family, unit class, detection reliability) for the mnemonic using Energystics rules.
        Gives all the variants if one_best=False.
        company_code limits log dictionary to a particuler service company.
        '''
        mnemonic = mnemonic.lower()
        company_result = {}
        for cc in [company_code] if company_code is not None else self.db:
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
                res = self._nlp_assign_family(mnemonic, cc)
                if res is not None:
                    res = (res[0], res[1], res[2] * 0.7)  # decrease maximum NLP confidence

            if res is not None:
                company_result[cc] = res

        if company_result:
            if company_code is not None:
                return company_result.get(company_code)
            else:
                # remove family duplicates
                uniq_family_results = {}
                for company_code, mnemonic_info in company_result.items():
                    found_same_family_at_cc = None
                    for unic_company_code, unic_mnemonic_info in uniq_family_results.items():
                        if mnemonic_info[0] == unic_mnemonic_info[0]:    # compare by family
                            found_same_family_at_cc = unic_company_code
                            break
                    if found_same_family_at_cc is not None:
                        unic_mnemonic_info = uniq_family_results[found_same_family_at_cc]
                        newRank = min(1, unic_mnemonic_info[2] + mnemonic_info[2] / len(company_result))    # increase rank by a part of the second source rank
                        uniq_family_results[found_same_family_at_cc] = (unic_mnemonic_info[0], unic_mnemonic_info[1], newRank)  # update rank, drop second source variant
                    else:
                        uniq_family_results[company_code] = mnemonic_info   # just add the variant

                if one_best:
                    return sorted(uniq_family_results.values(), key=operator.itemgetter(2), reverse=True)[0]     # sorted by rank
                else:
                    return uniq_family_results
        return None

    def assign_families(self, mnemonics):
        '''
        Returns {mnemonic: (log family, unit class, detection reliability), ...} for the mnemonic using Energystics rules.
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
                smis = sorted(mis.items(), key=lambda cc_info: (cc_info[1][2], company_count[cc_info[0]]), reverse=True)    # sort starting from the highest rank, then by company share
                all_company_mnemonic_info[mnemonic] = smis[0][1]   # update with the best variant
        return all_company_mnemonic_info


class TestFamilyAssignment(unittest.TestCase):
    def setUp(self):
        self.fa = FamilyAssigner()

    def test_main(self):
        mnem_list = ['GR_NORM', 'PS', 'RB', 'BS', 'RAW_TNPH', 'FCAZ', 'HAZI', 'CALI_2']
        result1 = ['Gamma Ray', 'Spontaneous Potential', 'Relative Bearing', 'Outside Diameter', 'Thermal Neutron Porosity', 'Z Acceleration', 'Hole Azimuth', 'Borehole Diameter']
        result2 = ['Gamma Ray', 'Spontaneous Potential', 'Relative Bearing', 'Nom Borehole Diameter', 'Thermal Neutron Porosity', 'Z Acceleration', 'Hole Azimuth', 'Borehole Diameter']
        for n in range(len(mnem_list)):
            self.assertEqual(self.fa.assign_family(mnem_list[n], one_best=True)[0], result1[n])
        self.assertEqual([r[0] for r in self.fa.assign_families(mnem_list).values()], result2)


if __name__ == '__main__':
    # fa = FamilyAssigner()
    # mnem_list = ['GR_NORM', 'PS', 'RB', 'BS', 'RAW_TNPH', 'FCAZ', 'HAZI', 'CALI_2']
    # for m in mnem_list:
    #     print(m, '->', fa.assign_family(m, one_best=True), 'All:', fa.assign_family(m, one_best=False))
    # print('Batch assigning', fa.assign_families(mnem_list))

    unittest.main(TestFamilyAssignment())
