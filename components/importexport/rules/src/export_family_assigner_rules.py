import os
import pickle
import re
from collections import defaultdict
import pandas

from components.database.RedisStorage import RedisStorage
from components.importexport.FamilyAssigner import FamilyAssigner
from components.petrophysics.data.src.best_log_tags_assessment import read_best_log_tags_assessment

SOURCE_FAMASS_BOOK = os.path.join(os.path.dirname(__file__), 'LogsAttributes.xlsx')


def capitalize(s):
    '''
    Make the first letter of all words capital.
    '''
    return ' '.join(map(str.capitalize, s.split(' ')))


def build_family_assigner():
    property_mapping = [
        # Excel column, Meta property, Data type
        ['Tags', 'tags', set],
        ['Logging Service', 'logging_service', str],
        ['DOI, in', 'DOI', float],
        ['Vertical Resolution, ft', 'vertical_resolution', float],
        ['Frequency, MHz', 'frequency', float],
        ['Tool', 'tool', str]
    ]
    # read family assignment rules from Excel
    cols = ['Company Name', 'Curve Mnemonic', 'Alt Mnemonics', 'Family', 'Dimension', 'Curve Description'] + [prop[0] for prop in property_mapping]
    with open(SOURCE_FAMASS_BOOK, 'rb') as f:
        df = pandas.read_excel(f, 'Curves', header=0, usecols=cols)

    # create search REs for tags
    description_tags_patterns = {}
    space = r'[^\w-]+'  # non-tag characters
    LOG_ASSESSMENT = read_best_log_tags_assessment()
    for tag in set(LOG_ASSESSMENT['General log tags'].keys()) | set(LOG_ASSESSMENT['Formation Resistivity']['description tags'].keys()):
        stag = tag.split('_')  # for multi-word tags
        restag = f'(^|{space}){space.join(stag)}({space}|$)'  # surround tag words with spaces
        description_tags_patterns[tag] = re.compile(restag, re.IGNORECASE)

    # create family assignment database
    fa_rules = defaultdict(list)  # { company: [ ({mnemonics}, dimension, family), ...] }
    known_mnemonics = defaultdict(set)  # { company: {mnemonics} }; already saved mnemonics to avoid duplicate records
    for r in range(len(df.index)):
        company = df['Company Name'][r].strip()
        family = capitalize(df['Family'][r].strip())
        dimension = df['Dimension'][r].strip()

        # combine list of mnemonic variant from 'Curve Mnemonic' and 'Alt Mnemonics'
        mnemonics = set([str(df['Curve Mnemonic'][r]).strip().upper()])
        for alternative_mnemonic in map(str.strip, str(df['Alt Mnemonics'][r]).split(',')):
            if alternative_mnemonic:
                mnemonics.add(alternative_mnemonic.upper())
        mnemonics.difference_update(known_mnemonics[company])  # remove possible multiple entries of a mnemonic

        # read optional properties
        optional_properties = {}
        for excel_column, meta_property, ty in property_mapping:
            raw_value = df[excel_column][r]
            if not pandas.isna(raw_value):
                if ty == set:
                    items = set()
                    for item in map(str.strip, raw_value.split(',')):
                        if item:
                            items.add(item)
                    res_value = items or None
                elif ty == str:
                    value = str(raw_value).strip()
                    res_value = value or None
                elif ty == float:
                    res_value = float(raw_value)
                else:
                    raise NotImplementedError
                if res_value is not None:
                    optional_properties[meta_property] = res_value

        # read tags from description
        description = df['Curve Description'][r]
        if pandas.notnull(description):
            description_tags = {tag for tag, pattern in description_tags_patterns.items() if pattern.search(description)}
            # add description tags to custom tags
            if description_tags:
                optional_properties.setdefault('tags', set()).update(description_tags)

        # add record to db
        if company and mnemonics and dimension and family:
            fa_rules[company].append((sorted(mnemonics), dimension, family, optional_properties))
            known_mnemonics[company].update(mnemonics)

    # buld family assignment rules database
    db = {}
    for company, rules in fa_rules.items():
        cdb = db[company] = FamilyAssigner.DataBase()  # initialize company FamilyAssigner db
        for mnemonics, dimension, family, optional_properties in rules:
            mnemonic_info = FamilyAssigner.MnemonicInfo(family=family, dimension=dimension, company=company, optional_properties=optional_properties)
            for mnemonic in mnemonics:
                if '*' in mnemonic or '?' in mnemonic:
                    re_mask = mnemonic.replace('*', '.*').replace('?', '.')
                    re_pattern = re.compile(re_mask, re.IGNORECASE)
                    cdb.re_match[re_pattern] = mnemonic_info
                else:
                    cdb.precise_match[mnemonic] = mnemonic_info
                cdb.nlpp.learn(mnemonic, mnemonic_info)

    s = RedisStorage()
    s.object_set(FamilyAssigner.TABLE_NAME, pickle.dumps(db))


if __name__ == '__main__':
    build_family_assigner()
