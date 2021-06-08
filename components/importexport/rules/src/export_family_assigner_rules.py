import os
import pickle
import re

import pandas

from components.database.RedisStorage import RedisStorage
from components.importexport.FamilyAssigner import FamilyAssigner

SOURCE_FAMASS_BOOK = os.path.join(os.path.dirname(__file__), 'PWLS v3.0 Logs.xlsx')


def capitalize(s):
    '''
    Make the first letter of all words capital.
    '''
    return ' '.join(map(str.capitalize, s.split(' ')))


def readTools() -> dict:
    '''
    Reads Excel table and returns { COMPANY_NAME: {MNEMONIC: TOOL} }
    '''
    # read Excel table
    cols = ['Company Name', 'Curve Mnemonic', 'Tool Code or Tool Model']
    with open(SOURCE_FAMASS_BOOK, 'rb') as f:
        df = pandas.read_excel(f, 'Curves Within Tools', header=0, usecols=cols, na_filter=False)

    # create { Company_Name: {Mnemonic: Tool} } dictionary
    cc_mnem_tool = {}
    for r in range(len(df.index)):
        company = df['Company Name'][r].strip()
        mnemonic = df['Curve Mnemonic'][r].strip().upper()
        tool = df['Tool Code or Tool Model'][r].strip()
        if mnemonic and tool:
            cc_mnem_tool.setdefault(company, {})[mnemonic] = tool
    return cc_mnem_tool


def build_family_assigner():
    # read family assignment rules from Excel
    cols = ['Company Name', 'Curve Mnemonic', 'Alt Mnemonics', 'Family', 'Dimension']
    with open(SOURCE_FAMASS_BOOK, 'rb') as f:
        df = pandas.read_excel(f, 'Curves', header=0, usecols=cols)

    # create family assignment database
    fa_rules = {}  # { company: [ ({mnemonics}, dimension, family), ...] }
    known_mnemonics = {}  # { company: {mnemonics} }; already saved mnemonics to avoid duplicate records
    for r in range(len(df.index)):
        company = df['Company Name'][r].strip()
        family = capitalize(df['Family'][r].strip())
        dimension = df['Dimension'][r].strip()

        # combine list of mnemonic variant from 'Curve Mnemonic' and 'Alt Mnemonics'
        mnemonics = set([str(df['Curve Mnemonic'][r]).strip().upper()])
        for alternative_mnemonic in map(str.strip, str(df['Alt Mnemonics'][r]).split(',')):
            if alternative_mnemonic:
                mnemonics.add(alternative_mnemonic.upper())
        mnemonics.difference_update(known_mnemonics.setdefault(company, set()))  # remove possible multiple entries of a mnemonic

        # add record to db
        if company and mnemonics and dimension and family:
            fa_rules.setdefault(company, []).append((sorted(mnemonics), dimension, family))
            known_mnemonics[company].update(mnemonics)

    # read logging tools from Excel
    tools = readTools()

    db = {}
    for company, rules in fa_rules.items():
        cdb = db[company] = FamilyAssigner.DataBase()  # initialize company FamilyAssigner db
        for mnemonics, dimension, family in rules:
            for mnemonic in mnemonics:
                tool = tools.get(company, {}).get(mnemonic)  # source logging tool for mnemonic
                mnemonic_info = FamilyAssigner.MnemonicInfo(family=family, dimension=dimension, company=company, tool=tool)
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
