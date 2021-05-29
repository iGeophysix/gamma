import pandas
import json
import os

SOURCE_FAMASS_BOOK = os.path.join(os.path.dirname(__file__), 'PWLS v3.0 Logs.xlsx')
EXPORT_FAMASS_FILE = os.path.join(os.path.dirname(__file__), '..', 'FamilyAssignment.json')
EXPORT_TOOLS_FILE = os.path.join(os.path.dirname(__file__), '..', 'LoggingTools.json')


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


def main():
    # read family assignment rules from Excel
    cols = ['Company Name', 'Curve Mnemonic', 'Alt Mnemonics', 'Family', 'Dimension']
    with open(SOURCE_FAMASS_BOOK, 'rb') as f:
        df = pandas.read_excel(f, 'Curves', header=0, usecols=cols)

    # create family assignment database
    fa = {}     # { company: [ ({mnemonics}, dimension, family), ...] }
    known_mnemonics = {}    # { company: {mnemonics} }; already saved mnemonics to avoid duplicate records
    for r in range(len(df.index)):
        company = df['Company Name'][r].strip()
        family = capitalize(df['Family'][r].strip())
        dimension = df['Dimension'][r].strip()

        # combine list of mnemonic variant from 'Curve Mnemonic' and 'Alt Mnemonics'
        mnemonics = set([str(df['Curve Mnemonic'][r]).strip().upper()])
        for alternative_mnemonic in map(str.strip, str(df['Alt Mnemonics'][r]).split(',')):
            if alternative_mnemonic:
                mnemonics.add(alternative_mnemonic.upper())
        mnemonics.difference_update(known_mnemonics.setdefault(company, set()))    # remove possible multiple entries of a mnemonic

        # add record to db
        if company and mnemonics and dimension and family:
            fa.setdefault(company, []).append((sorted(mnemonics), dimension, family))
            known_mnemonics[company].update(mnemonics)

    # export family assignment JSON
    with open(EXPORT_FAMASS_FILE, 'w') as f:
        json.dump(fa, f, sort_keys=True, indent='\t')

    # read logging tools from Excel
    cc_mnen_tool = readTools()

    # export logging tools JSON
    with open(EXPORT_TOOLS_FILE, 'w') as f:
        json.dump(cc_mnen_tool, f, sort_keys=True, indent='\t')


if __name__ == '__main__':
    main()
