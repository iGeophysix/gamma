# Log family properties database
# Manually run this script to update JSON file
# Import FAMILY_PROPERTIES from this script to get access to family properties

import os
import openpyxl as xl
import itertools
import json

SOURCE_FAMSET_BOOK = os.path.join(os.path.dirname(__file__), 'rules', 'src', 'FamilySettings.xlsx')
EXPORT_FAMSET_FILE = os.path.join(os.path.dirname(__file__), 'rules', 'FamilySettings.json')

DEFAULT_THICKNESS = 1
DEFAULT_COLOR = '000000'    # black in hex RGB24

# Family properties storage {'Family': {'splice': bool, 'mnemonic': str, 'min': float or None, 'max': float or None, 'unit': str or None, 'color': (R, G, B), 'thickness': int} }
FAMILY_PROPERTIES = {}


def capitalize(s):
    '''
    Make the first letter of all words capital.
    '''
    return ' '.join(map(str.capitalize, s.split(' ')))


def parse_excel_table(src_path, sheet) -> dict:
    '''
    Generates family properties dictionary from excel sheet
    '''
    left_top_row = 1    # table start
    left_top_col = 1    #

    wb = xl.load_workbook(src_path, read_only=True, data_only=True)
    ws = wb[sheet]
    # read header
    header = {}     # maping column name to column index
    for c in itertools.count(left_top_col):
        col_name = ws.cell(left_top_row, c).value
        if not col_name:    # table header ends with first empty cell
            break
        header[col_name] = c - 1
    # read table data
    data = {}
    for row_data in ws.iter_rows(min_row=left_top_row + 1, min_col=left_top_col, max_col=left_top_col + len(header) - 1):
        family = row_data[header['Family']].value
        if not family:
            break
        family = capitalize(family)
        splice = row_data[header['Splice']].value == 'TRUE'
        mnemonic = row_data[header['Mnemonic']].value or family
        min = row_data[header['Min']].value
        max = row_data[header['Max']].value
        unit = row_data[header['Unit']].value
        thickness = row_data[header['Thickness']].value or DEFAULT_THICKNESS
        # read cell backgroud color
        color = DEFAULT_COLOR
        cell_background = row_data[header['Color']].fill
        if cell_background is not None and cell_background.patternType == 'solid':
            bg_color = cell_background.start_color
            # theme color is not supported
            if bg_color.type == 'rgb':
                color = bg_color.rgb[-6:]   # cut off alpha channel
            elif bg_color.type == 'indexed':
                color = xl.styles.colors.COLOR_INDEX[bg_color.indexed][-6:]
        else:   # color may be typed as cell text
            s_color = row_data[header['Color']].value
            if s_color is not None:
                color = s_color[-6:]
        rgb_triplet = tuple(int(color[n: n + 2], 16) for n in range(0, 6, 2))   # converts 0A0B0C to (10, 11, 12)

        data[family] = {'splice': splice, 'mnemonic': mnemonic, 'min': min, 'max': max, 'unit': unit, 'color': rgb_triplet, 'thickness': thickness}

    return data


if __name__ == '__main__':
    # Export Excel sheet to JSON family properties file
    family_properties = parse_excel_table(SOURCE_FAMSET_BOOK, 'FamilesSettings')
    with open(EXPORT_FAMSET_FILE, 'w') as f:
        json.dump(family_properties, f, indent='\t')
else:
    # Initialize FAMILY_PROPERTIES from JSON file
    with open(EXPORT_FAMSET_FILE, 'r') as f:
        FAMILY_PROPERTIES = json.load(f)
