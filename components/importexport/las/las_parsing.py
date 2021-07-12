# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.

import collections
import io
import math
import os
import re

from chardet.universaldetector import UniversalDetector

LasRequiredEntry = \
    collections.namedtuple('LasRequiredEntry', \
                           'value description')

AdditionalWellEntry = \
    collections.namedtuple('AdditionalWellEntry', \
                           'value units description')

LogMetricsEntry = \
    collections.namedtuple('LogMetricsEntry', \
                           'value units')

LoggingParameterEntry = \
    collections.namedtuple('LoggingParameterEntry', \
                           'value units description')

LogInformationEntry = \
    collections.namedtuple('LogInformationEntry', \
                           'units code description')


class LasStructure():
    """
    The class instance that is returned by the function
    'parse_las_file'. The whole LAS structure is represented
    by several disctionaries that typically have mnemonic names
    as keys and values as dict values.

    The class is able to hold the data for LAS 1.0-2.0.

    required_well_entries   : {name, [LasRequiredEntry]}
    additional_well_entries : {name, [AdditionalWellEntry]}
    log_metrics_entries     : {name, [LogMetricsEntry]}
    log_information_entries : {name, [LogInformationEntry]}
    logging_parameters      : {name, [LoggingParameterEntry]}
    data                    : {name, [float]}
    error_message           : str or empty
    filename                : str
    """

    def __init__(self, filename):
        self.filename = filename

        self.error_message = ""

        # Parts that could be omitted in file
        self.logging_parameters = {}
        self.additional_well_entries = {}

    def well_info(self):
        return {k: v._asdict() for k, v in (self.required_well_entries | self.additional_well_entries).items()}

    def logs_info(self):
        # md_key = list(self.data.keys())[0]
        return {k: v._asdict() for k, v in self.log_information_entries.items()}

    def valid(self):
        """
        States that the file has been parsed correctly
        """
        return hasattr(self, "required_well_entries") and \
               hasattr(self, "log_information_entries") and \
               hasattr(self, "data")


def parse(filename, data: bytearray = None, use_chardet=True) -> LasStructure:
    """
    Function uses the `chardet` module in order to
    guess the file codepage. This could be essential
    for "russian" files that are randomly stored in
    CP-1251, CP-866, and sometimes in UTF-8
    """

    try:

        enc = "latin-1"
        if use_chardet:
            detector = UniversalDetector()
            ff = io.BytesIO(data) if data else open(filename, 'rb')

            for l in ff:
                detector.feed(l)
                if detector.done:
                    break
            detector.close()
            if data is None:
                ff.close()

            enc = detector.result['encoding'] if detector.result['encoding'] else enc

        f = io.StringIO(data.decode(enc)) if data else open(filename, 'r', encoding=enc)

        lines = f.readlines()
        lines = [l.strip() for l in lines]
        lines = [l for l in lines if not l.startswith('#')]

        f.close()

        ####
        las_file = LasStructure(os.path.basename(filename))

        try:
            i = 0
            while (i < len(lines)):
                line = lines[i]

                if line.startswith('~V'):
                    i = _parse_version_section(i, lines, las_file)
                elif line.startswith('~W'):
                    i = _parse_well_information_section(i, lines, las_file)
                elif line.startswith('~P'):
                    i = _parse_logging_parameter_section(i, lines, las_file)
                # elif line.startswith('~O'):
                # i = _parse_other_information_section(i, lines, las_file)
                elif line.startswith('~C'):
                    i = _parse_curve_information_section(i, lines, las_file)
                elif line.startswith('~A'):
                    i = _parse_ascii_log_data_section(i, lines, las_file)

                i += 1
        except Exception as e:
            las_file.error_message = str(e)

    except FileNotFoundError:
        print('File does not exist')

    finally:
        f.close()

    return las_file


def _parse_version_section(i, lines, las_file):
    i += 1
    r = re.search(r'(1\.2|2\.0)', lines[i]);

    if r:
        version = r.group(1)
        if version == '1.2':
            las_file.version = 12
        else:
            las_file.version = 20
    else:
        raise Exception('No version line')

    i += 1
    r = re.search(r'(YES|NO)', lines[i]);

    if r:
        las_file.wrap = (r.group(1) == 'YES')
    else:
        raise Exception('No wrap line')

    return i


def _parse_well_information_section(i, lines, las_file):
    i += 1

    base_field = r'({{name}} *)(\.\S*)(\s*{0})(:.*$)'
    numeric_word = r'[+-]?(\d+([.]\d*)?|[.]\d+) *'
    numeric_field = base_field.format(numeric_word)

    # STRT.M        583.0:
    log_metrics_res = {
        'STRT': re.compile(numeric_field.format(name='STRT')),
        'STOP': re.compile(numeric_field.format(name='STOP')),
        'STEP': re.compile(numeric_field.format(name='STEP')),
        'NULL': re.compile(numeric_field.format(name='NULL'))
    }

    #  WELL.                WELL:   4832/116
    non_numeric_word = r'.*'
    specific_field = base_field.format(non_numeric_word)

    required_res = {
        'WELL': re.compile(specific_field.format(name='WELL')),
        'COMP': re.compile(specific_field.format(name='COMP')),
        'SRVC': re.compile(specific_field.format(name='SRVC')),
        'FLD': re.compile(specific_field.format(name='FLD')),
        'LOC': re.compile(specific_field.format(name='LOC')),
        'DATE': re.compile(specific_field.format(name='DATE')),
        'CTRY': re.compile(specific_field.format(name='CTRY')),
        'STAT': re.compile(specific_field.format(name='STAT')),
        'CNTY': re.compile(specific_field.format(name='CNTY')),
        'PROV': re.compile(specific_field.format(name='PROV')),
        'API': re.compile(specific_field.format(name='API')),
        'UWI': re.compile(specific_field.format(name='UWI'))
    }

    #  UWI .      UNIQUE WELL ID:326R000K116_F0W4832_
    #  name .units   name:value
    re_additional_entries = re.compile(specific_field.format(name='\S+'))

    line_is_processed = False

    log_metrics_entries = {}
    required_well_entries = {key: LasRequiredEntry('', '') for key, _ in required_res.items()}
    additional_well_entries = {}

    while i < len(lines):
        line = lines[i]
        line_is_processed = False

        if line.startswith("~"):
            i -= 1
            break

        # First, try to recognize log metrics fields
        for key, value in log_metrics_res.items():
            r = value.search(line)

            if r:
                log_metrics_entries[r.group(1).strip()] = \
                    LogMetricsEntry(value=r.group(3).strip(), units=r.group(2)[1:])

                line_is_processed = True
                break

        # Try to recognize required fields
        if not line_is_processed:
            for key, value in required_res.items():
                r = value.search(line)

                if r:
                    fieldname = r.group(1).strip()
                    value = r.group(3).strip()
                    description = r.group(4)[1:].strip()

                    if las_file.version == 12:
                        value, description = description, value

                    required_well_entries[fieldname] = \
                        LasRequiredEntry(value, description)

                    line_is_processed = True
                    break

        # Try to recognize additional fields
        if not line_is_processed:
            r = re_additional_entries.search(line)

            if r:
                fieldname = r.group(1).strip()
                units = r.group(2)[1:].strip()
                value = r.group(3).strip()
                description = r.group(4)[1:].strip()

                if las_file.version == 12:
                    value, description = description, value

                additional_well_entries[fieldname] = \
                    AdditionalWellEntry(value, units, description)

                line_is_processed = True

        i += 1

    las_file.log_metrics_entries = log_metrics_entries
    las_file.required_well_entries = required_well_entries
    las_file.additional_well_entries = additional_well_entries

    if not "WELL" in las_file.required_well_entries:
        raise Exception('No WELL field in the file')

    return i


def _parse_logging_parameter_section(i, lines, las_file):
    i += 1

    parameter_field = r'(^[^ ]+ *)(\.[^ ]*)( .*)(:.*$)'

    re_parameter_field = re.compile(parameter_field)

    logging_parameters = {}

    while i < len(lines):
        line = lines[i]

        if line.startswith("~"):
            i -= 1
            break

        r = re_parameter_field.search(line)

        if r:
            fieldname = r.group(1).strip()
            units = r.group(2)[1:].strip()
            value = r.group(3).strip()
            description = r.group(4)[1:].strip()

            logging_parameters[fieldname] = \
                LoggingParameterEntry(value, units, description)

        i += 1

    las_file.logging_parameters = logging_parameters

    return i


def _parse_other_information_section(i, lines, las_file):
    return i


def _parse_curve_information_section(i, lines, las_file):
    i += 1

    #  UWI .      UNIQUE WELL ID:326R000K116_F0W4832_
    #                     name .units   :value
    re_log_info_entry = re.compile('(^[^ ]+ *)(\.[^ ]*)( .*)(:.*$)');

    log_information_entries = {}

    while i < len(lines):
        line = lines[i]

        if line.startswith("~"):
            i -= 1
            break

        r = re_log_info_entry.search(line)

        if r:
            mnem = r.group(1).strip()
            units = r.group(2)[1:].strip()
            code = r.group(3).strip()
            description = r.group(4)[1:].strip()

            if mnem in log_information_entries:
                raise Exception('Duplicating curve mnem "{}"'.format(mnem))

            log_information_entries[mnem] = \
                LogInformationEntry(units, code, description)
        else:
            raise Exception("Line couldn't be parsed '{}'".format(line))

        i += 1

    las_file.log_information_entries = log_information_entries

    return i


def _parse_ascii_log_data_section(i, lines, las_file):
    i += 1

    re_num_value = re.compile(r'[+-]?(\d+([.]\d*)?|[.]\d+)')

    mnemonics = list(las_file.log_information_entries)
    number_of_mnemonics = len(mnemonics)

    current_mnem_number = 0

    null_value = las_file.log_metrics_entries['NULL'].value
    f_null_value = float(null_value)

    data = dict((mnem, []) for mnem in mnemonics)

    while i < len(lines):
        line = lines[i]

        if line.startswith("~"):
            i -= 1
            break

        for m in re_num_value.finditer(line):
            value = m.group(0)
            key = mnemonics[current_mnem_number]

            f_value = float(value)

            if (null_value == value or math.isclose(f_value, f_null_value)):
                data[key].append(float('nan'))
            else:
                data[key].append(f_value)

            current_mnem_number += 1
            current_mnem_number %= number_of_mnemonics

        i += 1

    las_file.data = data

    return i
