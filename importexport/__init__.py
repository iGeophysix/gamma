# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.

import os
import sys
import time
import logging

import multiprocessing as mp
from importexport import las

def importFiles(self):

    files = ["",""]

    start = time.time()

    pool = mp.Pool(mp.cpu_count())

    las_files = []

    # Parallel Process-based parsing
    for (i, las_file) in enumerate(pool.imap_unordered(las.parse_las_file, files)):
        progress.setValue(i)

        if las_file.error_message:
            geo_logger.error(las_file.error_message)
        else:
            las_files.append(las_file)

        if progress.wasCanceled():
            print('Canceled')
            return;

    pool.close()
    pool.join()

    # Serial Parsing
    # for (i, f) in enumerate(files):
        # progress.setValue(i)
        # las_files.append(las.parse_las_file(f))

    # progress.setValue(len(files))


    end = time.time()
