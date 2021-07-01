#!/usr/bin/env python3
"""
Use this script to export a package for out sourcers
It will create a folder above current folder with name gamma_outsource_<current_date>.
e.g. if today is 28 of June 2021, then the folder will be gamma_outsource_06-28-2021

Put las files into folder:
./gamma_outsource_<current_date>/data_for_outsource/las

then archive the whole folder and send it to the developer.
"""

from datetime import datetime

import os
import shutil, errno

def copyfile(src_rel, dst_path):
    src = os.path.abspath(src_rel)
    dst = os.path.join(dst_path, src_rel)

    try:
        shutil.copytree(src, dst)
    except NotADirectoryError as exc:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy(src, dst)

date_time = datetime.now().strftime("%m-%d-%Y")
outsource_dir_name = f"gamma_outsource_{date_time}"
path_to_outsource_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', outsource_dir_name)


try:
    shutil.rmtree(path_to_outsource_dir)
except Exception as e:
    pass



files_to_copy = [
    "./components/__init__.py",
    "./components/database/__init__.py",
    "./settings.py",
    "./components/database/RedisStorage.py",
    "./components/domain/__init__.py",
    "./components/domain/Log.py",
    "./components/domain/Project.py",
    "./components/domain/Well.py",
    "./components/domain/WellDataset.py",
    "./components/domain/MarkersSet.py",
    "./components/importexport/__init__.py",
    "./components/importexport/las",
    "./components/importexport/UnitsSystem.py",
    "./components/importexport/rules/src/export_units_system.py",
    "./components/importexport/rules/src/Units.xlsx",
    "./utilities.py",
    "./data_for_outsource",
    "./import_data_for_outsource.py",
    ]


for f in files_to_copy:
    copyfile(f, path_to_outsource_dir)



# Write copyrights to all py files
for root, dirnames, filenames in os.walk(path_to_outsource_dir):
    for filename in filenames:
        if filename[-3:] == ".py":
            filename = os.path.join(root, filename)
            with open(filename, 'r') as original: data = original.read()
            with open(filename, 'w') as modified:
                c = "# Copyright (C) 2021. Anton Ovchinnikov, Alexey Lubinets, Alexander Filimonov, Dmitry Pinaev\n\n"
                modified.write(c + data)



docker_compose_content = \
"""
version: '3.4'
services:
  redis:
    image: redis
    container_name: gamma_redis
    ports:
      - "6379:6379"
  importer:
    build:
      context: .
      dockerfile: ./data_for_outsource/Dockerfile
    container_name: gamma_import_data
    depends_on:
      - redis
    command: [python, import_data_for_outsource.py]
"""

with open(os.path.join(path_to_outsource_dir, "docker-compose.yml"), 'w') as f: f.write(docker_compose_content)


readme_content = \
"""
1. Setup docker and docker-compose
2. Build and run auxiliary services
   ```bash
   docker-compose up --build -d
   ```
3. See how data is loading to the database
   ```bash
   docker logs -f gamma_import_data
   ```
   
4. If you see at the end
   ```
   Done building indices
   ```
   means data was imported successfully.
"""


with open(os.path.join(path_to_outsource_dir, "readme.md"), 'w') as f: f.write(readme_content)


