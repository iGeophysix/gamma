#!/usr/bin/env python3

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
    "./components/database/settings.py",
    "./components/database/RedisStorage.py",
    "./components/domain/__init__.py",
    "./components/domain/Log.py",
    "./components/domain/Project.py",
    "./components/domain/Well.py",
    "./components/domain/WellDataset.py",
    "./components/importexport/__init__.py",
    "./components/importexport/las",
    "./components/importexport/UnitsSystem.py",
    "./requirements.txt",
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
"""

with open(os.path.join(path_to_outsource_dir, "docker-compose.yml"), 'w') as f: f.write(docker_compose_content)


readme_content = \
"""
1. Setup docker and docker-compose
2. Build and run auxiliary services
   ```bash
   docker-compose up --build
   ```
3. Credentials to be configured via env vars (see settings.py)
4. create venv
    ```bash
    python3 -m virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
5. Run data import
    ```bash
    python import_data_for_outsource.py
    ```

6. You are now ready to access the data in a Redis db with credentials in `components/database/settings`
"""


with open(os.path.join(path_to_outsource_dir, "readme.md"), 'w') as f: f.write(readme_content)


