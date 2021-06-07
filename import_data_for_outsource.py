import os

from components.database.RedisStorage import build_log_meta_fields_index, build_dataset_meta_fields_index, build_well_meta_fields_index
from components.importexport import las

data_path = os.path.abspath("data_for_outsource")

for root, dirnames, filenames in os.walk(data_path):
    print("Start importing {len(filenames)} files.")
    l = len(filenames)
    for i, filename in enumerate(filenames):
        print(f"{i + 1}/{l}")
        filename = os.path.join(root, filename)
        las.import_to_db(filename)
    print("Import done.")

# build indices
build_log_meta_fields_index()
build_dataset_meta_fields_index()
build_well_meta_fields_index()
