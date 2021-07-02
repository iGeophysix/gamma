import os

from components.database.RedisStorage import build_log_meta_fields_index, build_dataset_meta_fields_index, build_well_meta_fields_index
from components.importexport import las
from components.importexport.rules.src.export_units_system import build_unit_system

data_path = os.path.join('data_for_outsource', 'las')
print("Building unit system")
build_unit_system()

print("Loading data")
for root, dirnames, filenames in os.walk(data_path):
    print(f"Start importing {len(filenames)} files.")
    l = len(filenames)
    for i, filename in enumerate(filenames):
        print(f"{i + 1}/{l}")
        if filename.endswith('.las'):
            filename = os.path.join(root, filename)
            las.import_to_db(filename)
    print("Import done.")

# build indices
build_log_meta_fields_index()
build_dataset_meta_fields_index()
build_well_meta_fields_index()
print("Done building indices")
