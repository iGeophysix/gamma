import io
import json
from datetime import datetime

import psycopg2
from psycopg2 import sql

from settings import STORAGE_DB, STORAGE_USER, STORAGE_HOST, STORAGE_PORT, STORAGE_PASSWORD, MISSING_VALUE
from utilities import my_timer


class ColumnStorage:

    def __init__(self, user=None, password=None, host=None, port=None, db=None):
        self._user = STORAGE_USER if user is None else user
        self._password = STORAGE_PASSWORD if password is None else password
        self._host = STORAGE_HOST if host is None else host
        self._port = STORAGE_PORT if port is None else port
        self._db = STORAGE_DB if db is None else db
        self._engine = psycopg2.connect(f"postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db}")
        self._conn = self._engine.cursor()

    def init_db(self):
        sql = '''
            create table if not exists wells (
            	wellname varchar(256) not null
            		constraint wells_pk
            			primary key,
            	info jsonb
            );
            
            CREATE TABLE public.datasets (
                wellname varchar NULL,
                datasetname varchar NULL,
                info jsonb NULL
            );
            CREATE INDEX datasets_wellname_idx ON public.datasets (wellname);
            CREATE INDEX datasets_datasetname_idx ON public.datasets (datasetname);
            CREATE UNIQUE INDEX datasets_wellname_together_idx ON public.datasets (wellname,datasetname);

            '''
        self._conn.execute(sql)
        self._engine.commit()

    def flush_db(self):
        self._conn.execute(sql.SQL("DROP SCHEMA public CASCADE;"))
        self._conn.execute(sql.SQL(f"CREATE SCHEMA public;"))
        self.commit()

    def commit(self):
        self._engine.commit()

    # WELLS

    def list_wells(self):
        self._conn.execute('select wellname from wells')
        return [well for well in self._conn]

    def create_well(self, name):
        self._conn.execute(f"INSERT INTO wells VALUES ('{name}','{{}}')")
        self._engine.commit()

    def update_well_info(self, name, info):
        _info = json.dumps(info)
        sql = f"""INSERT INTO wells (wellname, info) VALUES ('{name}', '{_info}') 
            ON CONFLICT (wellname) DO UPDATE 
            SET info = '{_info}' WHERE wells.wellname = '{name}'"""
        self._conn.execute(sql)
        self._engine.commit()

    def get_well_info(self, name):
        self._conn.execute(f"SELECT info FROM wells where wells.wellname = '{name}'")
        return self._conn.fetchone()[0]

    def delete_well(self, well):
        self._conn.execute(f"DELETE FROM wells where wellname = '{well}'")

    # DATASETS

    @staticmethod
    def __generate_dataset_name(well, name):
        return f"{well}__{name}"

    def create_dataset(self, well, name):
        query = f'''create table IF NOT EXISTS "{self.__generate_dataset_name(well, name)}"
    (
    	reference double precision not null
    		constraint "{well}__{name}_pk"
    			primary key
    );
    create unique index IF NOT EXISTS "{self.__generate_dataset_name(well, name)}_reference_uindex"
    	on "{self.__generate_dataset_name(well, name)}" (reference);'''
        self._conn.execute(query)
        self._engine.commit()
        return f"{well}__{name}"

    def delete_dataset(self, well, name):
        query = f'''DROP TABLE "{self.__generate_dataset_name(well, name)}";'''
        self._conn.execute(query)
        self._engine.commit()

    @my_timer
    def bulk_load_dataset(self, well_name, dataset_name, logs: dict, values: dict, size: int = 8192, trunc=True) -> None:
        with self._engine.cursor() as cursor:
            if trunc:
                try:
                    self.delete_dataset(well_name, dataset_name)
                finally:
                    self.create_dataset(well_name, dataset_name)

            for log, dtype in logs.items():
                self.add_log(well_name, dataset_name, log, self.get_data_type(dtype), autocommit=False)
            self.commit()

            data_to_insert = io.StringIO("\n".join(
                f"{key}|{'|'.join(map(str, data.values()))}" for key, data in values.items())
            )

            cursor.copy_from(data_to_insert, f'"{self.__generate_dataset_name(well_name, dataset_name)}"', sep='|', size=size, null=str(MISSING_VALUE))

        self.commit()

    @my_timer
    def read_dataset(self, well_name, dataset_name, logs=None, depth=None, depth__gt=None, depth__lt=None):
        if logs:
            fields = 'reference,' + ','.join([f'"{log}"' for log in logs])
        else:
            fields = '*'
        query = f'select {fields} from "{self.__generate_dataset_name(well_name, dataset_name)}" where 1=1'
        if depth:
            query += f' and reference={depth}'
        elif depth__lt is not None or depth__gt is not None:
            if depth__gt is not None:
                query += f' and reference > {depth__gt}'
            if depth__lt is not None:
                query += f' and reference < {depth__lt}'

        self._conn.execute(query)
        colnames = [desc[0] for desc in self._conn.description]
        result = {row[0]: {key: val for key, val in zip(colnames[1:], row[1:])} for row in self._conn}

        return result

    @my_timer
    def update_dataset(self, well_name, dataset_name, data, autocommit=True):
        logs = ','.join([f'"{l}"' for l in next(iter(data.values())).keys()])
        values = "),(".join(f"""{ref},'{"','".join(str(v) for k, v in vals.items())}'""" for ref, vals in data.items())
        excluded_logs = ','.join([f'EXCLUDED."{l}"' for l in next(iter(data.values())).keys()])
        query = f"""
        insert into "{self.__generate_dataset_name(well_name, dataset_name)}" (reference, {logs}) 
        values 
        ({values})
        on conflict (reference) do update set ({logs}) = ({excluded_logs});
        """
        self._conn.execute(query)
        if autocommit:
            self._engine.commit()

    # LOGS

    def add_log(self, well_name, dataset_name, log_name, log_type, autocommit=True):
        query = sql.SQL(f"""ALTER TABLE "{self.__generate_dataset_name(well_name, dataset_name)}" ADD COLUMN "{log_name}" {log_type} NULL;""")
        self._conn.execute(query)
        if autocommit:
            self._engine.commit()
        return log_name

    def delete_log(self, well_name, dataset_name, log_name, autocommit=True):
        query = sql.SQL(f"""ALTER TABLE "{self.__generate_dataset_name(well_name, dataset_name)} DROP COLUMN {log_name}" ;""")
        self._conn.execute(query)
        if autocommit: self._engine.commit()

    @staticmethod
    def get_data_type(data):
        LOG_TYPES = {
            float: "double precision",
            int: "integer",
            str: "varchar",
            bool: "boolean",
            datetime: "timestamp",
        }
        return LOG_TYPES.get(data, 'varchar')
