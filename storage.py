import io
import json

import psycopg2
from psycopg2 import sql

from settings import *
from utilities import *


class Storage:
    def __init__(self, user=None, password=None, host=None, port=None, db=None):
        self._user = STORAGE_USER if user is None else user
        self._password = STORAGE_PASSWORD if password is None else password
        self._host = STORAGE_HOST if host is None else host
        self._port = STORAGE_PORT if port is None else port
        self._db = STORAGE_DB if db is None else db
        self._engine = psycopg2.connect(f"postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db}")
        self._conn = self._engine.cursor()

    def init_db(self):
        sql = '''create table if not exists wells
        (
        	wellname varchar(256) not null
        		constraint wells_pk
        			primary key,
        	info jsonb
        );'''
        self._conn.execute(sql)
        self._engine.commit()

    def flush_db(self):
        self._conn.execute(sql.SQL("DROP SCHEMA public CASCADE;"))
        self._conn.execute(sql.SQL(f"CREATE SCHEMA public;"))
        self.commit()

    def list_wells(self):
        self._conn.execute('select wellname from wells')
        return [well for well in self._conn]

    def create_well(self, name):
        self._conn.execute(f"INSERT INTO wells VALUES ('{name}','{{}}')")
        self._engine.commit()

    def update_well(self, name, info):
        _info = json.dumps(info)
        sql = f"""INSERT INTO wells (wellname, info) VALUES ('{name}', '{_info}') 
        ON CONFLICT (wellname) DO UPDATE 
        SET info = '{_info}' WHERE wells.wellname = '{name}'"""
        self._conn.execute(sql)
        self._engine.commit()

    def get_well_info(self, name):
        self._conn.execute(f"SELECT info FROM wells where wells.wellname = '{name}'")
        return self._conn.fetchone()[0]

    def create_dataset(self, well, name):
        query = f'''create table IF NOT EXISTS "{well}__{name}"
(
	reference double precision not null
		constraint "{well}__{name}_pk"
			primary key,
	values jsonb
);
create unique index IF NOT EXISTS "{well}__{name}_reference_uindex"
	on "{well}__{name}" (reference);'''
        self._conn.execute(query)
        self._engine.commit()
        return f"{well}__{name}"

    @my_timer
    def read_dataset(self, dataset_table_name, logs=None, depth=None, depth__gt=None, depth__lt=None):
        fields = 'values'
        if logs:
            fields = ','.join([f"values-> '{log}' as \"{log}\"" for log in logs])
        sql = f'select "reference", {fields} from "{dataset_table_name}" where 1=1'
        if depth:
            sql += f' and reference={depth}'
        elif depth__lt is not None or depth__gt is not None:
            if depth__lt is not None:
                sql += f' and reference < {depth__lt}'
            if depth__gt is not None:
                sql += f' and reference > {depth__gt}'
        else:
            sql += f' and reference != {META_REFERENCE}'

        self._conn.execute(sql)
        if logs:
            result =  {row[0]: {key: val for key, val in zip(logs, row[1:])} for row in self._conn}
        else:
            result = {depth: values for depth, values in self._conn}


        return result

    def update_dataset(self, dataset_table_name, depth, value, autocommit=True):
        sql = f'''INSERT INTO "{dataset_table_name}" VALUES ({depth}, '{json.dumps(value)}')
        ON CONFLICT (reference) DO UPDATE SET values = '{json.dumps(value)}' where "{dataset_table_name}".reference = {depth}'''
        self._conn.execute(sql)
        if autocommit:
            self._engine.commit()

    @my_timer
    def bulk_load_dataset(self, dataset_table_name, reference: list, values: list[dict], size: int = 8192, trunc=True) -> None:
        with self._engine.cursor() as cursor:
            cursor.execute(f'TRUNCATE "{dataset_table_name}";')

            data_to_insert = io.StringIO('\n'.join(f"{r}|{json.dumps(v)}" for r, v in zip(reference, values)))

            cursor.copy_from(data_to_insert, f'"{dataset_table_name}"', sep='|', size=size)

        self._engine.commit()

    def commit(self):
        self._engine.commit()

    def delete_curve(self, dataset_table_name, curve):
        query = sql.SQL('''UPDATE "{table}" SET values = values - '{curve}';'''.format(table=dataset_table_name, curve=curve))
        self._conn.execute(query)
        self.commit()

    def delete_dataset(self, well, dataset):
        self._conn.execute(f'DROP TABLE "{well}__{dataset}"')

    def delete_well(self, well):
        self._conn.execute(f"DELETE FROM wells where wellname = '{well}'")


class Storage2(Storage):
    def __init__(self, user=None, password=None, host=None, port=None, db=None):
        self._user = STORAGE_USER if user is None else user
        self._password = STORAGE_PASSWORD if password is None else password
        self._host = STORAGE_HOST if host is None else host
        self._port = STORAGE_PORT if port is None else port
        self._db = STORAGE_DB if db is None else db
        self._engine = psycopg2.connect(f"postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db}")
        self._conn = self._engine.cursor()

    def init_db(self):
        sql = '''create table if not exists wells
        (
        	wellname varchar(256) not null
        		constraint wells_pk
        			primary key,
        	info jsonb
        );'''
        self._conn.execute(sql)
        self._engine.commit()

    def flush_db(self):
        self._conn.execute(sql.SQL("DROP SCHEMA public CASCADE;"))
        self._conn.execute(sql.SQL(f"CREATE SCHEMA public;"))
        self.commit()

    def list_wells(self):
        self._conn.execute('select wellname from wells')
        return [well for well in self._conn]

    def create_well(self, name):
        self._conn.execute(f"INSERT INTO wells VALUES ('{name}','{{}}')")
        self._engine.commit()

    def update_well(self, name, info):
        _info = json.dumps(info)
        sql = f"""INSERT INTO wells (wellname, info) VALUES ('{name}', '{_info}') 
        ON CONFLICT (wellname) DO UPDATE 
        SET info = '{_info}' WHERE wells.wellname = '{name}'"""
        self._conn.execute(sql)
        self._engine.commit()

    def get_well_info(self, name):
        self._conn.execute(f"SELECT info FROM wells where wells.wellname = '{name}'")
        return self._conn.fetchone()[0]

    def create_dataset(self, well, name):
        query = sql.SQL(f'''
        CREATE TABLE public."{well}__{name}" (
	log varchar(64) NOT NULL primary key ,
	"data" jsonb NOT NULL
);
CREATE UNIQUE INDEX "{well}__{name}_log_idx" ON public."{well}__{name}" (log);
''')
        self._conn.execute(query)
        self._engine.commit()
        return f"{well}__{name}"

    @my_timer
    def read_dataset(self, dataset_table_name, logs=None, depth=None, depth__gt=None, depth__lt=None):

        # filter by logs
        where_clause = ""
        if logs is None:
            where_clause += f" and log != '{META_REFERENCE}'"
        elif len(logs) == 1:
            where_clause += f" and log = '{logs[0]}'"
        else:
            where_clause += " and log in ('" + "', '".join((str(l) for l in logs)) + "')"

        # fetch data
        query = f'select "log", data from "{dataset_table_name}" where 1=1 {where_clause}'
        self._conn.execute(query)
        # if logs:
        #     return {row[0]: {key: val for key, val in zip(logs, row[1:])} for row in self._conn}
        # else:
        data = my_timer(self._conn.fetchall)()


        # filter depths
        def slice_depth(ref, depth__gt, depth__lt) -> bool:
            float_ref = float(ref)
            if depth__gt is not None and float_ref < depth__gt:
                return False
            if depth__lt is not None and float_ref > depth__lt:
                return False
            return True

        if depth is not None:
            raise Exception("WRITE READ DATA FOR SINGLE DEPTH")
        elif depth__lt is not None or depth__gt is not None:
            out = {}
            for row in data:
                mnemonic = row[0]
                values = {ref: val for ref, val in row[1].items() if slice_depth(ref, depth__gt, depth__lt)}
                out.update({mnemonic: values})
            result = out
        else:
            result = {logs: values for logs, values in self._conn}

        return result

    def update_dataset(self, dataset_table_name, log, values, autocommit=True):
        query = f'''INSERT INTO "{dataset_table_name}" VALUES ({log}, '{json.dumps(values)}')
        on conflict ("log") do update set data = '{json.dumps(values)}' where "{dataset_table_name}"."log" = '{log}'  '''
        self._conn.execute(query)
        if autocommit:
            self._engine.commit()

    def bulk_load_dataset(self, dataset_table_name, reference: list, values: list[dict], size: int = 8192) -> None:
        with self._engine.cursor() as cursor:
            cursor.execute(f'TRUNCATE "{dataset_table_name}"')
            data_to_insert = io.StringIO(
                '\n'.join(
                    f"{key}|{json.dumps(val)}" for key, val in values.items()
                )
            )
            cursor.copy_from(data_to_insert, f'"{dataset_table_name}"', sep='|', size=size)

        self._engine.commit()

    def commit(self):
        self._engine.commit()

    def delete_curve(self, dataset_table_name, curve):
        query = sql.SQL('''UPDATE "{table}" SET values = values - '{curve}';'''.format(table=dataset_table_name, curve=curve))
        self._conn.execute(query)
        self.commit()

    def delete_dataset(self, well, dataset):
        self._conn.execute(f'DROP TABLE "{well}__{dataset}"')

    def delete_well(self, well):
        self._conn.execute(f"DELETE FROM wells where wellname = '{well}'")
