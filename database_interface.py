import MySQLdb
import pandas as pd
from time import time, sleep

from datetime import datetime

class DatabaseInterface:

    INSERT_FLOW_STR = ('INSERT INTO flow (datetime, volume_flow, mass_flow, pressure, set_point, temperature) '
                       'VALUES ("{datetime}", {volume_flow}, {mass_flow}, {pressure}, {set_point}, {temperature});')

    INSERT_PRESSURE_STR = ('INSERT INTO pressure (datetime, pressure) VALUES ("{datetime}", {pressure});')

    INSERT_SAMPLE_STR = ('INSERT INTO sample (datetime, resistance, temperature) '
                         'VALUES ("{datetime}", {resistance}, {temperature});')

    INSERT_TEMPERATURE_STR = ('INSERT INTO temperature '
                              '(datetime, loop_id, temperature, working_set_point, target_set_point, output_power) '
                              'VALUES ("{datetime}", {loop_id}, {temperature}, {working_set_point}, '
                              '{target_set_point}, {output_power});')

    INSERT_VALVES_STR = ('INSERT INTO valves (datetime, name, state) VALUES ("{datetime}", "{name}", {state});')

    INSERT_PROCESS_LOG_STR = ('INSERT INTO process_log (start_time, end_time, cycle_no, phase) '
                              'VALUES ("{start_time}", "{end_time}", {cycle_no}, "{phase}");')

    GET_FLOW_RANGE_STR = ('SELECT datetime, volume_flow, mass_flow, pressure, set_point, temperature from flow '
                          'where datetime between "{start}" and {end};')

    GET_SAMPLE_RANGE_STR = ('SELECT datetime, resistance, temperature from sample '
                            'where datetime between "{start}" and {end};')

    GET_PRESSURE_RANGE_STR = ('SELECT datetime, pressure from pressure '
                              'where datetime between "{start}" and {end};')

    GET_VALVES_RANGE_STR =   ('SELECT datetime, name, state from valves '
                              'where datetime between "{start}" and {end};')

    GET_TEMPERATURE_RANGE_STR =   ( 'SELECT datetime, loop_id, temperature, working_set_point, '
                                    'target_set_point, output_power from temperature '
                                    'where datetime between "{start}" and {end};')

    GET_PROCESS_LOG_STR = ('SELECT start_time, end_time, cycle_no, phase FROM process_log '
                           'WHERE start_time between "{start}" and {end};')

    def __init__(self, connection):
        self._connection = connection
        self._new_to_commit = False

    def get_flow(self, start_timestamp, end_timestamp = None):
        return self._get(DatabaseInterface.GET_FLOW_RANGE_STR, start_timestamp, end_timestamp)

    def get_sample_temperature(self, start_timestamp, end_timestamp = None):
        return self._get(DatabaseInterface.GET_SAMPLE_RANGE_STR, start_timestamp, end_timestamp)

    def get_pressure(self, start_timestamp, end_timestamp = None):
        return self._get(DatabaseInterface.GET_PRESSURE_RANGE_STR, start_timestamp, end_timestamp)

    def get_valves(self, start_timestamp, end_timestamp = None):
        return self._get(DatabaseInterface.GET_VALVES_RANGE_STR, start_timestamp, end_timestamp)

    def get_temperature(self, start_timestamp, end_timestamp = None):
        return self._get(DatabaseInterface.GET_TEMPERATURE_RANGE_STR, start_timestamp, end_timestamp)

    def get_process_log(self, start_timestamp, end_timestamp = None):
        return self._get(DatabaseInterface.GET_PROCESS_LOG_STR, start_timestamp, end_timestamp)

    def _get(self, sql_query, start_timestamp, end_timestamp = None):

        args = {'start': start_timestamp.isoformat(),
                'end': '"{}"'.format(end_timestamp.isoformat()) if end_timestamp is not None else 'NOW()'}

        cursor = self._connection.cursor()
        sql_str = sql_query.format(**args)
        cursor.execute(sql_str)

        name_list = [d[0] for d in cursor.description]
        list_of_lists = [list(row) for row in cursor.fetchall()]
        df = pd.DataFrame(list_of_lists, columns=name_list)

        return df

    def insert_flow(self, datetime=datetime.now(), volume_flow=0, mass_flow=0, pressure=0, set_point=0, temperature=0):
        args = {'datetime': datetime.isoformat(), 'volume_flow': volume_flow, 'mass_flow':mass_flow, 'pressure': pressure,
                'set_point': set_point, 'temperature': temperature}

        self._set(DatabaseInterface.INSERT_FLOW_STR.format(**args))


    def insert_pressure(self, datetime=datetime.now(), pressure=0):
        args = {'datetime': datetime.isoformat(), 'pressure': pressure}

        self._set(DatabaseInterface.INSERT_PRESSURE_STR.format(**args))

    def insert_sample_temperature(self, datetime=datetime.now(), resistance=0, temperature=0):
        args = {'datetime': datetime.isoformat(), 'resistance': resistance, 'temperature': temperature}

        self._set(DatabaseInterface.INSERT_SAMPLE_STR.format(**args))

    def insert_temperature(self, datetime=datetime.now(), loop_id=0, temperature=0, working_set_point=0,
                           target_set_point=0, output_power=0):
        args = {'datetime': datetime.isoformat(), 'loop_id': loop_id, 'temperature': temperature,
                'working_set_point': working_set_point, 'target_set_point': target_set_point,
                'output_power': output_power}

        self._set(DatabaseInterface.INSERT_TEMPERATURE_STR.format(**args))

    def insert_valves(self, datetime=datetime.now(), name='', state=False):
        args = {'datetime': datetime.isoformat(), 'name': name, 'state': state}

        self._set(DatabaseInterface.INSERT_VALVES_STR.format(**args))

    def insert_process_log(self, start_time=datetime.now(), end_time=datetime.now(), cycle_no=0, phase="platinum"):
        args = {'start_time': start_time.isoformat(), 'end_time': end_time.isoformat(), 'cycle_no': cycle_no,
                'phase': phase}

        self._set(DatabaseInterface.INSERT_PROCESS_LOG_STR.format(**args))


    def _set(self, sql_query):
        cursor = self._connection.cursor()
        cursor.execute(sql_query)
        self._new_to_commit = True

    def commit(self):
        if self._new_to_commit:
            self._new_to_commit = False
            self._connection.commit()

class OpenDatabaseConnection:
    def __init__(self, host, user, passwd, db):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db = db

        self._connection = None


    def __enter__(self):
        self._connection = MySQLdb.connect( host=self._host,
                                            user=self._user,
                                            passwd=self._passwd,
                                            db=self._db)

        return DatabaseInterface(self._connection)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.commit()
        self._connection.close()


if '__main__' == __name__:
    now = datetime(1, 1, 1)

    host = input("Database host name: ")
    user = input("User name: ")
    import getpass
    passwd = getpass.getpass(prompt='Password: ')
    database = input("Database name: ")

    with OpenDatabaseConnection(host, user, passwd, database) as db:
        db.insert_flow()
        db.insert_pressure()
        db.insert_sample_temperature()
        db.insert_temperature()
        db.insert_valves()
        db.insert_process_log()

    with OpenDatabaseConnection(host, user, passwd, database) as db:
        print(db.get_flow(now))
        print(db.get_pressure(now))
        print(db.get_sample_temperature(now))
        print(db.get_temperature(now))
        print(db.get_valves(now))
        print(db.get_process_log(now))



