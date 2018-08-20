from abc import ABC, abstractmethod
import configparser
from datetime import datetime, timedelta
import dateutil.parser
from paho.mqtt.client import Client, MQTTMessage
import os
import signal
import time
from threading import Thread
import json
from ald.database_interface import OpenDatabaseConnection, DatabaseInterface
from queue import Queue


DEBUG = True
DB_CREDENTIALS_PATH = "credentials.conf"


def debug_print(message: str) -> None:
    if DEBUG:
        print("DEBUG:", message)


class Message(ABC):

    @abstractmethod
    def insert_into_db(self, database_connection: DatabaseInterface) -> None:
        pass

class SampleTemperatureMessage(Message):
    def __init__(self, temperature: float, resistance: float, timestamp: datetime) -> None:
        self._temperature = temperature
        self._resistance = resistance
        self._timestamp = timestamp

    def insert_into_db(self, database_connection: DatabaseInterface) -> None:
        database_connection.insert_sample_temperature(
            dateutil.parser.parse(self._timestamp), self._resistance, self._temperature
        )

        
class PressureMessage(Message):
    def __init__(self, timestamp: datetime, pressure: float) -> None:
        self._timestamp = timestamp
        self._pressure = pressure

    def insert_into_db(self, database_connection: DatabaseInterface) -> None:
        database_connection.insert_pressure(
            dateutil.parser.parse(self._timestamp), self._pressure
        )

        
class FlowMessage(Message):
    def __init__(self, temperature: float, volume_flow: float, mass_flow: float, pressure: float,
                 setpoint: float, timestamp: datetime) -> None:
        self._temperature = temperature
        self._volume_flow = volume_flow
        self._mass_flow = mass_flow
        self._pressure = pressure
        self._setpoint = setpoint
        self._timestamp = timestamp

    def insert_into_db(self, database_connection: DatabaseInterface) -> None:
        database_connection.insert_flow(
            dateutil.parser.parse(self._timestamp), self._volume_flow, self._mass_flow,
            self._pressure, self._setpoint, self._temperature
        )

class ValveMessage(Message):
    def __init__(self, timestamp: datetime, name: str, state: bool) -> None:
        self._timestamp = timestamp
        self._name = name
        self._state = state

    def insert_into_db(self, database_connection: DatabaseInterface) -> None:
        database_connection.insert_valves(
            dateutil.parser.parse(self._timestamp), self._name, self._state
        )

class TemperatureMessage(Message):
    def __init__(self, loop_id: int, datetime: datetime, temperature: float, wsp: float, tsp: float, power: float) -> None:
        self._loop_id = loop_id
        self._datetime = datetime
        self._temperature = temperature
        self._wsp, self._tsp = wsp, tsp
        self._power = power

    def insert_into_db(self, database_connection: DatabaseInterface) -> None:
        database_connection.insert_temperature(
            dateutil.parser.parse(self._datetime), self._loop_id, self._temperature,
            self._wsp, self._tsp, self._power
        )


class MQTTReceiver:
    """Asynchronously put ALD log data received via MQTT into a queue for processing.

    Always user in with ... as ... construct!

    Attributes:
        _queue  Queue into which MQTT data is inserted.
        _client
    """

    
    SAMPLE_TEMP_MESSAGE_KEYS = ["temperature", "resistance", "timestamp"]
    FLOW_MESSAGE_KEYS = ["temperature", "volflow", "massflow", "pressure", "setpoint", "timestamp"]
    PRESSURE_MESSAGE_KEYS = ["timestamp", "pressure"]
    TEMPERATURE_TOPICS = ['leftlinevacuum','centerlinevacuum','copperheater','rightlinevacuum',
                          'leftprecursor', 'leftlineair', 'rightlineair', 'rightprecursor']
    TEMPERATURE_MESSAGE_KEYS = ["loop", "datetime", "temperature", "wsp", "tsp", "power"]


    def __init__(self, data_queue: Queue) -> None:
        self._queue = data_queue
        
        self._client = Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

        self._client.username_pw_set("ald", "ald2017")
        self._client.connect("ald", port=1883, keepalive=60)

        debug_print("Initialised.")

    def __enter__(self):
        self._client.loop_start()
        debug_print("Loop started.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.loop_stop()
        debug_print("Loop stopped.")

    def _on_connect(self, client: Client, userdata, flags, rc) -> None:
        client.subscribe("ald/sample/temperature")
        client.message_callback_add('ald/sample/temperature', self._on_sample_temperature)

        client.subscribe("ald/flow/state")
        client.message_callback_add('ald/flow/state', self._on_flow_state)       

        client.subscribe("ald/pressure/main")
        client.message_callback_add('ald/pressure/main', self._on_pressure_main)

        client.subscribe("ald/io/state")
        client.message_callback_add('ald/io/state', self._on_valves)

        for topic in self.TEMPERATURE_TOPICS:
            client.subscribe("ald/temperature/{}".format(topic))
            client.message_callback_add("ald/temperature/{}".format(topic),
                                        self._on_temperature)

        debug_print("Connected.")

    def _on_message(self, client: Client, userdata, message: MQTTMessage) -> None:
        debug_print('DEBUG {}'.format(message.payload))

    def _on_flow_state(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        temp = values[self.FLOW_MESSAGE_KEYS[0]]
        volflow = values[self.FLOW_MESSAGE_KEYS[1]]
        massflow = values[self.FLOW_MESSAGE_KEYS[2]]
        pressure = values[self.FLOW_MESSAGE_KEYS[3]]
        setpoint = values[self.FLOW_MESSAGE_KEYS[4]]
        timestamp = values[self.FLOW_MESSAGE_KEYS[5]]

        self._queue.put(FlowMessage(temp, volflow, massflow, pressure, setpoint, timestamp))
            
    def _on_pressure_main(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        timestamp = values[self.PRESSURE_MESSAGE_KEYS[0]]
        pressure = values[self.PRESSURE_MESSAGE_KEYS[1]]

        self._queue.put(PressureMessage(timestamp, pressure))

    def _on_sample_temperature(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        temp = values[self.SAMPLE_TEMP_MESSAGE_KEYS[0]]
        resistance = values[self.SAMPLE_TEMP_MESSAGE_KEYS[1]]
        datetime = values[self.SAMPLE_TEMP_MESSAGE_KEYS[2]]

        self._queue.put(SampleTemperatureMessage(temp, resistance, datetime))

    def _on_valves(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        
        for valve_name, valve_data in values.items():
            state = valve_data["state"]
            datetime = valve_data["timestamp"]

            self._queue.put(ValveMessage(datetime, valve_name, state))

    def _on_temperature(self, client: Client, userdata, message: MQTTMessage) -> None:
        values = json.loads(message.payload.decode())
        debug_print("Message received: {}".format(values))

        loop = values[self.TEMPERATURE_MESSAGE_KEYS[0]]
        datetime = values[self.TEMPERATURE_MESSAGE_KEYS[1]]
        temperature = values[self.TEMPERATURE_MESSAGE_KEYS[2]]
        wsp = values[self.TEMPERATURE_MESSAGE_KEYS[3]]
        tsp = values[self.TEMPERATURE_MESSAGE_KEYS[4]]
        power = values[self.TEMPERATURE_MESSAGE_KEYS[5]]

        self._queue.put(TemperatureMessage(loop, datetime, temperature,
                                           wsp, tsp, power))


        
class DatabaseWorker():
    """Move ALD log data items from a queue into an SQL database."""

    COMMIT_FREQUENCY = timedelta(seconds=10)

    def __init__(self, queue: Queue, database_connection: DatabaseInterface) -> None:
        self._queue = queue
        self._db_connection = database_connection
        self._time_of_last_commit = datetime.now()

    def process_data(self) -> None:
        """Process new data synchronously."""
        while True:
            item = self._queue.get()  # type: Message  # Blocks if no items are available
            item.insert_into_db(self._db_connection)

            time_since_last_commit = datetime.now() - self._time_of_last_commit
            if time_since_last_commit / self.COMMIT_FREQUENCY:
                self.commit()

    def commit(self) -> None:
        """Commit pending changes to the database."""
        self._db_connection.commit()




if __name__ == "__main__":
    data_queue = Queue()  # type: Queue[Message]

    if not os.path.isfile(DB_CREDENTIALS_PATH):
        print("ERROR: No database credentials file found at expected path '{}'".format(DB_CREDENTIALS_PATH))
        exit(1)
        
    db_credentials = configparser.ConfigParser()
    try:
        db_credentials.read(DB_CREDENTIALS_PATH)
        section = db_credentials["Credentials"]
        db_hostname = section["Hostname"]
        db_username = section["Username"]
        db_password = section["Password"]
        db_name = section["DatabaseName"]
    except:
        print("ERROR: Failed to parse credentials file.\n"
              "Expected format:\n\n"
              "[Credentials]\n"
              "Hostname = myhost\n"
              "Username = mydbuser\n"
              "Password = mydbpassword\n"
              "DatabaseName = mydbname\n")
        exit(2)
    
    
    with OpenDatabaseConnection(db_hostname, db_username, db_password, db_name) as db_connection:
        with MQTTReceiver(data_queue) as receiver:
            try:
                worker = DatabaseWorker(data_queue, db_connection)
                worker.process_data()
            except KeyboardInterrupt:
                db_connection.commit()

    exit(0)

