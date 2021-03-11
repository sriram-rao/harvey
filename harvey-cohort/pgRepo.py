from datetime import datetime
import psycopg2
import string
from psycopg2._psycopg import DatabaseError


class PostgresRepo:

    def __init__(self, port: int):
        self.port = port
        self.connection = psycopg2.connect(
            host="localhost",
            port=port,
            database="sensors",
            user="sriramrao",
            password="")

        self.observe_map = {
            'temperature': self.observe_temperature,
            'wemo': self.observe_wemo,
            'wifi': self.observe_wifi
        }

    def fetch_entity(self, sql: string):
        cursor = self.connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result

    def fetch_entities(self, sql: string):
        cursor = self.connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute(self, command: string, transaction="trans1") -> int:
        transaction_id = self.get_transaction_id(transaction)
        self.connection.tpc_begin(transaction_id)
        row_count = self.execute_single(command)
        self.connection.tpc_prepare()
        return row_count

    def execute_single(self, command: string) -> int:
        cursor = self.connection.cursor()
        cursor.execute(command)
        row_count = cursor.rowcount
        cursor.close()
        return row_count

    def begin_transaction(self, transaction: string):
        transaction_id = self.get_transaction_id(transaction)
        self.connection.tpc_begin(transaction_id)
        # command = f"BEGIN TRANSACTION;"
        # self.execute(command)

    def prepare_commit(self, name: string) -> bool:
        try:
            self.connection.tpc_prepare()
            # command = f"PREPARE TRANSACTION '{name}';"
            return True  # self.execute(command) >= 0
        except Exception:
            return False

    def commit_prepared(self):
        self.connection.tpc_commit()

    def recover_commit_prepared(self, name: string):
        self.connection.commit()
        self.connection.tpc_commit(self.get_transaction_id(name))
        # command = f"COMMIT PREPARED '{name}';"
        # self.execute(command)

    def abort_prepared(self):
        self.connection.tpc_rollback()

    def recover_abort_prepared(self, name: string):
        self.connection.tpc_rollback(self.get_transaction_id(name))
        # command = f"ROLLBACK PREPARED '{name}';"
        # self.execute(command)

    def log(self, transaction: string, event: string) -> bool:
        command = f"INSERT INTO transactionlog (transaction, cohort, message, eventtime) " \
                  f"VALUES('{transaction}', 0, '{event}', '{datetime.utcnow()}');"
        result = self.execute_single(command) > 0
        self.connection.commit()
        return result

    def get_last_status(self) -> (string, string):
        query = f"SELECT transaction, message FROM transactionlog WHERE cohort = 0 ORDER BY eventtime DESC LIMIT 1"
        row = self.fetch_entity(query)
        return row[0], row[1]

    def get_prepared_count(self) -> int:
        query = f"SELECT COUNT(*) FROM pg_prepared_xacts;"
        return self.fetch_entity(query)[0]

    def observe(self, sensor_type: string, transaction: string, data: list) -> bool:
        return self.observe_map[sensor_type](transaction, data)

    def observe_temperature(self, transaction: string, data: list) -> bool:
        command = f"INSERT INTO ThermometerObservation (id, temperature, timeStamp, sensor_id) VALUES "
        command += ', '.join(f"('{row['id']}', {row['temperature']}, '{row['timeStamp']}', '{row['sensor_id']}')"
                             for row in data)
        return self.execute(command, transaction) > 0

    def observe_wemo(self, transaction: string, data: list) -> bool:
        command = f"INSERT INTO WeMoObservation (id, currentMilliWatts, onTodaySeconds, timeStamp, sensor_id) VALUES "
        command += ', '.join(f"('{row['id']}', {row['currentMilliWatts']}, {row['onTodaySeconds']}, "
                             f"'{row['timeStamp']}', '{row['sensor_id']}')" for row in data)
        return self.execute(command, transaction) > 0

    def observe_wifi(self, transaction: string, data: list) -> bool:
        command = f"INSERT INTO WiFiAPObservation (id, clientId, timeStamp, sensor_id) VALUES "

        command += ', '.join(f"('{row['id']}', {row['clientId']}, '{row['timeStamp']}', '{row['sensor_id']}')"
                             for row in data)
        return self.execute(command, transaction) > 0

    def get_transaction_id(self, name: string) -> string:
        return self.connection.xid(1, name, "branch")
