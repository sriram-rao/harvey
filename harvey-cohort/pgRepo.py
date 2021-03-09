from datetime import datetime
from flask import current_app
import psycopg2
import string

from psycopg2._psycopg import DatabaseError


class PostgresRepo:
    def __init__(self, port: int):
        self.connection = psycopg2.connect(
            host="localhost",
            port=port,
            database="sensors",
            user="sriramrao",
            password="")

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

    def execute(self, command: string) -> int:
        cursor = self.connection.cursor()
        cursor.execute(command)
        row_count = cursor.rowcount
        self.connection.commit()
        cursor.close()
        return row_count

    def begin_transaction(self, name: string):
        command = f"BEGIN TRANSACTION {name}"
        self.execute(command)

    def prepare_commit(self, name: string) -> bool:
        try:
            command = f"PREPARE TRANSACTION {name}"
            return self.execute(command) >= 0
        except Exception:
            return False

    def commit_prepared(self, name: string):
        command = f"COMMIT PREPARED {name}"
        self.execute(command)

    def abort_prepared(self, name: string):
        command = f"ROLLBACK PREPARED {name}"
        self.execute(command)

    def log(self, transaction: string, event: string) -> bool:
        command = f"INSERT INTO transactionlog (transaction, cohort, message, eventtime) " \
                  f"VALUES('{transaction}', {current_app.config.get('COHORT')}, '{event}', '{datetime.utcnow()}');"
        return self.execute(command) > 0
