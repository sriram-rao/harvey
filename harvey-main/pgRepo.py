import string
from datetime import datetime

import psycopg2
from flask import current_app


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

    def log(self, transaction: string, cohort: int, event: string) -> bool:
        command = f"INSERT INTO transactionlog (transaction, cohort, message, eventtime) " \
                  f"VALUES('{transaction}', {cohort}, '{event}', '{datetime.utcnow()}');"
        return self.execute(command) > 0

    def remove_log(self, transaction: string, cohort: int, event: string) -> bool:
        command = f"DELETE FROM transactionlog WHERE transaction = '{transaction}' " \
                  f"AND cohort = {cohort} AND message = '{event}'"
        return self.execute(command) > 0
