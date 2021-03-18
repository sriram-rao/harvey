from datetime import datetime
import psycopg2
import string


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
        count = cursor.rowcount
        cursor.close()
        return result, count

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
                  f"AND cohort = {cohort} AND message = '{event}';"
        return self.execute(command) > 0

    def remove_transaction(self, transaction: string) -> bool:
        command = f"DELETE FROM transactionlog WHERE transaction = '{transaction}';"
        return self.execute(command) > 0

    def get_last_status(self) -> (string, string):
        query = f"SELECT transaction, message FROM transactionlog WHERE cohort = 0 ORDER BY eventtime DESC LIMIT 1;"
        row, count = self.fetch_entity(query)
        return row[0], row[1]

    def get_status(self, transaction: string, cohort: int) -> string:
        query = f"SELECT message FROM transactionlog WHERE transaction = '{transaction}' AND cohort = {cohort} " \
                f"ORDER BY eventtime DESC LIMIT 1;"
        row, count = self.fetch_entity(query)
        if count == 0:
            return 'complete'
        return row[0]

    def is_complete(self, transaction: string) -> bool:
        query = f"SELECT COUNT(*) FROM transactionlog WHERE transaction = '{transaction}' AND cohort <> 0;"
        row, count = self.fetch_entity(query)
        return row[0] == 0
