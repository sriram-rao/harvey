import string
import psycopg2
from flask import current_app


class PostgresRepo:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port=current_app.config.get('POSTGRES_PORT'),
            database="sensors",
            user="sriramrao",
            password="")

    def fetch_entity(self, sql: string):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result

    def fetch_entities(self, sql: string):
        cursor = self.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute(self, command: string):
        cursor = self.conn.cursor()
        cursor.execute(command)
        cursor.commit()
        cursor.close()
