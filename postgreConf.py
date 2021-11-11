import psycopg2
from config import *
import json
import uuid
import psycopg2.extras

psycopg2.extras.register_uuid()

class PostgreSQL:

    def __init__(self):
        self.connection = psycopg2.connect(
            host = host,
            user = user,
            password = password,
            database = db_name,
            port = port)

    def get_user_data(self, user_id):
        """ Находит internal_user по author_id в другой табличке , и выводит его данные в виде имени итд"""

        
        with self.connection.cursor() as cursor:
            cursor.execute(""" SELECT internal_user_id FROM users WHERE user_id = (%s) """, (user_id, ))
            internal_user_id = cursor.fetchone()

        with self.connection.cursor() as cursor:
            cursor.execute(""" SELECT internal_user_first_name, internal_user_last_name, internal_user_primary_email FROM internal_users WHERE internal_user_id = (%s) """, (internal_user_id))
            user = cursor.fetchone()

            if user is not None:
                first_name = user[0]
                last_name = user[1]
                email = user[2]
                return (first_name, last_name, email)
    
    def get_organization_name(self, organization_id):
        """ Получение имени отдела по linked_organization_ids """

        with self.connection.cursor() as cursor:
            cursor.execute(""" SELECT organization_name FROM organizations WHERE organization_id = (%s)""", (organization_id, ))

            return cursor.fetchone()[0]

    def get_events(self, chat_room_id):

        with self.connection.cursor() as cursor:
            cursor.execute(""" SELECT * FROM events WHERE event_object_id = (%s)  """, (uuid.UUID(chat_room_id),) )
            return cursor.fetchall()

    def close_db(self):
        """ Закрытия базы данных"""
        return self.connection.close()

