import logging
from psycopg2.extras import RealDictCursor
from functools import wraps
from typing import *
from threading import Thread
from queue import Queue
import databases
import utils
import psycopg2
import uuid
import os

# Configure the logging tool in the AWS Lambda function.
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Initialize constants with parameters to configure.
POSTGRESQL_USERNAME = os.environ["POSTGRESQL_USERNAME"]
POSTGRESQL_PASSWORD = os.environ["POSTGRESQL_PASSWORD"]
POSTGRESQL_HOST = os.environ["POSTGRESQL_HOST"]
POSTGRESQL_PORT = int(os.environ["POSTGRESQL_PORT"])
POSTGRESQL_DB_NAME = os.environ["POSTGRESQL_DB_NAME"]

# The connection to the database will be created the first time the AWS Lambda function is called.
# Any subsequent call to the function will use the same database connection until the container stops.
POSTGRESQL_CONNECTION = None


def run_multithreading_tasks(functions: List[Dict[AnyStr, Union[Callable, Dict[AnyStr, Any]]]]) -> Dict[AnyStr, Any]:
    # Create the empty list to save all parallel threads.
    threads = []

    # Create the queue to store all results of functions.
    queue = Queue()

    # Create the thread for each function.
    for function in functions:
        # Check whether the input arguments have keys in their dictionaries.
        try:
            function_object = function["function_object"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        try:
            function_arguments = function["function_arguments"]
        except KeyError as error:
            logger.error(error)
            raise Exception(error)

        # Add the instance of the queue to the list of function arguments.
        function_arguments["queue"] = queue

        # Create the thread.
        thread = Thread(target=function_object, kwargs=function_arguments)
        threads.append(thread)

    # Start all parallel threads.
    for thread in threads:
        thread.start()

    # Wait until all parallel threads are finished.
    for thread in threads:
        thread.join()

    # Get the results of all threads.
    results = {}
    while not queue.empty():
        results = {**results, **queue.get()}

    # Return the results of all threads.
    return results


def reuse_or_recreate_postgresql_connection():
    global POSTGRESQL_CONNECTION
    if not POSTGRESQL_CONNECTION:
        try:
            POSTGRESQL_CONNECTION = databases.create_postgresql_connection(
                POSTGRESQL_USERNAME,
                POSTGRESQL_PASSWORD,
                POSTGRESQL_HOST,
                POSTGRESQL_PORT,
                POSTGRESQL_DB_NAME
            )
        except Exception as error:
            logger.error(error)
            raise Exception("Unable to connect to the PostgreSQL database.")
    return POSTGRESQL_CONNECTION


def postgresql_wrapper(function):
    @wraps(function)
    def wrapper(**kwargs):
        try:
            postgresql_connection = kwargs["postgresql_connection"]
            if postgresql_connection.closed:
                postgresql_connection = psycopg2.connect(user=POSTGRESQL_USERNAME, password=POSTGRESQL_PASSWORD,
                                                         host=POSTGRESQL_HOST, port=POSTGRESQL_PORT,
                                                         database=POSTGRESQL_DB_NAME)
                postgresql_connection.autocommit = True
        except KeyError as error:
            logger.error(error)
            raise Exception(error)
        cursor = postgresql_connection.cursor(cursor_factory=RealDictCursor)
        kwargs["cursor"] = cursor
        result = function(**kwargs)
        cursor.close()
        return result
    return wrapper


@postgresql_wrapper
def get_chatroom_events(**kwargs) -> None:
    # Check if the input dictionary has all the necessary keys.
    cursor = kwargs["cursor"]
    sql_arguments = kwargs["sql_arguments"]
    queue = kwargs["queue"]

    # Prepare the SQL request that return definite information about exist events.
    sql_statement = """
    SELECT
        event_name::text,
        event_created_at::text,
        event_new_value -> 'linked_organizations_ids' as linked_organizations_ids,
        event_new_value -> 'organizations_ids' as organizations_ids,
        event_new_value -> 'operator_id' as operator_id,
        event_author_id
    FROM
        events
    WHERE
        events.event_object_id = %(chatroom_id)s
    AND
        events.event_name IN %(events_name)s
    ORDER BY
        event_created_at DESC
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement, sql_arguments)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    queue.put({"chatroom_events": cursor.fetchall()})


@postgresql_wrapper
def get_internal_users_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    cursor = kwargs["cursor"]
    queue = kwargs["queue"]

    # Prepare the SQL request that return all internal users data
    sql_statement = """
    select
        users.user_id::text,
        internal_users.internal_user_first_name::text,
        internal_users.internal_user_last_name::text,
        internal_users.internal_user_middle_name::text,
        internal_users.internal_user_primary_email::text
    from
        users
    right join internal_users on
        users.internal_user_id = internal_users.internal_user_id
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Prepare dictionary for the further clear process, instead RealDictCursor
    internal_users = {}
    for i in cursor.fetchall():
        internal_users[i["user_id"]] = {
            "internal_user_first_name": i["internal_user_first_name"],
            "internal_user_last_name": i["internal_user_last_name"],
            "internal_user_primary_email": i["internal_user_primary_email"]
        }
    queue.put({"internal_users": internal_users})


@postgresql_wrapper
def get_organizations_data(**kwargs) -> Any:
    # Check if the input dictionary has all the necessary keys.
    cursor = kwargs["cursor"]
    queue = kwargs["queue"]

    # Prepare the SQL request that return internal users data
    sql_statement = """
    select
        organizations.organization_id::text,
        organizations.organization_name::text
    from
        organizations
    """

    # Execute the SQL query dynamically, in a convenient and safe way.
    try:
        cursor.execute(sql_statement)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    # Prepare dictionary for the further clear process, instead RealDictCursor
    organizations = {}
    for i in cursor.fetchall():
        organizations[i["organization_id"]] = i["organization_name"]

    queue.put({"organizations": organizations})


def events_cleaning_process(**kwargs) -> Any:

    # Check if the input dictionary has all the necessary keys.
    chatroom_events = kwargs["chatroom_events"]
    organizations = kwargs["organizations"]
    internal_users = kwargs["internal_users"]

    sorted_and_cleaning_data = []

    for event in chatroom_events:
        event_name = event['event_name']
        camelcase_event_name = utils.camel_case(event_name)
        created_at = event['event_created_at']
        results = {'eventName': camelcase_event_name, 'createdAt': created_at}

        if event_name == 'take_room':
            results["operator"] = {
                'operatorId': event['event_author_id'],
                'firstName': internal_users[event['event_author_id']]['internal_user_first_name'],
                'lastName': internal_users[event['event_author_id']]['internal_user_last_name'],
                'email': internal_users[event['event_author_id']]['internal_user_primary_email']
            }

        elif event_name == 'assign_room_to_user_by_supervisor' or \
                event_name == 'detach_from_user_and_move_to_another_user':
            results["from"] = {
                'operatorId': event['event_author_id'],
                'firstName':  internal_users[event['event_author_id']]['internal_user_first_name'],
                'lastName': internal_users[event['event_author_id']]['internal_user_last_name'],
                'email': internal_users[event['event_author_id']]['internal_user_primary_email']
            }
            results["to"] = {
                'operatorId': event['operator_id'],
                'firstName':  internal_users[event['operator_id']]['internal_user_first_name'],
                'lastName': internal_users[event['operator_id']]['internal_user_last_name'],
                'email': internal_users[event['operator_id']]['internal_user_primary_email']
            }

        elif event_name == 'change_room_visibility':
            if not event["linked_organizations_ids"]:
                organizations_data = []
                for i in event["organizations_ids"]:
                    organizations_data.append({
                        "organizations": {
                            "organizationId": i,
                            "organizationName": organizations[i]
                        }
                    })
                results["organizations"] = organizations_data
            else:
                results["organizations"] = [{
                        "organizationId": event["linked_organizations_ids"][0],
                        "organizationName": organizations[event["linked_organizations_ids"][0]]
                    }]

        elif event_name == 'detach_from_user_and_move_to_organizations':
            if not event["linked_organizations_ids"]:
                organizations_data = []
                for i in event["organizations_ids"]:
                    organizations_data.append({
                        "organizations": {
                            "organizationId": i,
                            "organizationName": organizations[i]
                        }
                    })
                results["operator"] = {
                    'operatorId': event['event_author_id'],
                    'firstName': internal_users[event['event_author_id']]['internal_user_first_name'],
                    'lastName': internal_users[event['event_author_id']]['internal_user_last_name'],
                    'email': internal_users[event['event_author_id']]['internal_user_primary_email']
                }
                results["organizations"] = organizations_data

            else:
                results["operator"] = {
                    'operatorId': event['event_author_id'],
                    'firstName': internal_users[event['event_author_id']]['internal_user_first_name'],
                    'lastName': internal_users[event['event_author_id']]['internal_user_last_name'],
                    'email': internal_users[event['event_author_id']]['internal_user_primary_email']
                    }
                results["organizations"] = [{
                    "organizationId": event["linked_organizations_ids"][0],
                    "organizationName": organizations[event["linked_organizations_ids"][0]]
                }]

        sorted_and_cleaning_data.append(results)
    return sorted_and_cleaning_data


def lambda_handler(event, context):
    """
    :param event: The AWS Lambda function uses this parameter to pass in event data to the handler.
    :param context: The AWS Lambda function uses this parameter to provide runtime information to your handler.
    """

    # check input arguments
    try:
        chatroom_id = event["arguments"]["chatRoomId"]
        uuid.UUID(chatroom_id)
    except Exception as error:
        logger.error(error)
        raise Exception(error)

    postgresql_connection = reuse_or_recreate_postgresql_connection()

    results_of_tasks = run_multithreading_tasks([
        {
            "function_object": get_organizations_data,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {}
            }
        },
        {
            "function_object": get_internal_users_data,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {}
            }
        },
        {
            "function_object": get_chatroom_events,
            "function_arguments": {
                "postgresql_connection": postgresql_connection,
                "sql_arguments": {
                    "chatroom_id": chatroom_id,
                    "events_name": (
                        "create_new_room",
                        "detach_from_user_and_move_to_another_user",
                        "change_room_visibility",
                        'take_room',
                        "assign_room_to_user_by_supervisor",
                        "activate_completed_room",
                        "detach_from_user_and_move_to_organizations",
                        "complete_room",
                        "init_new_bot_session")
                    }
                }
            }
        ])

    chatroom_events = results_of_tasks["chatroom_events"]
    internal_users = results_of_tasks["internal_users"]
    organizations = results_of_tasks["organizations"]

    # Cleaning and sorting process
    chatroom_history = events_cleaning_process(
        chatroom_events=chatroom_events,
        internal_users=internal_users,
        organizations=organizations
    )

    # Return the chatroom_history
    return chatroom_history

