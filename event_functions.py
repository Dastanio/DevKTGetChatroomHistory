from postgreConf import PostgreSQL
import uuid

obj = PostgreSQL()


def create_new_room_and_complete_room(created_date, camelcase_event_name):
    return {
        'eventName': camelcase_event_name,
        'createdAt': created_date
    }

def detach_from_user_and_move_to_another_user(created_date, operator, operator_id, camelcase_event_name, event_new_value):
    new_operator = obj.get_user_data(event_new_value['operator_id'])
    new_operator_id = event_new_value['operator_id']

    return {
        'eventName': camelcase_event_name,
        'createdAt': created_date,
        'operatorFrom': {
            'operatorId': operator_id,
            'firstName': operator[0],
            'lastName': operator[1],
            'email': operator[2]
        },
        'operatorTo': {
            'operatorId': new_operator_id,
            'firstName': new_operator[0],
            'lastName': new_operator[1],
            'email': operator[2]
        },
    }

def change_room_visibility(created_date, camelcase_event_name, event_new_value):
    linked_organizations_ids = event_new_value['linked_organizations_ids']
    if linked_organizations_ids == []:

        organizations_data = []
        for x in event_new_value['organization_ids']:
            converted_id = obj.get_organization_name(uuid.UUID(x))
            organizations_data.append({
                'organizationId': x,
                'organizationName': converted_id
            })

        return {
            'eventName': camelcase_event_name,
            'createdAt': created_date,
            'organizations': organizations_data
        }
    else:
        organization_name = obj.get_organization_name(event_new_value['linked_organizations_ids'][0])

        return {
            'eventName': camelcase_event_name,
            'createdAt': created_date,
            'organizations': {
                'organizationId': linked_organizations_ids[0],
                'organizationName': organization_name,
            }
        }

def take_room(created_date, operator, operator_id, camelcase_event_name):
    return {
        'eventName': camelcase_event_name,
        'createdAt': created_date,
        'operator': {
            'operatorId': operator_id,
            'firstName': operator[0],
            'lastName': operator[1],
            'email': operator[2]
        }
    }


def detach_from_user_and_move_to_organizations(created_date, operator, operator_id, camelcase_event_name,event_new_value):

    linked_organizations_ids = event_new_value['linked_organizations_ids']

    if linked_organizations_ids == []:

        organizations_data = []
        for x in event_new_value['organizations_ids']:
            converted_id = obj.get_organization_name(uuid.UUID(x))
            organizations_data.append({
                'organizationId': x,
                'organizationName': converted_id
            })

        return {
            'eventName': camelcase_event_name,
            'createdAt': created_date,
            'operator': {
                'operatorId': operator_id,
                'firstName': operator[0],
                'lastName': operator[1],
                'email': operator[2]
            },
            'organizations': organizations_data
        }
    else:
        organization_name = obj.get_organization_name(event_new_value['linked_organizations_ids'][0],)
        return {
            'eventName': camelcase_event_name,
            'createdAt': created_date,
            'operator': {
                'operatorId': operator_id,
                'firstName': operator[0],
                'lastName': operator[1],
                'email': operator[2]
            },
            'organizations': {
                'organizationId':  linked_organizations_ids[0],
                'ogranizationName': organization_name
            },
        }



def assign_room_to_user_by_supervisor(created_date, operator, operator_id, camelcase_event_name, event_new_value):

    new_operator = obj.get_user_data(event_new_value['operator_id'])
    new_operator_id = event_new_value['operator_id']

    return {
        'eventName': camelcase_event_name,
        'createdAt': created_date,
        'superviserFrom': {
            'superviserId': operator_id,
            'firstName': operator[0],
            'lastName': operator[1],
            'email': operator[2]
        },
        'operatorTo': {
            'operatorId': new_operator_id,
            'firstName': new_operator[0],
            'lastName': new_operator[1]}
        }

def activate_completed_room(created_date, camelcase_event_name):
    return {
        'eventName': camelcase_event_name,
        'createdAt': created_date,
    }