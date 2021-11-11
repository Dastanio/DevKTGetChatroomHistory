from postgreConf import PostgreSQL
import json
import uuid
import re
from event_functions import *

obj = PostgreSQL()
events = obj.get_events('1bba6cbc-3bd3-11ec-8882-6fb7232d36df')

def to_camelcase(s):
    return re.sub(r'(?!^)_([a-zA-Z])', lambda m: m.group(1).upper(), s)

def data_cleaning_process():

    data = []
    print(len(events))
    for i in events:

        event_name = i[1]
        camelcase_event_name = to_camelcase(i[1])
        operator = obj.get_user_data(i[5])
        operator_id = f'{i[5]}'
        created_date = str(i[4])
        event_new_value = i[7]

        if event_name == 'create_new_room':
            #Создание диалога
            data.append(create_new_room_and_complete_room(created_date, camelcase_event_name))

        if event_name == 'detach_from_user_and_move_to_another_user':
            #Оператор перенаправил свой диалог к другому оператору
            data.append(detach_from_user_and_move_to_another_user( 
                        created_date, 
                        operator, 
                        operator_id, 
                        camelcase_event_name, event_new_value))
 
        if event_name == 'change_room_visibility':
            #Из обработки ботом , клиент попал к в общую очередь , вывожу в какой отдел Если нет linked_org_ids , то Берем organization_ids
            data.append(change_room_visibility(created_date, camelcase_event_name, event_new_value))

        if event_name == 'take_room':
            #Оператор взял на себя диало
            data.append(take_room(created_date, operator, operator_id, camelcase_event_name))

        if event_name == 'assign_room_to_user_by_supervisor':
            #Супервайзер назначил диалог на оператора
            data.append(assign_room_to_user_by_supervisor(
                created_date, 
                operator, 
                operator_id, 
                camelcase_event_name, 
                event_new_value))

        if event_name == 'activate_completed_room':
            #Восстановление завершенного диалога
            data.append(activate_completed_room(created_date, camelcase_event_name))

        if event_name == 'detach_from_user_and_move_to_organizations':
            #Оператор перенаправил свой диалог в другой отдел
            detach_from_user_and_move_to_organizations(
                created_date, 
                operator, 
                operator_id, 
                camelcase_event_name, 
                event_new_value)
    
        if event_name == 'complete_room':
            #Оператор завершил диалог
            data.append(create_new_room_and_complete_room(created_date, camelcase_event_name))

    return json.dumps(data, ensure_ascii=False)

def main():
    return print(data_cleaning_process())

if __name__ == '__main__':
    main()