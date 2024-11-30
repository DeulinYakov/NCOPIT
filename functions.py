# Для перевода времени в строку
import datetime
# Для даты в расписание
import re
# Для проверки обновлений
import hashlib


# Функция открывает ячейку [0,11] и из 'РАСПИСАНИЕ ЗАНЯТИЙ с 25.03 по 30.03.2024 г.' получает дату с помощью регулярного выражения
def extract_dates(text):
    # Используем регулярное выражение для поиска дат в формате "dd.mm"
    pattern = r'\b\d{2}\.\d{2}\b'
    # Находим все совпадения в тексте
    matches = re.findall(pattern, text)

    # Возвращаем первую и последнюю дату плюс год
    return matches[0] + '.2024', matches[-1] + '.2024'


# Функция очистки ненужных строк !только после функции получения даты!
def deleting_lines(data):
    # удаляем первые ненужные строки
    data = data.drop([0, 1, 2, 3, 5])
    # удаляем посдедние ненужные столбцы после столбца 73
    # all_colums = list(data.columns.values)

    # last_index = all_colums.index(64)
    # bad_colums = all_colums[last_index+1:]

    # data = data.drop(bad_colums, axis = 1)
    return (data)


# Функция получает датафрейм и выдаёт список групп
def getting_groups(data):
    lst = data.loc[4].values.tolist()
    del lst[:3]  # Удаляем первые три элемента из списка
    groups = []
    for index, elem in enumerate(lst):
        if index % 2 == 0:
            groups.append(elem)
    groups.pop()
    return groups


# напишем функцию которая находит расписание нужной группы
def get_schedule(ds, group, found_line_name):
    # найдем индекс столбцы с номером этой группы
    idx = ds.loc[found_line_name].tolist().index(group)

    # найдем названия столбцов по найденному индексу
    columns_names = [0, 1, 2, ds.columns[idx], ds.columns[idx + 1]]

    # вернем отфильтрованный датафрейм
    return ds.loc[:, columns_names]


# Функция перебирает список и преобразует времени из datetime в формат %H:%M строка
def clean_group(item):
    return item.strftime('%H:%M') if isinstance(item, datetime.time) else item


# Функция перебирает словарь и добавлеяет к time end
def add_end_time(items):
    from datetime import datetime, timedelta
    time_format = "%H:%M"
    for item in items:
        if item.get("day") == "Понедельник":
            for elem in item["classes"]:
                if elem.get("lesson") == "кл ч":
                    start_time_str = elem["time"]["start"].strip()  # Удаление пробелов
                    start_datetime = datetime.strptime(start_time_str, time_format)
                    end_datetime = start_datetime + timedelta(minutes=45)
                    end_time = end_datetime.strftime(time_format)
                    elem["time"]["end"] = end_time
                else:
                    start_time_str = elem["time"]["start"].strip()  # Удаление пробелов
                    start_datetime = datetime.strptime(start_time_str, time_format)
                    end_datetime = start_datetime + timedelta(hours=1, minutes=30)
                    end_time = end_datetime.strftime(time_format)
                    elem["time"]["end"] = end_time
        elif item.get("day") == 'суббота':
            for elem in item["classes"]:
                start_time_str = elem["time"]["start"].strip()  # Удаление пробелов
                start_datetime = datetime.strptime(start_time_str, time_format)
                end_datetime = start_datetime + timedelta(hours=1, minutes=30)
                end_time = end_datetime.strftime(time_format)
                elem["time"]["end"] = end_time
        else:
            for elem in item["classes"]:
                start_time_str = elem["time"]["start"].strip()  # Удаление пробелов
                start_datetime = datetime.strptime(start_time_str, time_format)
                end_datetime = start_datetime + timedelta(hours=1, minutes=35)
                end_time = end_datetime.strftime(time_format)
                elem["time"]["end"] = end_time

    return items


def day_to_date(weekday, start_date_str):
    from datetime import datetime, timedelta
    # Преобразование строки в объект datetime
    initial_date = datetime.strptime(start_date_str, "%d.%m.%Y")

    # Словарь, где ключ — это день недели, а значение — количество дней, которое нужно добавить
    days_to_add = {
        "Понедельник": 0,
        "Вторник": 1,
        "Среда": 2,
        "Четверг": 3,
        "Пятница": 4,
        "Суббота": 5,
        "Воскресенье": 6
    }

    # Логика изменения даты

    if weekday in days_to_add:
        # Добавляем нужное количество дней к исходной дате
        new_date = initial_date + timedelta(days=days_to_add[weekday])
        return new_date.strftime('%d.%m.%Y')
    else:
        return False


# Место проведения
def get_place_by_room(room):
    room = str(room)
    if "общ" in room:
        return "dormitory"
    elif "тр" in room:
        return "gym"
    elif room in ["дист", "онлайн", "сферум"]:
        return "distance"
    elif "сп" in room:
        return "sports-hall"
    elif "конф" in room:
        return "conf-hall"
    elif room == "цех":
        return "workshop"
    elif "акт" in room:
        return "assembly-hall"
    else:
        return "college"


# Перевод в хеш
def calculate_content_hash(content):
    # Создаем объект хеша SHA-256
    hash_object = hashlib.sha256()

    # Обновляем хеш-объект с содержимым файла
    hash_object.update(content)

    # Получаем хеш в виде строки
    content_hash = hash_object.hexdigest()
    return content_hash
