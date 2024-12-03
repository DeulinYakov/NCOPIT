from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import asyncio
# Для парсинга
import numpy as np
import pandas as pd
# Для проверки обновлений
import requests
import hashlib
# Для работы с json
import orjson
import os

# Внешние функции
import functions as fn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],                     # Список разрешённых источников
    allow_credentials=True,                  # Разрешить передачу cookies и авторизационных заголовков
    allow_methods=["*"],                     # Разрешить все методы HTTP (GET, POST, PUT, DELETE и т.д.)
    allow_headers=["*"],                     # Разрешить все заголовки
)

# Переменная для json
groups_data = {"groups": []}
# Урл расписания
url_schedule = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTEBDntWzVNkDDXL6ebutiOIhOmD70jmLydOGepsNdHkA323CT1wf4-jFKMgCB5uQ/pub?output=xlsx'
# Действующий файл
all_data = pd.ExcelFile(url_schedule)


# Тестовый файл
# all_data = pd.ExcelFile('https://docs.google.com/spreadsheets/d/e/2PACX-1vShcAJbZ3TzjO8x2JGP9_zZ_32sr8etktW2OjczyaiaOjop9-Vlt9tVtqLw6kdelg/pub?output=xlsx')


# Асинхронная функция обработки GET-запроса
@app.get("/")
async def get_async_data():
    return orjson.dumps(groups_data).decode("utf-8")


# Асинхронная функция обработки GET-запроса
@app.get("/api")
async def get_async():
    return {'message': 'Hello World'}


previous_hash = None


# Асинхронная задача, которая запускается при старте и каждые 60 секунд
async def periodic_task():
    global previous_hash
    global groups_data
    while True:
        # тестово (костыль)
        groups_data = {"groups": []}
        # Получаем файл exls по ссылке
        response = requests.get(url_schedule)
        # Сохраняем файл в объекте excel_data
        excel_data = response.content
        # Вызываем функцию calculate_content_hash() с содержимым файла excel_data
        # Вычисляем хэш содержимого
        current_hash = fn.calculate_content_hash(excel_data)

        # Сравниваем текущий хэш с предыдущим
        if previous_hash is None or current_hash != previous_hash:
            # print("Файл проверен впервые. Хэш:", current_hash)
            # Получаем весь файл без конкретного листа
            # Узнаём какие листы есть
            # Получаем список названий всех листов
            sheet_names = all_data.sheet_names

            # Теперь можем пройти по каждому листу
            for sheet_name in sheet_names:
                # открываем листы по очереди
                data = pd.read_excel(all_data, sheet_name=sheet_name, header=None).fillna('Нет')
                # получаем дату
                start_date_str, end_date_str = fn.extract_dates(data.iat[0, 11])

                # определяем какая это неделя
                # Получаем текущую дату
                current_date = fn.datetime.datetime.now()
                # Получаем номер недели в текущем году
                current_week = current_date.isocalendar()[1]
                # Определяем даты начала и конца текущей недели
                start_of_week = current_date - fn.datetime.timedelta(days=current_date.weekday())
                end_of_week = start_of_week + fn.datetime.timedelta(days=6)
                # Определяем даты начала и конца следующей недели
                start_of_next_week = end_of_week + fn.datetime.timedelta(days=1)
                end_of_next_week = start_of_next_week + fn.datetime.timedelta(days=5)
                # Определяем даты начала и конца предыдущей недели
                start_of_prev_week = start_of_week - fn.datetime.timedelta(days=7)
                end_of_prev_week = start_of_week - fn.datetime.timedelta(days=1)

                end_date = fn.datetime.datetime.strptime(start_date_str, "%d.%m.%Y")
                start_date = fn.datetime.datetime.strptime(end_date_str, "%d.%m.%Y")
                data = fn.deleting_lines(data)
                # Создаем пустой список для хранения информации о том, какие файлы были обработаны
                processed_files = []
                # Сравниваем ваши данные с датами начала и конца следующей, текущей и предыдущей недели
                if start_of_prev_week <= start_date <= end_of_prev_week:
                    # Если неделя прошлая
                    # print("Неделя прошлая")
                    # получаем список групп
                    groups = fn.getting_groups(data)
                    # парсим и преобразуем в json
                    # Проходим по каждодой группе, парсим, чистим, преобразуем в словарь и в json
                    for group in groups:
                        # Создаем данные для записи в файл
                        # парсим (датафрейм, группа, строка где указаны группы)
                        rasp = fn.get_schedule(data, group, 4).values.tolist()

                        # преобразуем непонятную дату в строковое значение
                        dict_rasp_group = [[fn.clean_group(item) for item in sublist] for sublist in rasp]
                        schedule = []
                        # перебираем список dict_rasp_group и раскидываем по словарю
                        for item in dict_rasp_group:
                            # тут мы фильтруем дату
                            if item[0] != "Нет" and item[1:4] == ["Нет", "Нет", "Нет"]:
                                date = item[0]
                            elif item[0:4] == ["Нет", "Нет", "Нет", "Нет"]:
                                date = False
                            # тут определяем день недели
                            elif item[0] != "Нет":
                                # с первой пары или нет
                                if item[3] != "Нет" or item[4] != "Нет":
                                    date = date if date else fn.day_to_date(item[0], start_date_str)
                                    day_schedule = {"day": item[0], "date": date, "classes": [
                                        {"lesson": item[1], "time": {"start": item[2]}, "course": item[3],
                                         "rooms": [{"room": item[4], "place": fn.get_place_by_room(item[4])}]}]}
                                else:
                                    date = date if date else fn.day_to_date(item[0], start_date_str)
                                    day_schedule = {"day": item[0], "date": date, "classes": []}
                                schedule.append(day_schedule)
                            else:
                                lesson = item[1]
                                time_start = item[2]
                                course = item[3]
                                room = item[4]
                                place = fn.get_place_by_room(item[4])
                                # Ну и пары
                                if lesson != "Нет" and time_start != "Нет" and room != "Нет":
                                    class_info = {
                                        "lesson": lesson,
                                        "time": {"start": time_start},
                                        "course": course,
                                        "rooms": [{"room": room, "place": place}]
                                    }
                                    schedule[-1]["classes"].append(class_info)
                        # К времени добавляем end, можно было сделать позже, но там вложеность тяжёлая
                        schedule = fn.add_end_time(schedule)

                        # Записываем в память
                        group_found = False
                        for group_info in groups_data["groups"]:
                            if group_info["group"] == group:
                                # Обновляем данные группы
                                group_info["prevWeek"] = {"from": start_date_str, "to": end_date_str, "days": schedule}
                                group_found = True
                                # print(f"Данные для группы '{group}' обновлены в памяти")
                                break

                        # Если группа не найдена, добавляем новую группу с расписанием
                        if not group_found:
                            new_group_info = {
                                "group": group,
                                "prevWeek": {"from": start_date_str, "to": end_date_str, "days": schedule},
                                "currentWeek": "",
                                "nextWeek": ""
                            }
                            groups_data["groups"].append(new_group_info)
                            # print(f"Группа '{group}' добавлена в память")

                        # Удаляем временные переменные, если нужно
                        del dict_rasp_group
                elif start_of_week <= start_date <= end_of_week:
                    # Если неделя текущая
                    # print("Неделя текущая")
                    # получаем список групп
                    groups = fn.getting_groups(data)
                    # парсим и преобразуем в json
                    # Проходим по каждодой группе, парсим, чистим, преобразуем в словарь и в json
                    for group in groups:
                        # Создаем данные для записи в файл
                        # парсим (датафрейм, группа, строка где указаны группы)
                        rasp = fn.get_schedule(data, group, 4).values.tolist()

                        # преобразуем непонятную дату в строковое значение
                        dict_rasp_group = [[fn.clean_group(item) for item in sublist] for sublist in rasp]
                        schedule = []
                        # перебираем список dict_rasp_group и раскидываем по словарю
                        for item in dict_rasp_group:
                            # тут мы фильтруем дату
                            if item[0] != "Нет" and item[1:4] == ["Нет", "Нет", "Нет"]:
                                date = item[0]
                            elif item[0:4] == ["Нет", "Нет", "Нет", "Нет"]:
                                date = False
                            # тут определяем день недели
                            elif item[0] != "Нет":
                                # с первой пары или нет
                                if item[3] != "Нет" or item[4] != "Нет":
                                    date = date if date else fn.day_to_date(item[0], start_date_str)
                                    day_schedule = {"day": item[0], "date": date, "classes": [
                                        {"lesson": item[1], "time": {"start": item[2]}, "course": item[3],
                                         "rooms": [{"room": item[4], "place": fn.get_place_by_room(item[4])}]}]}
                                else:
                                    date = date if date else fn.day_to_date(item[0], start_date_str)
                                    day_schedule = {"day": item[0], "date": date, "classes": []}
                                schedule.append(day_schedule)
                            else:
                                lesson = item[1]
                                time_start = item[2]
                                course = item[3]
                                room = item[4]
                                place = fn.get_place_by_room(item[4])
                                # Ну и пары
                                if lesson != "Нет" and time_start != "Нет" and room != "Нет":
                                    class_info = {
                                        "lesson": lesson,
                                        "time": {"start": time_start},
                                        "course": course,
                                        "rooms": [{"room": room, "place": place}]
                                    }
                                    schedule[-1]["classes"].append(class_info)
                        # К времени добавляем end, можно было сделать позже, но там вложеность тяжёлая
                        schedule = fn.add_end_time(schedule)

                        # Записываем в память
                        group_found = False
                        for group_info in groups_data["groups"]:
                            if group_info["group"] == group:
                                # Обновляем данные группы
                                group_info["currentWeek"] = {"from": start_date_str, "to": end_date_str,
                                                             "days": schedule}
                                group_found = True
                                # print(f"Данные для группы '{group}' обновлены в памяти")
                                break

                        # Если группа не найдена, добавляем новую группу с расписанием
                        if not group_found:
                            new_group_info = {
                                "group": group,
                                "prevWeek": "",
                                "currentWeek": {"from": start_date_str, "to": end_date_str, "days": schedule},
                                "nextWeek": ""
                            }
                            groups_data["groups"].append(new_group_info)
                            # print(f"Группа '{group}' добавлена в память")

                        # Удаляем временные переменные, если нужно
                        del dict_rasp_group
                elif start_of_next_week <= start_date <= end_of_next_week:
                    # Если неделя следующая
                    # print("следующая неделя")
                    # получаем список групп
                    groups = fn.getting_groups(data)
                    # парсим и преобразуем в json
                    # Проходим по каждодой группе, парсим, чистим, преобразуем в словарь и в json
                    for group in groups:
                        # Создаем данные для записи в файл
                        # парсим (датафрейм, группа, строка где указаны группы)
                        rasp = fn.get_schedule(data, group, 4).values.tolist()

                        # преобразуем непонятную дату в строковое значение
                        dict_rasp_group = [[fn.clean_group(item) for item in sublist] for sublist in rasp]
                        schedule = []
                        # перебираем список dict_rasp_group и раскидываем по словарю
                        for item in dict_rasp_group:
                            # тут мы фильтруем дату
                            if item[0] != "Нет" and item[1:4] == ["Нет", "Нет", "Нет"]:
                                date = item[0]
                            elif item[0:4] == ["Нет", "Нет", "Нет", "Нет"]:
                                date = False
                            # тут определяем день недели
                            elif item[0] != "Нет":
                                # с первой пары или нет
                                if item[3] != "Нет" or item[4] != "Нет":
                                    date = date if date else fn.day_to_date(item[0], start_date_str)
                                    day_schedule = {"day": item[0], "date": date, "classes": [
                                        {"lesson": item[1], "time": {"start": item[2]}, "course": item[3],
                                         "rooms": [{"room": item[4], "place": fn.get_place_by_room(item[4])}]}]}
                                else:
                                    date = date if date else fn.day_to_date(item[0], start_date_str)
                                    day_schedule = {"day": item[0], "date": date, "classes": []}
                                schedule.append(day_schedule)
                            else:
                                lesson = item[1]
                                time_start = item[2]
                                course = item[3]
                                room = item[4]
                                place = fn.get_place_by_room(item[4])
                                # Ну и пары
                                if lesson != "Нет" and time_start != "Нет" and room != "Нет":
                                    class_info = {
                                        "lesson": lesson,
                                        "time": {"start": time_start},
                                        "course": course,
                                        "rooms": [{"room": room, "place": place}]
                                    }
                                    schedule[-1]["classes"].append(class_info)
                        # К времени добавляем end, можно было сделать позже, но там вложеность тяжёлая
                        schedule = fn.add_end_time(schedule)

                        # Записываем в память
                        group_found = False
                        for group_info in groups_data["groups"]:
                            if group_info["group"] == group:
                                # Обновляем данные группы
                                group_info["nextWeek"] = {"from": start_date_str, "to": end_date_str, "days": schedule}
                                group_found = True
                                # print(f"Данные для группы '{group}' обновлены в памяти")
                                break

                        # Если группа не найдена, добавляем новую группу с расписанием
                        if not group_found:
                            new_group_info = {
                                "group": group,
                                "prevWeek": "",
                                "currentWeek": "",
                                "nextWeek": {"from": start_date_str, "to": end_date_str, "days": schedule}
                            }
                            groups_data["groups"].append(new_group_info)
                            # print(f"Группа '{group}' добавлена в память")

                        # Удаляем временные переменные, если нужно
                        del dict_rasp_group
                else:
                    # print("Это ни текущая, ни следующая, ни предыдущая неделе.")
                    pass
        else:
            # print("Файл не изменился.")
            pass

        # Обновляем предыдущий хэш
        previous_hash = current_hash
        await asyncio.sleep(1800)  # Ожидание 1800


# Запуск фоновой задачи при старте приложения
@app.on_event("startup")
async def startup_event():
    # Запускаем задачу в фоне
    asyncio.create_task(periodic_task())
