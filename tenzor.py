import sqlite3
from datetime import date
import uuid


class PersonDbTemplates:
    def __init__(self):
        self.insert_list = []
        self.delete_list = []
        self.update_list = []

    def in_list(self, check_list, id):  # проверка на нахождение в списке для добавления эл-та
        """
        Принимает список из кортежей/списков(list) и id(str).

        Возвращает:
                False, если такой id есть последним элементом в кортеже/списке
                True, если нет
        """
        for elem in check_list:
            if id == elem[-1]:
                return False
        return True

    def remove_in_list(self, check_list, id, pozition):  # нахождение элемента, для удаления
        """
        Список из кортежей/списков(list), id(str), индекс в кортеже/списке(int)

        Возвращает кортеж, включающий в себя id на заданном индексе или False
        """
        for elem in check_list:
            if id == elem[pozition]:
                return elem
        return False

    def add(self, id=None, name=None, birthdate=None):  # возможность добавлять id снаружи
        """
        Добавление строки в таблицу базы данных.

        Необязательные параметры:
            id(str): если не задано, генерируется автоматически
            name(str): None по умолчанию
            birthdate(date): None по умолчанию
        """
        if id is None:
            id = str(uuid.uuid4())  # uuid храню в строке, в sqlite нет такого типа данных
        if self.in_list(self.delete_list, id):
            a = (id, name, birthdate)  # добавление
            self.insert_list.append(tuple(a))
            if self.remove_in_list(self.update_list, id, -1):  # удаление из update, если там есть такой id
                self.update_list.remove(self.remove_in_list(self.update_list, id, -1))

    def update(self, id, name=None, birthdate=None):
        """
        Обновление строки в таблице базы данных.

        Параметры:
            id(str): обязательный параметр
            name(str): None по умолчанию
            birthdate(date): None по умолчанию
        """
        if self.in_list(self.delete_list, id) and self.in_list(self.insert_list, id):
            self.update_list.append(tuple([name, id, birthdate, id, id]))

    def delete(self, id):  # удаление по uuid
        """Удаление строки в таблице базы данных. Принимает id(str)."""
        self.delete_list.append((id,))
        if remove_in_list(self.insert_list, id, 0):  # удаление из add, если там есть такой id
            self.insert_list.remove(self.remove_in_list(self.insert_list, id, 0))
        if self.remove_in_list(self.update_list, id, -1):  # удаление из update, если там есть такой id
            self.update_list.remove(self.remove_in_list(self.update_list, id, -1))

    def get_sql_statements(self):
        """Формирует общие запросы к базе данных на основе вызовов функций add, update, delete"""
        try:
            connect = sqlite3.connect("Test_db.db")
            cursor = connect.cursor()

            if len(self.insert_list) != 0:
                cursor.executemany("""INSERT INTO person VALUES (?, ?, ?) ON CONFLICT DO NOTHING""",
                                   [el for el in self.insert_list]).fetchall()
            if len(self.delete_list) != 0:
                cursor.executemany("""DELETE FROM Person WHERE person = ?""", self.delete_list).fetchall()
            if len(self.update_list) != 0:
                que = """UPDATE person SET (Name, BirthDate) = 
                    (SELECT coalesce(?, (SELECT Name FROM Test_db WHERE Person = ?), Null),
                            (SELECT coalesce(?, (SELECT BirthDate FROM Test_db WHERE Person = ?), Null)))"""
                que += """WHERE person = ?"""
                cursor.executemany(que, self.update_list)

            connect.commit()
            connect.close()
        except Exception as e:
            print('Произошла ошибка: ', e)

        self.insert_list.clear()  # очистка
        self.delete_list.clear()
        self.update_list.clear()
