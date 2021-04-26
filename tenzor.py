import sqlite3
from datetime import date
import uuid


class PersonDbTemplates:
    def __init__(self):
        self.insert_list = []
        self.delete_list = []
        self.update_list = []

    def in_list(self, check_list, id):
        for elem in check_list:  # проверка на нахождение в списке для добавления эл-та
            if id == elem[-1]:
                return False
        return True

    def remove_in_list(self, check_list, id, pozition):
        for elem in check_list:  # удаление из update, если он там
            if id == elem[pozition]:
                return elem

    def add(self, id=None, name=None, birthdate=None):  # возможность добавлять id снаружи
        if id is None:
            id = str(uuid.uuid4())  # uuid храню в строке, sqlite нет такого типа данных
        if self.in_list(self.delete_list, id):
            a = (id, name, birthdate)  # добавление
            self.insert_list.append(tuple(a))
            self.update_list.remove(self.remove_in_list(self.update_list, id, -1))

    def update(self, id, name=None, birthdate=None):
        if self.in_list(self.delete_list, id) and self.in_list(self.insert_list, id):
            self.update_list.append(tuple([name, id, birthdate, id, id]))

    def delete(self, id):  # удаление по uuid
        self.delete_list.append((id,))
        self.insert_list.remove(self.remove_in_list(self.insert_list, id, 0))
        self.update_list.remove(self.remove_in_list(self.update_list, id, -1))

    def get_sql_statements(self):
        try:
            connect = sqlite3.connect("Test_db.db")
            cur = connect.cursor()

            if len(self.insert_list) != 0:  # add   returning
                cursor.executemany("""INSERT INTO person VALUES (?, ?, ?) ON CONFLICT DO NOTHING""",
                                   [el for el in self.insert_list]).fetchall()
            if len(self.delete_list) != 0:
                cursor.executemany("""DELETE FROM Person WHERE person = ?""", self.delete_list).fetchall()
            if len(self.update_list) != 0:  # upd  returning?
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