import sqlite3
from datetime import date
import uuid


class PersonDbTemplates:
    def __init__(self):
        self.insert_list = []
        self.delete_list = []
        self.update_list = []

    def add(self, id=None, name=None, birthdate=None):  # возможность добавлять id снаружи
        if id is None:
            id = str(uuid.uuid4())  # uuid храню в строке, sqlite нет такого типа данных
        f = 0
        for elem in self.delete_list:  # проверка на нахождение в списке удаления
            if id == elem[0]:
                f = 1
                break
        if not f:
            a = (id, name, birthdate)  # добавление
            self.insert_list.append(tuple(a))
            for elem in self.update_list:  # удаление из update, если он там
                if id == elem[-1]:
                    self.update_list.remove(elem)

    def update(self, id, name=None, birthdate=None):
        f = 0
        for elem in self.delete_list:  # проверка на нахождение в удалении
            if id == elem[0]:
                f = 1
                break
        for elem in self.insert_list:  # проверка на нахождение в добавлении
            if id == elem[0]:
                f = 1
                break
        if not f:
            a = [name, id, birthdate, id, id]  # добавление
            self.update_list.append(tuple(a))

    def delete(self, id):  # удаление по uuid
        self.delete_list.append((id,))
        for elem in self.insert_list:  # убираем из add если он там
            if id == elem[0]:
                self.insert_list.remove(elem)
        for elem in self.update_list:  # убираем из update усли он там
            if id == elem[-1]:
                self.update_list.remove(elem)

    def get_sql_statements(self):
        try:
            con = sqlite3.connect("Test_db.db")
            cur = con.cursor()
            if len(self.insert_list) != 0:  # add   returning
                cur.executemany("""INSERT INTO person VALUES (?, ?, ?) ON CONFLICT DO NOTHING""",
                                [el for el in self.insert_list]).fetchall()
                if len(self.delete_list) != 0:
                    cur.executemany("""DELETE FROM Person WHERE person = ?""", self.delete_list).fetchall()
                    if len(self.update_list) != 0:  # upd  returning?
                        que = """UPDATE person SET (Name, BirthDate) = 
                    (SELECT coalesce(?, (SELECT Name FROM Test_db WHERE Person = ?), Null),
                            (SELECT coalesce(?, (SELECT BirthDate FROM Test_db WHERE Person = ?), Null)))"""
                        que += """WHERE person = ?"""
                        cur.executemany(que, self.update_list)
            con.commit()
            con.close()
        except Exception as e:
            print('Произошла ошибка: ', e)

        self.insert_list.clear()  # очистка
        self.delete_list.clear()
        self.update_list.clear()