import sqlite3

try:
    sqlite_connection = sqlite3.connect('hw1.db')
    cursor = sqlite_connection.cursor()
    print("База данных создана и успешно подключена к SQLite")

    query = "create table okved (code text primary key, parent_code text, section text, name text , comment text);"
    cursor.execute(query)

    cursor.close()

except sqlite3.Error as error:
    print("Ошибка при подключении к sqlite", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()
      