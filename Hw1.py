import sqlite3
import json
import zipfile
with zipfile.ZipFile('./Hwork/okved_2.json.zip', 'r') as zipobj:
    zipobj.extractall('./Hwork/')
try:
    sqlite_connection = sqlite3.connect('./Hwork/hw1.db')
    cursor = sqlite_connection.cursor()
    query = "create table if not exists okved (code text primary key, parent_code text, section text, name text , comment text);"
    cursor.execute(query)
    cursor.close()
   # data = []
    with open('./Hwork/okved_2.json', 'r', encoding="UTF8") as f:
        #file_content=f.read()
        data_json=json.loads(f)
    #print(data_json)
    #with open('./Hwork/okved_2.json', 'r', encoding="UTF8") as f:
    #    data_json=json.load(f)
    print(type(data_json))
    print(data_json)
except sqlite3.Error as error:
    print("Ошибка при подключении к sqlite", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()
      