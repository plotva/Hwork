import sqlite3
import json
import zipfile
#Part 1
#Unpack zip
with zipfile.ZipFile('./Hwork/okved_2.json.zip', 'r') as zipobj:
    zipobj.extractall('./Hwork/')
#Load json file
with open('./Hwork/okved_2.json', 'r', encoding="UTF8") as f:
        data_json=json.load(f)     
#try to connect Sqlite DB
try:
    #Create DB
    sqlite_connection = sqlite3.connect('./Hwork/hw1.db')
    cursor = sqlite_connection.cursor()
    #Create table okved (drop if exists)
    cursor.execute("DROP TABLE IF EXISTS okved;")
    cursor.execute("CREATE TABLE IF NOT EXISTS okved (code text primary key, parent_code text, section text, name text , comment text);")
    #Parse json data and generate sql 
    for row in data_json:
            #Generate formatted string of columns
            columns = ', '.join(row.keys())
            #Generate formatted named placeholders (:column_name) 
            placeholders = ':'+', :'.join(row.keys())
            #Generate sql query for insert data
            sql = "INSERT INTO okved ( %s ) VALUES ( %s )" % (columns, placeholders)
            #Execute query
            cursor.execute(sql,row)
    #Commit in DB        
    sqlite_connection.commit()
    cursor.close()
except sqlite3.Error as error:
    print("Error in sqlite :: ", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()