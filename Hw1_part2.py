import sqlite3
import zipfile
import json
finaldata = {}
key1="СвОКВЭД"
key2="СвОКВЭДОсн"
count=0
try:
    sqlite_connection = sqlite3.connect('./Hwork/hw1.db')
    cursor = sqlite_connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS telecom_companies;")
    cursor.execute("CREATE TABLE IF NOT EXISTS telecom_companies (id integer primary key, fname text, sname text, inn text, okved text , ogrn text, kpp text);")
    with  zipfile.ZipFile('E://egrul.json.zip', 'r') as zipfl:
        files=zipfl.namelist()
        for names in files:
            with zipfl.open(names, 'r') as file:   
                data=json.load(file)
                for row in data: 
                    if key1 in row["data"].keys():
                        if key2 in row["data"][key1].keys():
                            okved=row["data"]["СвОКВЭД"]["СвОКВЭДОсн"]["КодОКВЭД"]
                            okved_s=okved.split('.')
                            if "61" in okved_s[:1] :
                                finaldata.clear()
                                count+=1
                                if "name" in row.keys():
                                    finaldata["sname"] = row["name"]
                                else: 
                                    finaldata["sname"]="null"
                                if "full_name" in row.keys():
                                    finaldata["fname"] = row["full_name"]
                                else: 
                                    finaldata["fname"]="null"
                                if "inn" in row.keys():
                                    finaldata["inn"] = row["inn"]
                                else: 
                                    finaldata["inn"]="null"
                                finaldata["okved"] = okved
                                if "ogrn" in row.keys():
                                    finaldata["ogrn"] = row["ogrn"]
                                else: 
                                    finaldata["ogrn"]="null"
                                if "kpp" in row.keys():
                                    finaldata["kpp"] = row["kpp"]
                                else: 
                                    finaldata["kpp"]="null"
                                columns = ', '.join(finaldata.keys())
                                placeholders = ':'+', :'.join(finaldata.keys())
                                sql = "INSERT INTO telecom_companies ( %s ) VALUES ( %s )" % (columns, placeholders)
                                cursor.execute(sql,finaldata)
                                print (count)
            sqlite_connection.commit()
    cursor.close()
except zipfile.BadZipfile as error:
    print("Error in  zip file :: ", error)                
except sqlite3.Error as error:
    print("Error in sqlite :: ", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()

