import sqlite3
import zipfile
import json
#In total, we were 20917 rows in the database
#init var
finaldata = {}
count=0
key1="СвОКВЭД"
key2="СвОКВЭДОсн"
try:
    #create connect to db
    sqlite_connection = sqlite3.connect('./Hwork/hw1.db')
    cursor = sqlite_connection.cursor()
    #create table
    cursor.execute("DROP TABLE IF EXISTS telecom_companies;")
    cursor.execute("CREATE TABLE IF NOT EXISTS telecom_companies (id integer primary key, fname text, sname text, inn text, okved text , ogrn text, kpp text);")
    #open zip and get file names list
    with  zipfile.ZipFile('E://egrul.json.zip', 'r') as zipfl:
        files=zipfl.namelist()
        for names in files:
            #open file in zip archive
            with zipfl.open(names, 'r') as file: 
                #parse json on file  
                data=json.load(file)
                for row in data: 
                    #check that such keys are in the dictionaries
                    if key1 in row["data"].keys():
                        if key2 in row["data"][key1].keys():
                            #get КодОКВЕД
                            okved=row["data"]["СвОКВЭД"]["СвОКВЭДОсн"]["КодОКВЭД"]
                            #split string by "point" and get first vaule 
                            okved_s=okved.split('.')
                            #comparing value with number 61
                            if "61" in okved_s[:1] :
                                #clear dict finaldata
                                finaldata.clear()
                                count+=1
                                #check that such keys are in the dictionaries, if none put null value in key
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
                                #Generate formatted string of columns and named placeholders (:column_name)
                                columns = ', '.join(finaldata.keys())
                                placeholders = ':'+', :'.join(finaldata.keys())
                                #generate sql query
                                sql = "INSERT INTO telecom_companies ( %s ) VALUES ( %s )" % (columns, placeholders)
                                #execute query
                                cursor.execute(sql,finaldata)
                                print (count)
            #commit in db after parse one file
            sqlite_connection.commit()
    cursor.close()
except zipfile.BadZipfile as error:
    print("Error in  zip file :: ", error)                
except sqlite3.Error as error:
    print("Error in sqlite :: ", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()

