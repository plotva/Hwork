import zipfile
import json
def parse_and_insert_egrl(fl,sqlite_hook):
    sqlite_hook=sqlite_hook
    finaldata = {}
    key1="СвОКВЭД"
    key2="СвОКВЭДОсн"
    ##sqlite_hook.run("DROP TABLE IF EXISTS telecom_companies;")
    ##sqlite_hook.run("CREATE TABLE IF NOT EXISTS telecom_companies (id integer primary key, fname text, sname text, inn text, okved text , ogrn text, kpp text);")
    with  zipfile.ZipFile(fl, 'r') as zipfl:
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
                                tupl=finaldata.values()
                                tupl=tuple(tupl) 
                                #logger.info(finaldata.values())
                                sqlite_hook.insert_rows(
                                    rows=[tupl],
                                    target_fields=['fname' , 'sname' , 'inn' , 'okved'  , 'ogrn' , 'kpp' ],
                                    table='telecom_companies',  
                                )



