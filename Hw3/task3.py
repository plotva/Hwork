import sqlite3
import re
from collections import Counter
#Берём названия компаний и ключевые навыки из вакансий
def get_all_vac_from_db(): 
    sqlite_connection = sqlite3.connect('/tmp/hw3.db')
    cursor = sqlite_connection.cursor() 
    cursor.execute("select company_name,key_skills from  vacancies_api;")
    return cursor.fetchall() 
#Берем названия компаний из базы ЕГРЮЛ
def get_all_company_from_db(): 
    sqlite_connection = sqlite3.connect('/tmp/hw3.db')
    cursor = sqlite_connection.cursor() 
    cursor.execute("select fname from telecom_companies;")
    return cursor.fetchall()

def get_top_skills(logger):
    vacancy=get_all_vac_from_db()
    company= get_all_company_from_db()
    trim_comp_name=[]
    #Парим названия компаний из ЕГРЮЛ, убирая лишние символы и названия правовых форм (ООО, ПАО и т.д.)
    for cname in company:
        lcname=str(cname).lower()
        result = re.findall(r'[^"]+', lcname)
        trim_comp_name.append("".join(result[1:2]))
    #Сравниваем название компании из вакансии и из базы ЕГРЮЛ, при совпадении добавляем в list список ключевых навыков из вакансии
    skls=[]
    for vac,skill in vacancy:
        lcomp_v=str(vac).lower()
        lskil_v=str(skill).lower() 
        if lcomp_v in trim_comp_name:
            skls_sep=lskil_v.split(sep=',')
            for skl in skls_sep:
                skls.append(skl)
    #Считаем количество вхождений каждого навыка в общий список
    top_skills=dict(Counter(skls))
    #Сортируем по убыванию
    sorted_dict = {}
    sorted_keys = sorted(top_skills, key=top_skills.get,reverse=True)  
    for w in sorted_keys:
        sorted_dict[w] = top_skills[w] 
    #Собираем строку из топ 10 названий ключевых навыков, которые требуются в вакансиях
    top=" ".join(str(x) for x in list(sorted_dict.keys())[:10])
    logger.info(f"Топ 10 навыков требующихся в телеком компаниях: {top}")  
