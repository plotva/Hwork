import requests
import sqlite3
import asyncio
import time
from aiohttp import ClientSession
import bleach
url_params = {'text':'middle python developer',
              'search_field':'name',
              'per_page':25
              }
user_agent = {'User-agent': 'Mozilla/5.0'}

def get_vacancy_id (headers,params,page):
    url=f'https://api.hh.ru/vacancies?page={page}'
    result = requests.get(url,headers = headers,params=params)
    vacancies = result.json().get('items')
    vacancy_id= []
    for i, vacancy in enumerate(vacancies):
        vacancy_id.append(vacancy['id'])        
    return vacancy_id

async def get_vacancy(id, session):
    url = f'/vacancies/{id}'  
    async with session.get(url=url) as response:
        vacancy_json = await response.json()
        return vacancy_json

async def main(ids):
    async with ClientSession('https://api.hh.ru/') as session:
        tasks = []
        data = []
        for id in ids:
            tasks.append(asyncio.create_task(get_vacancy(id, session)))
        results = await asyncio.gather(*tasks)
        for result in results:
            description=bleach.clean(result['description'], tags=[], strip=True)
            skill = ""
            for skills in result['key_skills'] :
                skill = skill + (skills['name']) + ","     
            data.append((result['employer']['name'],result['name'],description,skill[:-1]))
    cursor.executemany("insert into vacancies_api(company_name,position,job_description,key_skills) values (?,?,?,?)",data)    
    sqlite_connection.commit()


try:
    sqlite_connection = sqlite3.connect('./Hwork/hw2.db')
    cursor = sqlite_connection.cursor()
    #create table
    cursor.execute("DROP TABLE IF EXISTS vacancies_api;")
    cursor.execute("CREATE TABLE IF NOT EXISTS vacancies_api (id integer primary key, company_name text, position text, job_description text, key_skills text);")  
    for page_num in range (0,4) :
        ids = []
        ids=get_vacancy_id(user_agent,url_params,page_num)
        asyncio.run(main(ids))
        #antiban in hh
        time.sleep(0.5)
       
except sqlite3.Error as error:
    print("Error in sqlite :: ", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()

