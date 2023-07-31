import asyncio
import time
from aiohttp import ClientSession
import requests
import bleach

url_params = {'text':'middle python developer',
              'search_field':'name',
              'per_page':20
              }
user_agent = {'User-agent': 'Mozilla/5.0'}

def get_vacancy_id (headers,params,page):
    url=f'https://api.hh.ru/vacancies?page={page}'
    result = requests.get(url,headers = headers,params=params)
    vacancies = result.json().get('items')
    vacancy_id= []
    for i,vacancy in enumerate(vacancies):
        vacancy_id.append(vacancy['id'])        
    return vacancy_id

def get_pages(headers,params):
    url=f'https://api.hh.ru/vacancies'
    result = requests.get(url,headers = headers,params=params)
    pages = result.json().get('pages')
    return pages




async def get_vacancy(id, session):
    url = f'/vacancies/{id}'  
    async with session.get(url=url) as response:
        vacancy_json = await response.json()
        return vacancy_json

async def main(ids,sqlite_hook,logger):
    async with ClientSession('https://api.hh.ru/') as session:
        tasks = []
        data = []
        #Создаём задания в кол-ве полученных id 
        for id in ids:
            tasks.append(asyncio.create_task(get_vacancy(id, session)))
        results = await asyncio.gather(*tasks)
        #парсим результат и пишем в базу
        for result in results:
            if 'name' in result:
                if 'description' in result:
                    description=bleach.clean(result['description'], tags=[], strip=True)
                else: 
                    description=""
                skill = ""
                if 'key_skills' in result:
                    for skills in result['key_skills'] :
                        skill = skill + (skills['name']) + ","
                else:
                    skill = ""      
                data.append((result['employer']['name'],result['name'],description,skill[:-1])) 
                sqlite_hook.insert_rows(
                        rows=data,
                        target_fields=['company_name' , 'position' , 'job_description' , 'key_skills' ],
                        table='vacancies_api',  
                                    )    
            else:
                logger.info(f"нет описания вакансии {result}")   

def get_vacancy_hh(sqlite_hook,logger):
    sqlite_hook=sqlite_hook
    pages=get_pages(user_agent,url_params)
    logger.info(f"Количество страниц с вакансиями {pages}") 
    for page_num in range (0,pages) :
        ids = []
        #Получаем список id вакансий из 25 штук, page_num - номер страницы с вакансиями
        ids=get_vacancy_id(user_agent,url_params,page_num)
        logger.info(f"id вакансий {ids}") 
        asyncio.run(main(ids,sqlite_hook,logger))
        #antiban in hh
        time.sleep(1)