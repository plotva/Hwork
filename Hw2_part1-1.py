import requests
import sqlite3
from bs4 import BeautifulSoup
import time
import asyncio
page_c = []
user_agent = {'User-agent': 'Mozilla/5.0'}

async def get_vacancy_by_id(id, user_agent):
    url = f'https://hh.ru/vacancy/{id}'  
    vacancy = requests.get(url,headers = user_agent)
    soup = BeautifulSoup(vacancy.content.decode(), 'lxml')
    return soup

async def main(ids):
    tasks = []
    data = []
    for id in ids:
        tasks.append(asyncio.create_task(get_vacancy_by_id(id, user_agent)))
    results = await asyncio.gather(*tasks)
    for soup in results:
        key_skills = ""
        flag_archive = False
        company_name = soup.find('a', attrs={'data-qa': 'vacancy-company-name'})
        name = soup.find('h1')
        if not company_name:
         company_name = soup.find('div', attrs={'data-qa': 'vacancy-company__details'})
         if not company_name :
            flag_archive = True
        job_description = soup.find('div', attrs={'data-qa': 'vacancy-description'})
        skills = soup.find_all('div', attrs={'data-qa': 'bloko-tag bloko-tag_inline skills-element'})
        for skil in skills :
            key_skills=key_skills  + skil.text + ","
        if not flag_archive :
            data.append((company_name.text,name.text,job_description.text,key_skills[:-1]))
        else : pass          
    cursor.executemany("insert into vacancies(company_name,position,job_description,key_skills) values (?,?,?,?)",data)    
    sqlite_connection.commit()

try:
    sqlite_connection = sqlite3.connect('./Hwork/hw2.db')
    cursor = sqlite_connection.cursor()
    #create table
    cursor.execute("DROP TABLE IF EXISTS vacancies;")
    cursor.execute("CREATE TABLE IF NOT EXISTS vacancies (id integer primary key, company_name text, position text, job_description text, key_skills text);")
    #url_main = ' https://hh.ru/search/vacancy?text=middle python developer'
    #result = requests.get(url_main, headers=user_agent)
    #soup = BeautifulSoup(result.content.decode(), 'lxml')
    #papers = soup.find_all('a', attrs={'data-qa': 'pager-page'})
    #for paper in papers :
    #    page_c.append(paper.text)
    #for index in range (0, int(page_c[-1])) :
    for index in range (0, 5) :
        url=f"https://hh.ru//search/vacancy?text=middle python developer&page={index}&salary=&ored_clusters=true&search_field=name&search_field=name"
        ids = []
        result = requests.get(url, headers=user_agent)
        result.content.decode()
        soup = BeautifulSoup(result.content.decode(), 'lxml')
        vac = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
        for id_vac in vac:
            ids.append(id_vac.attrs.get('href').split('?')[0].split('/')[-1])
        print (ids)
        asyncio.run(main(ids))
        #antiban in hh
        time.sleep(0.5)     
except sqlite3.Error as error:
    print("Error in sqlite :: ", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()

