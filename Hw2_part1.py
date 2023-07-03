import requests
import sqlite3
from bs4 import BeautifulSoup
import time
page_c = []
count = 0
try:
    sqlite_connection = sqlite3.connect('./Hwork/hw2.db')
    cursor = sqlite_connection.cursor()
    #create table
    cursor.execute("DROP TABLE IF EXISTS vacancies;")
    cursor.execute("CREATE TABLE IF NOT EXISTS vacancies (id integer primary key, company_name text, position text, job_description text, key_skills text);")
    url_main = ' https://hh.ru/search/vacancy?text=middle+python+developer&salary=&ored_clusters=true&search_field=name&search_field=description'
    user_agent = {'User-agent': 'Mozilla/5.0'}
    result = requests.get(url_main, headers=user_agent)
    #print(result.status_code)
    result.content.decode()
    soup = BeautifulSoup(result.content.decode(), 'lxml')
    papers = soup.find_all('a', attrs={'data-qa': 'pager-page'})
    for paper in papers :
        page_c.append(paper.text)
    for index in range (1, int(page_c[-1]) + 1 ) :
        url=f"https://hh.ru//search/vacancy?text=middle+python+developer&amp;salary=&amp;ored_clusters=true&amp;search_field=name&amp;search_field=description&amp;page=&amp;hhtmFrom=vacancy_search_list&page={index}"
        data = []
        print (url)
        result = requests.get(url, headers=user_agent)
        #print(result.status_code)
        result.content.decode()
        soup = BeautifulSoup(result.content.decode(), 'lxml')
        names = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
        for name in names:
            key_skills = ""
            nn = name.text.lower()
            if "python" in nn :
              #print(name.text, name.attrs.get('href'))
                result_v = requests.get(name.attrs.get('href'), headers=user_agent) 
                result_v.content.decode()
                soup_v = BeautifulSoup(result_v.content.decode(), 'lxml') 
                company_name = soup_v.find('a', attrs={'data-qa': 'vacancy-company-name'})
                if not company_name:
                    company_name = soup_v.find('div', attrs={'data-qa': 'vacancy-company__details'})
                job_description = soup_v.find('div', attrs={'data-qa': 'vacancy-description'})
                skills = soup_v.find_all('div', attrs={'data-qa': 'bloko-tag bloko-tag_inline skills-element'})
                for skil in skills :
                    key_skills=key_skills  + skil.text + ", "
                #print ( company_name )
                if count < 100 :
                    data.append((company_name.text,name.text,job_description.text,key_skills))
                    count += 1
                else : pass
                time.sleep(1)
            #print(data)
        cursor.executemany("insert into vacancies(company_name,position,job_description,key_skills) values (?,?,?,?)",data)    
        sqlite_connection.commit()
       
except sqlite3.Error as error:
    print("Error in sqlite :: ", error)
finally:
    if (sqlite_connection):
        sqlite_connection.close()

