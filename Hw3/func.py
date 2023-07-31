def create_tables (sqlite_hook,logger):
    #sqlite_hook.run("DROP TABLE IF EXISTS telecom_companies;")
    sqlite_hook.run("CREATE TABLE IF NOT EXISTS telecom_companies (id integer primary key, fname text, sname text, inn text, okved text , ogrn text, kpp text);")
    sqlite_hook.run("DROP TABLE IF EXISTS vacancies_api;")
    sqlite_hook.run("CREATE TABLE IF NOT EXISTS vacancies_api (id integer primary key, company_name text, position text, job_description text, key_skills text);")
    logger.info("Таблицы создались")  