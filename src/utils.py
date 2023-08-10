import requests
import json
import psycopg2


def get_vacancies(employer_id: int) -> list[dict[str]]:
    """Получение вакансий с hh.ru по employer_id"""
    url = 'https://api.hh.ru/vacancies'
    params = {'employer_id': employer_id, 'per_page': 100}

    req = requests.get(url, params=params)
    data = req.content.decode()
    req.close()
    return json.loads(data)["items"]


def create_database(database_name: str, params: dict) -> None:
    """Создание базы данных для сохраненения данных о вакансиях"""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE employers (
                    employer_id INTEGER PRIMARY KEY,
                    employer_name TEXT NOT NULL
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE vacancies (
                    vacancy_id INTEGER PRIMARY KEY,
                    vacancy_name TEXT NOT NULL,
                    vacancy_url TEXT,
                    area_name VARCHAR(100),
                    salary_from INTEGER,
                    salary_to INTEGER,
                    employer_id INTEGER,
                    requirement TEXT
                );
                ALTER TABLE vacancies 
                ADD CONSTRAINT fk_vacancies_employers FOREIGN KEY(employer_id) REFERENCES employers(employer_id)
            """)

    conn.commit()
    conn.close()


def save_data_to_database(employers_dict: dict, data: list, database_name: str, params: dict) -> None:
    """Сохранение данных о вакансиях в БД"""
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for k, v in employers_dict.items():
            cur.execute(
                """
                INSERT INTO employers (employer_id, employer_name)
                VALUES (%s, %s)
                """,
                (k, v)
            )

    with conn.cursor() as cur:
        for vacancy in data:
            vacancy_id = vacancy["id"]
            vacancy_name = vacancy["name"]
            vacancy_url = 'https://hh.ru/vacancy/' + vacancy["id"]
            area_name = vacancy["area"]["name"]
            if vacancy["salary"] is not None:
                salary_from = vacancy["salary"]["from"]
                salary_to = vacancy["salary"]["to"]
            else:
                salary_from = None
                salary_to = None
            employer_id = vacancy["employer"]["id"]
            requirement = vacancy["snippet"]["requirement"]

            cur.execute(
                """
                INSERT INTO vacancies (vacancy_id, vacancy_name, vacancy_url, area_name,
                salary_from, salary_to, employer_id, requirement)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (vacancy_id, vacancy_name, vacancy_url, area_name,
                 salary_from, salary_to, employer_id, requirement)
            )

    conn.commit()
    conn.close()
