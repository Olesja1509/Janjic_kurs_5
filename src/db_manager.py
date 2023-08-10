import psycopg2


class DBManager:
    """Класс для работы с данными в БД"""

    def __init__(self, database_name: str, params: dict):
        self.conn = psycopg2.connect(dbname=database_name, **params)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self) -> list[dict]:
        """получает список всех компаний и количество вакансий у каждой компании."""
        with self.conn:
            self.cur.execute(f"SELECT employer_id, employer_name, COUNT(vacancies.vacancy_id) as vacancies_number "
                             f"FROM employers JOIN vacancies USING(employer_id) GROUP BY employer_id")
            data = self.cur.fetchall()
            data_dict = [{"employer_id": d[0], "employer_name": d[1], "vacancies_number": d[2]} for d in data]
            return data_dict

    def get_all_vacancies(self) -> list[dict]:
        """получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        with self.conn:
            self.cur.execute(f"SELECT vacancy_id, vacancy_name, employers.employer_name, vacancy_url, "
                             f"salary_from, salary_to FROM vacancies JOIN employers USING(employer_id)")
            data = self.cur.fetchall()
            data_dict = [{"vacancy_id": d[0], "vacancy_name": d[1], "employer_name": d[2],
                          "vacancy_url": d[3], "salary_from": d[4], "salary_to": d[5]} for d in data]
            return data_dict

    def get_avg_salary(self) -> int:
        """получает среднюю зарплату по вакансиям."""
        self.cur.execute(f"SELECT AVG(salary_to) FROM vacancies")
        data = self.cur.fetchall()
        return int(data[0][0])

    def get_vacancies_with_higher_salary(self) -> list[dict]:
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        with self.conn:
            self.cur.execute(f"SELECT * FROM vacancies WHERE salary_to > (SELECT AVG(salary_to) FROM vacancies)")
            data = self.cur.fetchall()
            data_dict = [{"vacancy_id": d[0], "vacancy_name": d[1], "vacancy_url": d[2],
                          "area_name": d[3], "salary_from": d[4], "salary_to": d[5],
                          "employer_id": d[6], "requirement": d[7]} for d in data]
            return data_dict

    def get_vacancies_with_keyword(self, keyword) -> list[dict]:
        """получает список всех вакансий, в названии которых содержатся переданные
        в метод слова, например “python”."""
        with self.conn:
            self.cur.execute(f"SELECT * FROM vacancies WHERE vacancy_name LIKE '%{keyword}%'")
            data = self.cur.fetchall()
            data_dict = [{"vacancy_id": d[0], "vacancy_name": d[1], "vacancy_url": d[2],
                          "area_name": d[3], "salary_from": d[4], "salary_to": d[5],
                          "employer_id": d[6], "requirement": d[7]} for d in data]
            return data_dict
