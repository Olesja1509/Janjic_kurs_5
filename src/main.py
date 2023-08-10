from utils import get_vacancies, create_database, save_data_to_database
from db_manager import DBManager
from config import config


if __name__ == "__main__":

    # перечень компаний для парсинга
    employers_dict = {
        5333428: "L’etoile Digital",
        1879877: "YLAB Development",
        3133610: "ПрограммСистемс",
        3280416: "Цифровые привычки",
        808: "Konica Minolta",
        699283: "РАСЧЕТНЫЕ РЕШЕНИЯ",
        3169763: "Мэврика",
        4250943: "hotellab.io",
        1579449: "idaproject",
        1885395: "Р-Вижн",
        1075575: "НПК РоТеК"
    }

    params = config()

    # Создание базы данных
    create_database('hh_vacancies', params)

    # Получеие данных с hh/ru и создание списка со всеми вакансиями по выбранным компаниям
    hh_vacancies_dict = []

    for k in employers_dict.keys():
        vacancies = get_vacancies(k)
        hh_vacancies_dict.extend(vacancies)

    # Сохранение данных в БД
    save_data_to_database(employers_dict, hh_vacancies_dict, 'hh_vacancies', params)

    # Создание класса для работы в БД
    db_manager = DBManager('hh_vacancies', params)

    # Получения списка компаний и количества вакансий по ним
    companies_and_vacancies_count = db_manager.get_companies_and_vacancies_count()
    for vacancy in companies_and_vacancies_count:
        print(vacancy)

    # Получение данных по всем вакансиям с указанием названия компании,
    # названия вакансии и зарплаты и ссылки на вакансию
    companies_and_vacancies_data = db_manager.get_all_vacancies()
    for vacancy in companies_and_vacancies_data:
        print(vacancy)

    # Получение данных по всем вакансиям с указанием названия компании,
    # названия вакансии и зарплаты и ссылки на вакансию
    avg_salary = db_manager.get_avg_salary()
    print(avg_salary)

    # получение списка всех вакансий, у которых зарплата выше средней по всем вакансиям
    vacancies_with_higher_salary = db_manager.get_vacancies_with_higher_salary()
    for vacancy in vacancies_with_higher_salary:
        print(vacancy)

    # получение списка всех вакансий, в названии которых содержатся переданные в метод слова”
    user_input = input('Введите ключевое слово для фильтрации вакансий')
    vacancies_with_keyword = db_manager.get_vacancies_with_keyword(user_input)
    for vacancy in vacancies_with_keyword:
        print(vacancy)
