import psycopg2
from dotenv import load_dotenv
import os
import requests

load_dotenv()

# Подключение к базе данных
def get_db_connection():
    """
    Устанавливает соединение с базой данных PostgreSQL и возвращает объект соединения с БД.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            sslmode=os.getenv('DB_SSLMODE'),
            sslrootcert=os.getenv('DB_CERT_PATH'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            target_session_attrs=os.getenv('DB_TSA')
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к базе данных: {e}")
        raise


def update_top100_to_db(data):
    """
    Обновляет данные топ-100 репозиториев в БД, добавляет новые записи при необходимости.
    """
    # Подключаемся к базе данных
    conn = get_db_connection()
    cur = conn.cursor()

    # Запрос для обновления данных по репо (если он уже есть в таблице) либо для добавления данных по новому репо
    insert_query = """
        INSERT INTO top_100_repos (repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (repo) DO UPDATE SET (position_cur, position_prev, stars, watchers, forks, open_issues) = (
            EXCLUDED.position_cur, EXCLUDED.position_prev, EXCLUDED.stars, EXCLUDED.watchers, EXCLUDED.forks, EXCLUDED.open_issues)
        """

    try:
        cur.executemany(insert_query, data)
    except Exception as e:
        print(f'При добавлении данных в бд произошла ошибка: {e}')
    
    # Подтверждаем изменения, закрываем курсор и соединение
    conn.commit()
    cur.close()
    conn.close()


def get_repo_position_cur_from_db():
    """
    Проверяет существование таблицы top_100_repos и возвращает текущие позиции репозиториев в топ-100. 
    """
    # Подключаемся к базе данных
    conn = get_db_connection()
    cur = conn.cursor()


    # Создаем таблицу, если ее еще нет
    cur.execute("""
        CREATE TABLE IF NOT EXISTS top_100_repos (
            repo VARCHAR(255) PRIMARY KEY,
            owner VARCHAR(255) NOT NULL,
            position_cur INTEGER NOT NULL, 
            position_prev INTEGER,
            stars INTEGER NOT NULL,              
            watchers INTEGER NOT NULL,           
            forks INTEGER NOT NULL,              
            open_issues INTEGER NOT NULL,        
            language VARCHAR(100)
        )
    """)

    cur.execute("""
        SELECT repo, position_cur 
        FROM top_100_repos
    """)

    rows = cur.fetchall()

    # Подтверждаем изменения, закрываем курсор и соединение
    conn.commit()
    cur.close()
    conn.close()

    # Возвращаем данные в виде {'repo': position_cur}
    return dict(rows)


def get_top100_from_db():
    """
    Извлекает топ-100 репозиториев из БД, отсортированный по количеству звезд.
    """
    # Подключаемся к базе данных
    conn = get_db_connection()

    # Создаем курсор
    cur = conn.cursor()

    cur.execute("""
        SELECT repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language 
        FROM top_100_repos 
        ORDER BY stars DESC 
        LIMIT 100
    """)

    rows = cur.fetchall()
    conn.close()

    return rows


def parse_github_top100():
    """
    Запрашивает и получаем данные о топ 100 репозиториев на Github по количеству звезд и обновляет данные в бд.
    """
    try:
        # Запрос нужно сузить, чтобы результат поиска был полным. 
        # Один из вариантов - просто сузить поиск среди репозиториев с кол-вом звезд > 1000
        params = {
            'q': 'stars:>1000', 
            'sort': 'stars',
            'order': 'desc',
            'per_page': 100 # максимум - 100
        }

        response = requests.get("https://api.github.com/search/repositories", params=params)
        
        if response.status_code == 200:
            print(f'Лимит по REST API Github: осталось {response.headers["X-RateLimit-Remaining"]} из {response.headers["X-RateLimit-Limit"]} запросов.')
        else:
            print(f"Ошибка: {response.status_code}")
        
    except Exception:
        print('Не удалось получить данные с сервера Github.')


    response_json = response.json()

    # Проверка что результаты полные
    if not response_json['incomplete_results'] is False:
        print('Полученные данные НЕ полные! Нужно сузить запрос к API Github')

    repos = response_json.get('items')
    print(f'Получены данные по {len(repos)} репозиториям')

    # Получаем данные из бд о позиции репозитория в рейтинге,
    # на момент предыдущего обновления данных в бд. В виде {'repo': position_cur}
    positions_prev = get_repo_position_cur_from_db()

    # Собираем требуемые данные в переменную
    repos_data_for_db = []
    count = 0
    for repo in repos:
        count += 1
        repo_dict = {
            'repo': repo['full_name'],
            'owner': repo['owner']['login'],
            'position_cur': count, # данные уже отсортированы в ответе от гитхаба
            'position_prev': positions_prev.get(repo['full_name'], None),
            'stars': repo['stargazers_count'],
            'watchers': repo['watchers_count'],
            'forks': repo['forks_count'],
            'open_issues': repo['open_issues_count'],
            'language': repo['language']
        }
        
        repos_data_for_db.append(tuple(repo_dict.values()))

    # Обновляем данные в бд
    update_top100_to_db(repos_data_for_db)


def get_repo_activity(owner, repo, since, until):
    """
    Запрашиваем и получаем данные об активности по репозиторию:
    - статистика по коммитам по дням со списком авторов
    """
    
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    
    # Указываем ограничение по количеству коммитов
    max_results = 1000 
    count_results = 0
    
    params = {
        'since': since, 
        'until': until,
        'per_page': 100 # максимум - 100
    }

    activity_data = {}

    while url and count_results < max_results:
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                print(f'Лимит по REST API Github: осталось {response.headers["X-RateLimit-Remaining"]} из {response.headers["X-RateLimit-Limit"]} запросов.')
            else:
                print(f"Ошибка: {response.status_code}")
            
        except Exception as e:
            print(f'Не удалось получить данные с сервера Github. Ошибка {e}')
        pass

        response_json = response.json()
        
        number_of_commits_on_current_page = len(response_json)
        
        # Добавляем данные в словарь activity_data  
        for commit in response_json:
            # 'date' берем из 'committer', а не из 'author', потому что по этой дате осуществляется поиск при обращении к API Github параметрами since, until
            date = commit['commit']['committer']['date'].split('T')[0]
            author = commit['commit']['author']['name']
            activity_data[date] = activity_data.get(date, {'commits': 0, 'authors': []})
            activity_data[date]['commits'] += 1
            if author not in activity_data[date]['authors']:
                activity_data[date]['authors'].append(author)
        
        print(f'Обработано {number_of_commits_on_current_page} коммитов')

        count_results += number_of_commits_on_current_page

        # Используем пагинацию в ответе от гитхаба
        # Проверяем, есть ли ссылка на следующую страницу в заголовке 'Link'
        if 'Link' in response.headers:
            links = response.headers['Link']
            # Ищем ссылку на следующую страницу
            next_link = None
            for link in links.split(','):
                if 'rel="next"' in link:
                    # Извлекаем URL из <URL>; rel="next"
                    next_link = link[link.find('<') + 1:link.find('>')]
                    break
            url = next_link
        else:
            url = None  # Если ссылки нет, выходим из цикла

    print(f'Всего найдено коммитов за выбранный промежуток времени: {count_results}. Максимум: {max_results}')
    return activity_data


# entry point для Yandex Cloud функции
def handler(event, context):    
    parse_github_top100()
    print('Парсинг данных завершен успешно. Данные в бд обновлены')
