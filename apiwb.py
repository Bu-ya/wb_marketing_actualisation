import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

# Функция для отправки запроса и получения данных
def fetch_data(secret_key, date_from, date_to, limit, company_name):
    v = 4 if date_to < '2024-01-29' else 5
    url = f"https://statistics-api.wildberries.ru/api/v{v}/supplier/reportDetailByPeriod"
    headers = {"Authorization": secret_key}
    params = {"dateFrom": date_from, "dateTo": date_to, "limit": limit}
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        if not os.path.exists(company_name):
            os.makedirs(company_name)
        csv_filename = os.path.join(company_name, f"report_{date_from}_to_{date_to}.csv")
        df.to_csv(csv_filename, index=False)
        print(f"Данные успешно сохранены в файл: {csv_filename}")
        return True
    else:
        print(f"Ошибка при запросе данных: {response.status_code}, {response.text}")
        return False

# Функция для актуализации данных за период
def update_data(secret_key, start_date, end_date, limit, company_name):
    current_date = start_date
    unsuccessful_dates = []
    success_log = []

    while current_date <= end_date:
        date_from = current_date.strftime("%Y-%m-%d")
        date_to = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")
        if fetch_data(secret_key, date_from, date_to, limit, company_name):
            success_log.append((date_from, date_to, "Success"))
        else:
            success_log.append((date_from, date_to, "Failed"))
            unsuccessful_dates.append((date_from, date_to))
        current_date += timedelta(days=2)
    
    # Повторная попытка для неудачных дат
    for date_from, date_to in unsuccessful_dates:
        if fetch_data(secret_key, date_from, date_to, limit, company_name):
            success_log.append((date_from, date_to, "Retry Success"))
        else:
            success_log.append((date_from, date_to, "Retry Failed"))

    # Сохранение лога успехов и неудач
    log_df = pd.DataFrame(success_log, columns=["Date From", "Date To", "Status"])
    log_filename = os.path.join(company_name, "update_log.csv")
    log_df.to_csv(f'./tables/{log_filename}', index=False)
    print(f"Лог обновления сохранен в файл: ./tables/{log_filename}")

# Главная функция для запуска в бесконечном цикле
def main_loop(secret_key, start_date, limit, company_name, interval):
    while True:
        try:
            end_date = datetime.now()
            update_data(secret_key, start_date, end_date, limit, company_name)
            print(f"Актуализация завершена. Следующая попытка через {interval} секунд.")
            time.sleep(interval)
        except KeyboardInterrupt:
            print("Программа остановлена пользователем.")
            break
        except Exception as e:
            print(f"Произошла ошибка: {e}")

# Пример использования функции
if __name__ == "__main__":
    SECRET_KEY = "ваш_секретный_ключ"
    start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
    limit = 100
    company_name = "company_name"
    interval = 3600  # Интервал в секундах (например, 1 час)

    main_loop(SECRET_KEY, start_date, limit, company_name, interval)

