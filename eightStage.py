import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

hostname = '127.0.0.1'
username = 'postgres'
password = '1234'
database = 'gul'

conn = None

def drawMap():
    cursor = conn.cursor()
    query = "SELECT longitude, latitude FROM data.coastline"
    
    cursor.execute(query)

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    data = pd.DataFrame(results, columns=['longitude', 'latitude'])

    plt.figure(figsize=(10, 6))
    plt.scatter(data['longitude'], data['latitude'], s=0.5, alpha=0.5)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Map')

    plt.xlim(-180, 180)
    plt.ylim(-90, 90)

    plt.show()

def get_connection():
    print("Начинаем соединение с БД")
    global conn
    attempts = 0
    max_attempts = 5
    delay = 2  
    
    while attempts < max_attempts:
        try:
            conn = psycopg2.connect(
                host=hostname,
                database=database,
                user=username,
                password=password
            )
            print("Успешное подключение к базе данных")
            break
        except psycopg2.Error as e:
            print("Ошибка подключения к базе данных:", e)
            attempts += 1
            print(f"Повторная попытка подключения через {delay} сек...")
            time.sleep(delay)


def verify_database_info():
    global ostname_sftp, username_sftp, password_sftp

verify_database_info()
get_connection()
drawMap()
