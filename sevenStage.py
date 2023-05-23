import paramiko
import psycopg2
import csv
import os
import time

hostname = '127.0.0.1'
username = 'postgres'
password = '1234'
database = 'gul'
hostname_sftp = '127.0.0.1'
username_sftp = 'postgres'
password_sftp = '1234'
conn = None

source_dir = '/home/postgres/lab3-prak/lab3/data/measurement'
target_dir = '/home/postgres/data/data/measurement'


def create_sftp_directory(sftp, directory_path):
    """
    Создает папку на сервере SFTP, если она не существует.
    """
    try:
        sftp.chdir(directory_path)
    except IOError:
        sftp.mkdir(directory_path)
        sftp.chdir(directory_path)

def clear_sftp_directory(sftp, directory_path):
    """
    Очищает содержимое папки на сервере SFTP.
    """
    sftp.chdir(directory_path)
    files = sftp.listdir()
    for file_name in files:
        file_path = directory_path + '/' + file_name
        sftp.remove(file_path)


def transfer_files_to_database():
    print("Передача файлов через SFTP")
    transport = paramiko.Transport((hostname_sftp, 22))
    transport.connect(username=username_sftp, password=password_sftp)
    sftp = paramiko.SFTPClient.from_transport(transport)
    create_sftp_directory(sftp, target_dir)

    clear_sftp_directory(sftp, target_dir)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname_sftp, username=username_sftp, password=password_sftp)
        sftp = ssh.open_sftp()
        sftp.chdir(target_dir)
        files = os.listdir(source_dir)
        for filename in files:
            source_path = os.path.join(source_dir, filename)
            target_path = os.path.join(target_dir, filename)
            if os.path.isfile(source_path):
                sftp.put(source_path, target_path)
                os.remove(source_path)

        sftp.close()

    finally:
        ssh.close()








def verify_database_info():
    global hostname, username, password, database, hostname_sftp, username_sftp, password_sftp
    
    print("Текущая информация о базе данных:")
    print(f"Хост: {hostname}")
    print(f"Пользователь: {username}")
    print(f"Пароль: {password}")
    print(f"База данных: {database}")

def create_scheme(scheme_name):
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {scheme_name};"
    cursor = conn.cursor()
    cursor.execute(create_schema_query)
    conn.commit()
    cursor.close()

def install_file_fdw(schema):
    cursor = conn.cursor()
    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS file_fdw WITH SCHEMA {schema};")
    conn.commit()
    cursor.close()

def create_file_server(schema):
    cursor = conn.cursor()

    drop_server_query = "DROP SERVER IF EXISTS file_server CASCADE"
    cursor.execute(drop_server_query)
    conn.commit()

    create_server_query = f"""
        CREATE SERVER file_server
        FOREIGN DATA WRAPPER file_fdw;
    """
    cursor.execute(create_server_query)
    conn.commit()
    cursor.close()


def create_foreign_table(table_name, csv_file):
    cursor = conn.cursor()
    create_scheme('external')
    cursor.execute(f"DROP FOREIGN TABLE IF EXISTS external.{table_name}")
    create_table_query = f"""
        CREATE FOREIGN TABLE external.{table_name} (
            city INTEGER,
            mark TEXT,
            temperature DOUBLE PRECISION
        )
        SERVER file_server
        OPTIONS (
            filename '{csv_file}',
            format 'csv',
            header 'true'
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


def create_measurement_foreign_tables():
    measurements_dir = '/home/postgres/lab3-prak/lab3/data/measurement'  # Путь к директории с файлами измерений
    measurement_files = [f for f in os.listdir(measurements_dir) if f.endswith('.csv')]

    for file_name in measurement_files:
        dataset = file_name.replace('.csv', '')
        table_name = f"measurement_{dataset}"
        csv_file = os.path.join(measurements_dir, file_name)
        create_foreign_table(table_name, csv_file)

    print("Созданы внешние таблицы для файлов измерений")


def create_table(scheme ,table_name, columns):
    cursor = conn.cursor()
    drop_query = f"DROP TABLE IF EXISTS {scheme}.{table_name.format()} CASCADE;"
    cursor.execute(drop_query)
    conn.commit()
    create_query = f"CREATE TABLE {scheme}.{table_name} ({', '.join(columns)})"
    cursor.execute(create_query)
    conn.commit()
    cursor.close()

def import_csv_to_table(schema, csv_file, table_name):
    cursor = conn.cursor()
    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        columns = next(csv_data)
        insert_query = f"INSERT INTO {schema}.{table_name} VALUES ({', '.join(['%s'] * len(columns))})"
        cursor.execute(f"SET search_path TO {schema}")
        file.seek(0)  
        next(csv_data)  
        cursor.copy_from(file, table_name, sep=',', null='', columns=columns)
        
    conn.commit()
    cursor.close()


def merge_all_scheme(schema, table_name):
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='external';")
    external_tables = cur.fetchall()
    
    for table in external_tables:
        cur.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM external.{table[0]};")


    conn.commit()
    cur.close()
    conn.close()



def get_csv_files():
    csv_files = []
    measurement_dir = '/home/postgres/lab3-prak/lab3/data/measurement'
    for file_name in os.listdir(measurement_dir):
        if file_name.endswith('.csv'):
            csv_files.append(os.path.join(measurement_dir, file_name))
    return csv_files

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


verify_database_info()
get_connection()

if conn is not None:
    print("Создаем схему data")
    create_scheme('data')

    install_file_fdw('data')
    create_file_server('data')

    print("Создаем таблицу регионов")
    create_table('data', 'region', ['identifier SERIAL PRIMARY KEY', 'description TEXT'])

    print("Создаем таблицу стран")
    create_table('data', 'country', ['identifier SERIAL PRIMARY KEY', 'region INTEGER REFERENCES data.region(identifier)', 'description TEXT'])

    print("Создаем таблицу городов")
    create_table('data', 'city', ['identifier SERIAL PRIMARY KEY', 'country INTEGER REFERENCES data.country(identifier)', 'description TEXT', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION', 'dataset TEXT'])

    print("Создаем таблицу измерений")
    create_table('data','measurement', ['city INTEGER REFERENCES data.city(identifier)', 'mark TEXT', 'temperature TEXT'])

    print("Создаем таблицу береговых линий")
    create_table('data', 'coastline', ['shape INTEGER', 'segment INTEGER', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION'])


    try:
        transfer_files_to_database()
    except:
        print(f"Не удалось подключиться к SFTP")

    create_measurement_foreign_tables()

    print("Импорт данных в таблицу регионов из data/regions.csv")
    import_csv_to_table('data', 'data/regions.csv', 'region')

    print("Импорт данных в таблицу стран из data/countries.csv")
    import_csv_to_table('data', 'data/countries.csv', 'country')

    print("Импорт данных в таблицу городов из data/cities.csv")
    import_csv_to_table('data', 'data/cities.csv', 'city')

    print("Импорт данных в таблицу береговых линий из data/coastline.csv")
    import_csv_to_table('data', 'data/coastline.csv', 'coastline')

    print("Соединение всех внешних таблиц из external в measurement")
    merge_all_scheme('data', 'measurement')

    conn.close()
else:
    print("Ошибка подключения! Убедитесь, что вы правильно выбрали IP-адрес")
