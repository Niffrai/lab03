import pandas as pd
import os

def createRegions():
    df = pd.read_csv('data/data.csv')
    unique_regions = df['Regions'].unique()

    regions_df = pd.DataFrame({'identifier': range(1, len(unique_regions) + 1),
                           'description': unique_regions})

    regions_df.to_csv('data/regions.csv', index=False)
    print("Создан файл регионов data/regions.csv")

def createCountries():
    df = pd.read_csv('data/data.csv')

    regions_df = pd.read_csv('data/regions.csv')

    unique_countries = df.iloc[:, 0].unique()  
    countries_regions = df.iloc[:, [0, 1]]  

    countries_df = pd.DataFrame({'identifier': range(1, len(unique_countries) + 1),
                                 'country': unique_countries})

    countries_df = countries_df.merge(countries_regions, how='left', left_on='country', right_on=df.columns[0])
    countries_df = countries_df.merge(regions_df, how='left', left_on=df.columns[1], right_on='description')

    countries_df = countries_df[['identifier_y', 'identifier_x', df.columns[1]]]
    countries_df.columns = ['identifier', 'region', 'description']
    countries_df['identifier'] = range(1, len(countries_df) + 1)

    

    countries_df.to_csv('data/countries.csv', index=False)
    print("Создан файл стран data/countries.csv")


def createCities():
 
    df = pd.read_csv('data/data.csv')


    countries_df = pd.read_csv('data/countries.csv')

    coordinates_df = pd.read_csv('countries/output_convert/ne_10m_populated_places.csv')

 
    unique_cities = df['Cities'].unique()
    cities_countries = df[['Cities', 'Regions', 'Dataset']]  

    cities_df = pd.DataFrame({'identifier': range(1, len(unique_cities) + 1),
                              'country': unique_cities})


    cities_df = cities_df.merge(cities_countries.drop_duplicates(), how='left', left_on='country', right_on='Cities')
    cities_df = cities_df.merge(countries_df.drop_duplicates(), how='left', left_on='Regions', right_on='description')


    cities_df = cities_df.merge(coordinates_df.drop_duplicates(subset=['NAME']), how='left', left_on='country', right_on='NAME')

    
    cities_df = cities_df[['identifier_x', 'identifier_y', 'country', 'latitude', 'longitude', 'Dataset']]
    cities_df.columns = ['identifier', 'country', 'description', 'latitude', 'longitude', 'dataset']

    
    cities_df['identifier'] = range(1, len(cities_df) + 1)
    cities_df['country'] = range(1, len(cities_df) + 1)
    cities_df[['latitude', 'longitude']] = cities_df[['latitude', 'longitude']].fillna('0')

    
    cities_df.to_csv('data/cities.csv', index=False)
    print("Создан файл городов data/cities.csv")


def createMeasurement():
    cities_df = pd.read_csv('data/cities.csv')

    measurement_dir = 'data/measurement/'
    if os.path.exists(measurement_dir):
        for file_name in os.listdir(measurement_dir):
            file_path = os.path.join(measurement_dir, file_name)
            os.remove(file_path)
        os.rmdir(measurement_dir)
    os.mkdir(measurement_dir)

    for index, row in cities_df.iterrows():
        city_id = row['identifier']
        dataset = row['dataset']

        file_path = os.path.join('dataset/output_csv', f'{dataset}.csv')
        if os.path.exists(file_path):
            with open(file_path, 'r') as data_file:
                next(data_file)  

                output_file_path = os.path.join(measurement_dir, f'{dataset}.csv')
                with open(output_file_path, 'w') as output_file:
                    output_file.write('city,timestamp,temperature\n') 

                    for line in data_file:
                        parts = line.strip().split(',')
                        timestamp = f'{parts[0]}-{parts[1]}-{parts[2]}'
                        temperature = parts[3]

                        output_file.write(f'{city_id},{timestamp},{temperature}\n')

    print("Созданы файлы с измерениями в папке data/measurement/")

    

def addCoastline():
    source_file = "coastline/output_convert/ne_10m_coastline.csv"
    target_file = "data/coastline.csv"
    df = pd.read_csv(source_file, delimiter=',')
    df['shape'] = range(1, len(df) + 1)
    df['segment'] = range(1, len(df) + 1)
    df = df[['shape', 'segment', 'latitude', 'longitude']]
    
    df.to_csv(target_file, index=False)

    print("Файл coastline/output_convert/ne_10m_coastline.csv перемещен и дополнен.")


createRegions()
createCountries()
createCities()
createMeasurement()
addCoastline()
