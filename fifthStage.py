import os
import csv
import shutil


output_folder = 'dataset/output_csv'
if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)


txt_files = []
for root, dirs, files in os.walk('dataset'):
    for file in files:
        if file.endswith('.txt'):
            txt_files.append(os.path.join(root, file))


for txt_file in txt_files:
    csv_file = os.path.join(output_folder, os.path.splitext(os.path.basename(txt_file))[0] + '.csv')
    
    data = []
    with open(txt_file, 'r') as file:
        for line in file:
            row = line.strip().split()
            if len(row) >= 4:   
                row = [int(row[0]), int(row[1]), int(row[2]), float(row[3])]
                data.append(row)
    
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

print(f"Данные dataset .txt успешно сконвертированы в .csv и сохранены в папку '{output_folder}'.")
