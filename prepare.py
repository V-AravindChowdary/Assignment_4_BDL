import pandas as pd
import yaml
import os

def create_monthly_aggregates(src_folder, dest_folder):
    csv_files = [filename for filename in os.listdir(src_folder) if filename.endswith('.csv')]
    for filename in csv_files:
        src_csv_path = os.path.join(src_folder, filename)
        data = pd.read_csv(src_csv_path)
        data['DATE'] = pd.to_datetime(data['DATE'])
        data['Month'] = data['DATE'].dt.month

        column_headers = data.columns
        monthly_columns = []
        actual_monthly_columns = []
        for header in column_headers:
            if 'Monthly' in header:
                actual_monthly_columns.append(header)
                field_name = header.replace('Monthly', '')
                if 'WetBulb' not in field_name and 'Departure' not in field_name:
                    field_name = field_name.replace('Temperature', 'DryBulbTemperature')
                monthly_columns.append(field_name)

        monthly_data = data.dropna(how='all', subset=actual_monthly_columns)[['Month'] + actual_monthly_columns]
        dest_csv_path = os.path.join(dest_folder, filename).replace('.csv', '_prepared.csv')
        monthly_data.to_csv(dest_csv_path, index=False)

        txt_dest_path = os.path.join(dest_folder, filename.replace('.csv', '.txt'))
        with open(txt_dest_path, 'w') as txt_file:
            txt_file.write(','.join(monthly_columns))

def main():
    with open("params.yaml", 'r') as file:
        params = yaml.safe_load(file)
    src_folder = params["data_source"]["temp_dir"]
    dest_folder = params["data_prepare"]["dest_folder"]
    os.makedirs(dest_folder, exist_ok=True)
    create_monthly_aggregates(src_folder, dest_folder)

if __name__ == "__main__":
    main()

