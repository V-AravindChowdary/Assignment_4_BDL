import pandas as pd
import yaml
import os

def calculate_monthly_means(input_dir, output_dir, columns_dir):
    csv_files_list = [file for file in os.listdir(input_dir) if file.endswith('.csv')]
    for csv_file in csv_files_list:
        input_file_path = os.path.join(input_dir, csv_file)
        dataframe = pd.read_csv(input_file_path)
        dataframe['DATE'] = pd.to_datetime(dataframe['DATE'])
        dataframe['Month'] = dataframe['DATE'].dt.month

        headers = dataframe.columns
        daily_columns = []
        daily_data = []
        selected_columns = []
        monthly_data = []
        renamed_headers = []

        for header in headers:
            if 'Daily' in header:
                daily_data.append(header)
                field_name = header.replace('Daily', '')
                daily_columns.append(field_name)
            if 'Monthly' in header:
                monthly_data.append(header)

        txt_file = os.path.join(columns_dir, csv_file.replace('.csv', '.txt'))
        with open(txt_file, 'r') as file:
            monthly_fields = file.read().split(',')

        for daily_column in daily_columns:
            for monthly_field in monthly_fields:
                if (monthly_field in daily_column) or ('Average' in daily_column and monthly_field.replace('Mean', '').replace('Average', '') in daily_column.replace('Average', '')):
                    selected_columns.append(daily_data[daily_columns.index(daily_column)])
                    renamed_headers.append(monthly_data[monthly_fields.index(monthly_field)])

        monthly_dataframe = dataframe.dropna(how='all', subset=selected_columns)[['Month'] + selected_columns]

        dtype_conversion = {col: float for col in monthly_dataframe.columns if col != 'Month'}
        monthly_dataframe = monthly_dataframe.astype(dtype_conversion)
        aggregated_data = monthly_dataframe.groupby('Month').mean().reset_index().rename(dict(zip(selected_columns, renamed_headers)), axis=1)

        output_file_path = os.path.join(output_dir, csv_file).replace('.csv', '_processed.csv')
        aggregated_data.to_csv(output_file_path, index=False)

def main():
    with open("params.yaml", 'r') as file:
        config = yaml.safe_load(file)
    input_dir = config["data_source"]["temp_dir"]
    columns_dir = config["data_prepare"]["dest_folder"]
    output_dir = config["data_process"]["dest_folder"]
    os.makedirs(output_dir, exist_ok=True)
    calculate_monthly_means(input_dir, output_dir, columns_dir)

if __name__ == "__main__":
    main()

