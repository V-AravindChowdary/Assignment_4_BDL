import pandas as pd
from sklearn.metrics import r2_score
import os
import yaml

def compute_r2_scores(gt_dir, pred_dir, output_dir):
    ground_truth_files = [fname for fname in os.listdir(gt_dir) if fname.endswith('.csv')]
    for gt_file_name in ground_truth_files:
        gt_path = os.path.join(gt_dir, gt_file_name)
        pred_path = os.path.join(pred_dir, gt_file_name).replace('_prepared.csv', '_processed.csv')

        gt_data = pd.read_csv(gt_path).dropna(axis=1, how='all')
        pred_data = pd.read_csv(pred_path).dropna(axis=1, how='all')

        shared_columns = set(gt_data.columns).intersection(pred_data.columns)
        gt_data = gt_data.dropna(subset=shared_columns)
        pred_data = pred_data.dropna(subset=shared_columns)

        common_months = set(gt_data['Month']).intersection(pred_data['Month'])
        gt_data = gt_data[gt_data['Month'].isin(common_months)]
        pred_data = pred_data[pred_data['Month'].isin(common_months)]

        r2_results = []
        for column in shared_columns:
            if column != 'Month':
                r2_results.append(r2_score(gt_data[column], pred_data[column]))

        overall_r2_status = 'Consistent' if all(score >= 0.9 for score in r2_results) else 'Inconsistent'

        output_file = os.path.join(output_dir, gt_file_name.replace('_prepare.csv', '_r2.txt'))
        with open(output_file, 'w') as file:
            file.write(overall_r2_status)
            file.write('\n')
            file.write(','.join([str(score) for score in r2_results]))

        print(f'{gt_file_name}: {overall_r2_status}')

def main():
    with open("params.yaml", 'r') as file:
        settings = yaml.safe_load(file)
    gt_dir = settings["data_prepare"]["dest_folder"]
    pred_dir = settings["data_process"]["dest_folder"]
    output_dir = settings["evaluate"]["output"]
    os.makedirs(output_dir, exist_ok=True)
    compute_r2_scores(gt_dir, pred_dir, output_dir)

if __name__ == "__main__":
    main()

