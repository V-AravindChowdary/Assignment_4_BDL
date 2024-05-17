import subprocess
import sys

from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import requests
import yaml
from tqdm import tqdm

def retrieve_data(base_link, fetch_year, output_html, storage_dir, limit_files):
    subprocess.run(["curl", "-L", "-o", output_html, base_link + str(fetch_year)])
    
    with open(output_html, 'r') as html_file:
        html_text = html_file.read()

    soup = BeautifulSoup(html_text, 'html.parser')
    csv_links_sizes = []

    for row in soup.find_all('tr')[2:]:
        cells = row.find_all('td')
        if cells and cells[2].text.strip().endswith('M'):
            link = urljoin(base_link + str(fetch_year) + '/', cells[0].text.strip())
            file_size = float(cells[2].text.strip().replace('M', ''))
            csv_links_sizes.append((link, file_size))

    filtered_links = [link for link, size in csv_links_sizes if size > 45][:limit_files]
    os.makedirs(storage_dir, exist_ok=True)

    for link in filtered_links:
        response = requests.get(link, stream=True)
        if response.status_code == 200:
            file_path = os.path.join(storage_dir, os.path.basename(link))
            total_size = int(response.headers.get('content-length', 0))
            with tqdm(total=total_size, unit='iB', unit_scale=True, desc=os.path.basename(link)) as progress_bar:
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(1024):
                        file.write(chunk)
                        progress_bar.update(len(chunk))

def main():
    with open("params.yaml", 'r') as yaml_file:
        parameters = yaml.safe_load(yaml_file)

    base_link = parameters["data_source"]["base_url"]
    fetch_year = parameters["data_source"]["year"]
    output_html = parameters["data_source"]["output"]
    storage_dir = parameters["data_source"]["temp_dir"]
    limit_files = parameters["data_source"]["max_files"]

    retrieve_data(base_link, fetch_year, output_html, storage_dir, limit_files)

if __name__ == "__main__":
    main()

