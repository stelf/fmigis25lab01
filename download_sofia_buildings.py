# download_bulgaria_buildings.py
"""
Скрипт за изтегляне на файловете със сгради за България от Microsoft GlobalMLBuildingFootprints.
Чете input.txt, тегли .gz файловете в IN/ и обновява input.txt само с успешните изтегляния.
Работи на всички платформи (Windows, macOS, Linux).
"""
import os
import requests
import csv
import tempfile

DATASET_CSV_URL = "https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv"
INPUT_FILE = "input.txt"
OUT_DIR = "IN"
COUNTRY = "Bulgaria"

os.makedirs(OUT_DIR, exist_ok=True)

def fetch_bulgaria_urls():
    print(f"Изтегляне на dataset-links.csv от {DATASET_CSV_URL} ...")
    with requests.get(DATASET_CSV_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False, mode="w+b") as tmp:
            for chunk in r.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.flush()
            tmp.seek(0)
            tmp_name = tmp.name
    urls = []
    with open(tmp_name, encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row.get("Country or Region", "").strip() == COUNTRY:
                urls.append((row.get("QuadKey", "").strip(), row.get("Country or Region", "").strip(), row.get("URL", "").strip()))
    os.remove(tmp_name)
    return urls

def write_input_txt(url_tuples):
    with open(INPUT_FILE, "w", encoding="utf-8") as f:
        for quadkey, country, url in url_tuples:
            f.write(f"{quadkey},{country},{url}\n")
    print(f"input.txt е обновен с {len(url_tuples)} файла за {COUNTRY}.")

def parse_input_lines():
    with open(INPUT_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split(",")
            if len(fields) >= 3:
                url = fields[2].strip()
                yield url, line

def download_file(url, dest):
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception as e:
        print(f"Неуспешно изтегляне: {url} ({e})")
        return False

def main():
    # 1. Дърпаме dataset-links.csv и обновяваме input.txt
    url_tuples = fetch_bulgaria_urls()
    write_input_txt(url_tuples)
    # 2. Дърпаме файловете от input.txt
    successful_lines = []
    for url, orig_line in parse_input_lines():
        filename = os.path.basename(url)
        dest = os.path.join(OUT_DIR, filename)
        if os.path.exists(dest):
            print(f"{filename} вече съществува. Пропуснато.")
            successful_lines.append(orig_line)
            continue
        print(f"Изтегляне: {url} ...")
        if download_file(url, dest):
            successful_lines.append(orig_line)
    if successful_lines:
        with open(INPUT_FILE, "w", encoding="utf-8") as f:
            for l in successful_lines:
                f.write(l + "\n")
        print("input.txt е обновен само с успешно изтеглените файлове.")

if __name__ == "__main__":
    main()