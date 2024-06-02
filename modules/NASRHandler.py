from datetime import datetime, timedelta
import os
import requests

NASR_BASE_URL = "https://nfdc.faa.gov/webContent/28DaySub/"
NASR_FILE_BASE = "28DaySubscription_Effective_"
NASR_FILE_TYPE = "zip"
AIRAC_CYCLE_DAYS = 28

# The original script queried the FAA APRA site for the NASR URL
# The APRA site has since been taken down.
# This implementation takes a known AIRAC date and iterates through cycles to find the closest cycle.
# If this is being used significantly far in the future, you may want to modify the base_date_str
# to a known AIRAC date closer to the current date.

base_date_str = "2024-03-21"
base_date = datetime.strptime(base_date_str, "%Y-%m-%d")


def get_cycle_date(version: str = "next") -> str:
    result = ""
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    current_date = datetime.strptime(current_date_str, "%Y-%m-%d")

    new_date_obj = base_date + timedelta(days=AIRAC_CYCLE_DAYS)

    while True:
        if current_date < new_date_obj:
            if version == "next":
                result = new_date_obj.strftime("%Y-%m-%d")
            else:
                new_date_obj = new_date_obj - timedelta(days=AIRAC_CYCLE_DAYS)
                result = new_date_obj.strftime("%Y-%m-%d")
            break
        new_date_obj = new_date_obj + timedelta(days=AIRAC_CYCLE_DAYS)

    return result


def download_nasr(save_directory: str) -> str:
    print("Downloading NASR File")
    result = ""
    airac_date = get_cycle_date()
    nasr_url = f"{NASR_BASE_URL}{NASR_FILE_BASE}{airac_date}.{NASR_FILE_TYPE}"
    file_name = os.path.basename(nasr_url)

    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    file_path = os.path.join(save_directory, file_name)

    response = requests.get(nasr_url, stream=True)
    if response.status_code == 200:
        total_size_in_bytes = int(response.headers.get("content-length", 0))
        block_size = 1048576  # 1 MB
        downloaded_size = 0
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=block_size):
                file.write(chunk)
                downloaded_size += len(chunk)
                progress = downloaded_size / total_size_in_bytes * 100
                print(
                    f"Downloaded: {downloaded_size}/{total_size_in_bytes} bytes ({progress:.2f}%)",
                    end="\r",
                )
        print(f"NASR downloaded to: {file_path}")
        result = file_path
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

    return result
