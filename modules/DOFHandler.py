import os
import requests

DOF_BASE_URL = "http://aeronav.faa.gov/Obst_Data/DAILY_DOF_DAT.ZIP"


def download_dof(save_directory: str) -> str:
    print("Downloading Daily Obstacle File")
    result = ""
    dof_url = DOF_BASE_URL
    file_name = os.path.basename(dof_url)

    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    file_path = os.path.join(save_directory, file_name)

    response = requests.get(dof_url, stream=True)
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
        print(f"DOF downloaded to: {file_path}")
        result = file_path
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

    return result


def format_and_move(file_path: str, output_path: str) -> None:
    strip_limit = 4
    with open(file_path, "r", errors="ignore") as f_in:
        lines = f_in.readlines()[strip_limit:]

    with open(output_path, "w") as f_out:
        f_out.writelines(lines)
