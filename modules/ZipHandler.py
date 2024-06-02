import zipfile
import os

ZIP_EXT = ".zip"


def recursive_unzip(zip_path) -> str:
    def unzip(zip_file_path) -> str:
        base_path = os.path.splitext(zip_file_path)[0]
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(base_path)
            for file_name in zip_ref.namelist():
                extracted_path = os.path.join(base_path, file_name)
                if zipfile.is_zipfile(extracted_path):
                    unzip(extracted_path)
                    os.remove(extracted_path)

        return base_path

    return unzip(zip_path)
