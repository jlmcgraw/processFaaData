import os
import shutil


def is_subdir(directory_path: str, main_directory: str) -> bool:
    subdir_path = os.path.abspath(directory_path)
    return os.path.commonpath([main_directory]) == os.path.commonpath(
        [main_directory, subdir_path]
    )


def remove_file(file_path: str) -> None:
    if os.path.exists(file_path):
        os.remove(file_path)


def remove_directory(directory_path: str) -> None:
    if os.path.exists(directory_path):
        if os.path.isdir(directory_path):
            shutil.rmtree(directory_path)


def create_directory(directory_path: str) -> None:
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
