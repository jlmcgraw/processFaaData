import argparse
import os

from modules.DatabaseHandler import create_databases
from modules.DOFHandler import download_dof, format_and_move
from modules.FileHandler import create_directory, is_subdir, remove_directory
from modules.NASRHandler import download_nasr
from modules.ZipHandler import recursive_unzip

DOWNLOAD_DIR = "./data/downloads"
OUTPUT_DIR = "./data"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable debug output"
    )
    parser.add_argument("-e", "--expand", action="store_true", help="expand text")
    parser.add_argument(
        "-g", "--geometry", action="store_true", help="create geometry for spatialite"
    )

    args = parser.parse_args()

    # Original Perl never uses this. Not yet used here.
    should_print_debug = args.verbose

    should_expand = args.expand
    should_create_geometry = args.geometry

    main_directory = os.path.dirname(os.path.abspath(__file__))
    if is_subdir(OUTPUT_DIR, main_directory):
        remove_directory(OUTPUT_DIR)
        create_directory(DOWNLOAD_DIR)
        nasr_file = download_nasr(DOWNLOAD_DIR)
        data_dir = recursive_unzip(nasr_file)

        obstacle_file = download_dof(DOWNLOAD_DIR)
        obstacle_dir = recursive_unzip(obstacle_file)
        format_and_move(f"{obstacle_dir}/DOF.DAT", f"{data_dir}/OBSTACLE.txt")

        data_dir = "./data/downloads/28DaySubscription_Effective_2024-06-13"
        create_databases(data_dir, OUTPUT_DIR, should_expand, should_create_geometry)

    else:
        print("DOWNLOAD_DIR is not a subdirectory of the root dir.")
        print(
            "This enforcement prevents important files on your hard drive from being deleted."
        )
        print("Modify the DOWNLOAD_DIR variable at your own risk.")


if __name__ == "__main__":
    main()
