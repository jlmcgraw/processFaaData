from modules.Expanders import parse_expander
from modules.FileDefinitions import definitions
from modules.GeometryProcessors import parse_geometry
from modules.LineParser import LineParser
from modules.Normalizers import parse_normalizer


import glob
import os
import re
import sqlite3
import subprocess


def create_nasr_database(
    source_path: str,
    nasr_database_path: str,
    should_expand: bool = False,
    should_create_geometry: bool = False,
) -> None:

    have_created_table = []

    with sqlite3.connect(nasr_database_path) as dbh:
        dbc = dbh.cursor()

        dbc.execute("PRAGMA page_size=4096")
        dbc.execute("PRAGMA synchronous=OFF")
        # These were commented out in the original Perl:
        # # dbc.execute("PRAGMA count_changes=OFF")
        # # dbc.execute("PRAGMA temp_store=MEMORY")
        # # dbc.execute("PRAGMA journal_mode=MEMORY")

        # Create base tables
        dbc.execute("DROP TABLE IF EXISTS android_metadata;")
        dbc.execute("CREATE TABLE android_metadata ( locale TEXT );")
        dbc.execute("INSERT INTO android_metadata VALUES ( 'en_US' );")

        for key, _ in definitions.items():
            data_file = f"{source_path}/{key}.txt"
            base_file = key

            try:
                with open(data_file, "r", errors="ignore") as df:
                    master_record_row_id = 0
                    for line_num, line in enumerate(df):
                        if line_num % 1000 == 0:
                            print(f"Loading {base_file}: {line_num}")

                        record_type = ""
                        line_text = line.strip("\n")

                        if base_file in [
                            "ARB",
                            "COM",
                            "HARFIX",
                            "NATFIX",
                            "SSD",
                            "STARDP",
                            "OBSTACLE",
                        ]:
                            record_type = base_file
                        elif base_file == "WXL":
                            record_type = base_file
                            # BUG TODO Handle the oddball continuation records
                            # Skipping for now
                            if re.match(r"^\*", line_text):
                                continue
                        elif base_file == "FSS":
                            record_type = base_file
                            # BUG TODO Handle the oddball continuation records
                            # Skipping for now
                            if re.match(r"\*", line_text[0:4]):
                                continue
                        elif base_file == "AWOS":
                            record_type = line_text[0:5].strip()
                        elif base_file in [
                            "AFF",
                            "ANR",
                            "ATS",
                            "AWY",
                            "FIX",
                            "HPF",
                            "ILS",
                            "MTR",
                            "NAV",
                            "PJA",
                            "PFR",
                            "TWR",
                        ]:
                            record_type = line_text[0:4].strip()
                        elif base_file == "APT":
                            record_type = line_text[0:3].strip()
                        elif base_file == "LID":
                            record_type = line_text[0:1].strip()
                        else:
                            print(
                                f"Could not recognize {base_file} while reading {data_file}."
                            )

                        if record_type not in definitions.get(base_file, {}):
                            print(
                                f"{data_file} line #{line_num}: No parser defined for this recordType: {record_type}"
                            )
                            continue

                        parsed_line = LineParser(base_file, record_type, line_text)
                        line_dict = parsed_line.line_dict

                        if len(line_text) > parsed_line.line_length:
                            print(
                                f"Line # {line_num} - {line_text}"
                                f"Bad parse for {record_type}: Expected {parsed_line.line_length} characters, but read {len(line_text)}."
                            )
                            continue

                        normalizer_statements = parse_normalizer(
                            base_file, record_type, line_dict
                        )
                        if (
                            normalizer_statements != None
                            and len(normalizer_statements) > 0
                        ):
                            for statement in normalizer_statements:
                                dbc.execute(statement)
                                dbh.commit()

                        if should_expand:
                            expanded_dict = parse_expander(
                                base_file, record_type, line_dict
                            )

                            if expanded_dict != None:
                                line_dict = expanded_dict

                        if should_create_geometry:
                            geometry_dict = parse_geometry(
                                base_file, record_type, line_dict
                            )

                            if geometry_dict != None:
                                line_dict = geometry_dict

                        blanks_to_remove = [key for key in line_dict if "blank" in key]
                        for key in blanks_to_remove:
                            del line_dict[key]

                        if not f"{base_file}_{record_type}" in have_created_table:
                            # Drop existing table
                            dbc.execute(
                                f"DROP TABLE IF EXISTS {base_file}_{record_type};"
                            )

                            # Makes a "CREATE TABLE" statement based on the keys of the hash, columns sorted alphabetically
                            # Include the master_record_row_id as an explicit foreign key to master record
                            # The inclusion of " NONE," here is  to force sqlite to not assign affinity to columns, since that is making it "TEXT" by default
                            sorted_keys = sorted(
                                line_dict.keys(), key=lambda x: x.lower()
                            )
                            columns = " NONE,".join(sorted_keys) + " NONE"
                            create_stmt = f"CREATE TABLE {base_file}_{record_type} (_id INTEGER PRIMARY KEY AUTOINCREMENT, master_record_row_id INTEGER, {columns})"
                            # Create the table
                            dbc.execute(create_stmt)

                            # Mark it as created
                            have_created_table.append(f"{base_file}_{record_type}")

                            # Master records have a master_record_row_id of 0
                        if re.search(r"1$", record_type) or re.search(
                            r"^APT$", record_type
                        ):
                            master_record_row_id = 0

                        # -------------------
                        # Make an "INSERT INTO" statement based on the keys and values of the hash
                        # Include the master_record_row_id as an explicit foreign key to master record
                        columns = "master_record_row_id," + ",".join(line_dict.keys())
                        placeholders = ",".join(["?"] * len(line_dict))
                        insert_statement = f"INSERT INTO {base_file}_{record_type} ({columns}) VALUES (?,{placeholders})"

                        values = [master_record_row_id] + list(line_dict.values())
                        dbc.execute(insert_statement, values)

                        # If we just inserted a master record
                        # Then get and save its rowId to be used as a foreign key for its child records
                        if re.search(r"1$", record_type) or re.search(
                            r"^APT$", record_type
                        ):
                            master_record_row_id = dbc.lastrowid

            except FileNotFoundError:
                # Handle the case where the file does not exist
                print(f"The file '{data_file}' does not exist.")
                continue

            finally:
                print(f"Committing records from {base_file} for {record_type}.")
                dbh.commit()

        print("Closing cursor.")
        dbc.close()


def create_databases(
    source_path: str,
    output_directory: str,
    should_expand: bool = False,
    should_create_geometry: bool = False,
) -> None:
    # Names of output files
    nasr_database_path = f"{output_directory}/nasr.sqlite"
    nasr_spatialite_database_path = f"{output_directory}/spatialite_nasr.sqlite"
    sua_spatialite_database_path = (
        f"{output_directory}/special_use_airspace_spatialite.sqlite"
    )
    controlled_airspace_spatialite_database_path = (
        f"{output_directory}/controlled_airspace_spatialite.sqlite"
    )

    # Create NASR Database
    create_nasr_database(
        source_path, nasr_database_path, should_expand, should_create_geometry
    )
    add_indexes(nasr_database_path, "./sql/add_indexes.sql")
    # Create Spatialite Databases
    if should_create_geometry:
        result = create_spatialite(
            nasr_database_path,
            nasr_spatialite_database_path,
            "./sql/sqlite_to_spatialite.sql",
        )
        if result:
            print(
                "---------- Converting controlled and special use airspaces into spatialite databases"
            )
            sua_input_directory = next(
                (
                    os.path.join(source_path, subdir)
                    for subdir in os.listdir(source_path)
                    if os.path.isdir(os.path.join(source_path, subdir))
                    and "Saa_Sub_File" in subdir
                ),
                None,
            )

            if sua_input_directory == None:
                print(
                    f"No Special Use Airspace information found in {sua_input_directory}"
                )
            else:
                xml_to_spatialite(sua_spatialite_database_path, sua_input_directory)

            # Find directories containing Shape_Files
            controlled_airspace_input_directory = next(
                (
                    os.path.join(source_path, subdir)
                    for subdir in os.listdir(source_path)
                    if os.path.isdir(os.path.join(source_path, subdir))
                    and "Shape_Files" in subdir
                ),
                None,
            )

            if controlled_airspace_input_directory == None:
                print(
                    f"No Controlled Airspace information found in {controlled_airspace_input_directory}"
                )
            else:
                xml_to_spatialite(
                    controlled_airspace_spatialite_database_path,
                    controlled_airspace_input_directory,
                )


def add_indexes(nasr_database_path: str, indexes_sql_path: str) -> None:
    print("---------- Adding indexes")
    with sqlite3.connect(nasr_database_path) as dbh:
        dbc = dbh.cursor()

        try:
            with open(indexes_sql_path) as s:
                indexes_sql = s.read()

            dbc.executescript(indexes_sql)
            dbh.commit()

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            dbh.rollback()

        finally:
            dbc.close()
    print("           Complete")


def create_spatialite(
    nasr_database_path: str, spatialite_database_path: str, spatilite_sql_path: str
) -> bool:
    print("---------- Creating the spatialite version of the database")
    result = False

    with sqlite3.connect(nasr_database_path) as src_conn:
        with sqlite3.connect(spatialite_database_path) as dest_conn:
            src_conn.backup(dest_conn)

    with sqlite3.connect(spatialite_database_path) as dbh:

        try:
            with open(spatilite_sql_path) as s:
                spatialite_sql = s.read()

            dbh.enable_load_extension(True)
            dbh.executescript(spatialite_sql)
            dbh.commit()

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            dbh.rollback()

        finally:
            result = True
            print("           Complete")

    return result


def xml_to_spatialite(spatialite_database_path: str, input_directory: str) -> None:
    os.environ["GML_FETCH_ALL_GEOMETRIES"] = "YES"
    os.environ["GML_SKIP_RESOLVE_ELEMS"] = "NONE"

    xml_files = glob.glob(f"{input_directory}/**/*.xml", recursive=True)

    # Command template for ogr2ogr
    ogr2ogr_cmd_template = [
        "ogr2ogr",
        "-f",
        "SQLite",
        spatialite_database_path,
        "",
        "-explodecollections",
        "-a_srs",
        "WGS84",
        "-update",
        "-append",
        "-wrapdateline",
        "-fieldTypeToString",
        "ALL",
        "-dsco",
        "SPATIALITE=YES",
        "-skipfailures",
        "-lco",
        "SPATIAL_INDEX=YES",
        "-lco",
        "LAUNDER=NO",
        "--config",
        "OGR_SQLITE_SYNCHRONOUS",
        "OFF",
        "--config",
        "OGR_SQLITE_CACHE",
        "128",
        "-gt",
        "65536",
    ]

    # Execute ogr2ogr for each XML file
    for xml_file in xml_files:
        print(f"Processing file: {xml_file}")
        cmd = ogr2ogr_cmd_template[:]
        cmd[4] = xml_file
        subprocess.run(cmd, check=True)

    # Vacuum the database
    vacuum_cmd = ["ogrinfo", spatialite_database_path, "-sql", "VACUUM"]
    subprocess.run(vacuum_cmd, check=True)

    print("Conversion complete.")
