import re


def parse_normalizer(base_file: str, record_type: str, line_dict: dict) -> list | None:
    result = None
    base_group = parsers.get(base_file)
    if base_group != None:
        function_reference = base_group.get(record_type)
        if function_reference != None:
            result = function_reference(base_file, record_type, line_dict)

    return result


def normalize_TWR_TWR3(
    base_file: str,
    record_type: str,
    line_dict: dict,
) -> list:
    new_table = {}
    have_created_this_table = False
    result = []

    new_record_type = f"{record_type}A"

    for i in range(1, 10):
        key = f"frequencys_for_master_airport_use_only_and_sectorization_{i}"
        frequency = line_dict.get(key)
        key = f"frequencys_for_master_airport_use_only_and_sectorization_not_{i}"
        frequency_not_truncated = line_dict.get(key)
        key = f"frequency_use_{i}"
        frequency_use = line_dict.get(key)
        terminal_communications_facility_identifier = line_dict.get(
            "terminal_communications_facility_identifier"
        )

        if frequency != None:
            new_table["frequency"] = frequency

        if frequency_not_truncated != None:
            new_table["frequency_not_truncated"] = frequency_not_truncated
            split_result = re.split(r"\s|\(", frequency_not_truncated, 1)
            freq = split_result[0]
            sector = split_result[1] if len(split_result) > 1 else ""
            sector = sector.replace("(", "").replace(")", "")
            sector = re.sub(r"(\d{3}-\d{3})", r"(\1)", sector)

            new_table["freq"] = freq
            new_table["sector"] = sector

        if frequency_use != None:
            new_table["frequency_use"] = frequency_use

        if terminal_communications_facility_identifier != None:
            new_table["terminal_communications_facility_identifier"] = (
                terminal_communications_facility_identifier
            )

        # Create the table for each recordType if we haven't already
        # uses all the sorted keys in the hash as column names
        if not (have_created_this_table):
            # Drop existing table
            drop_statement = f"DROP TABLE IF EXISTS {base_file}_{new_record_type}"
            result.append(drop_statement)

            # Makes a "CREATE TABLE" statement based on the keys of the hash, columns sorted alphabetically
            sorted_keys = sorted(new_table.keys(), key=lambda x: x.lower())
            joined_keys = ", ".join(sorted_keys)
            create_statement = f"CREATE TABLE {base_file}_{new_record_type} (_id INTEGER PRIMARY KEY AUTOINCREMENT, master_record_row_id INTEGER, {joined_keys})"
            result.append(create_statement)

            # Mark it as created
            have_created_this_table = True

        # Only add a row if frequency is defined
        if frequency:
            joined_keys = ", ".join(new_table.keys())
            values = ", ".join(
                [
                    f"'{str(value)}'" if isinstance(value, str) else str(value)
                    for value in new_table.values()
                ]
            )
            insert_statement = f"INSERT INTO {base_file}_{new_record_type} ({joined_keys}) VALUES ({values})"
            result.append(insert_statement)

    return result


parsers = {
    # "APT": {"APT1": normalize_APT_APT1_Fuel},
    # "COM": {"COM": normalize_COM_COM_Frequencies},
    # "FSS": {"FSS": normalize_FSS_FSS_Data},
    # "NAV": {"NAV": normalize_NAV_NAV_Data},
    "TWR": {"TWR3": normalize_TWR_TWR3},
    # "ANR": {"ANR2": normalize_ANR_ANR2},
}
