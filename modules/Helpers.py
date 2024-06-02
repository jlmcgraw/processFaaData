import re

MIN_IN_DEGREE = 60
SEC_IN_MIN = 60


# Trim leading and trailing spaces. Remove duplicate internal spaces. Optionally, return as list on spaces.
def trim(input: str, as_array: bool = False) -> list | str:
    result = re.sub(r"\s+", " ", input.strip())
    if as_array:
        return result.split(" ")
    return result


# Right trim function to remove trailing whitespace
def rtrim(input: str) -> str:
    return input.rstrip()


# Left trim function to remove leading whitespace
def ltrim(input: str) -> str:
    return input.lstrip()


# Python version of ATOI with error handling
def atoi(input: str) -> int | bool:
    result = False
    try:
        result = int(input)
    except ValueError as e:
        print(f"Error converting '{input}': {e}")
    return result


def atof(input: str) -> float | bool:
    result = False
    try:
        result = float(input)
    except ValueError as e:
        print(f"Error converting '{input}: {e}")
    return result


# Check if frequency is in VHF range
def is_vhf(frequency_string: str) -> bool:
    if re.match(frequency_string, r"1[1-3][0-9]\.\d{1,3}"):
        integer_value = int(frequency_string[:3])
        if integer_value >= 118 and integer_value < 137:
            return True
    return False


# Takes DMS numeric values and returns D.d
def dmsToDecimal(
    degree: int, minute: int, second: float, declination: str
) -> float | bool:
    result = False

    if declination not in ["N", "S", "E", "W"]:
        return result

    deg = degree
    min = minute / MIN_IN_DEGREE
    sec = second / (MIN_IN_DEGREE * SEC_IN_MIN)

    result = deg + min + sec

    if declination in ["N", "S"] and result > 90:
        return result

    if declination in ["E", "W"] and result > 180:
        return result

    if declination in ["W", "S"]:
        result = -(result)

    return result


# Takes a DMS string and returns D.d
def dmsStringToDecimal(coordinate_string: str) -> float | bool:
    result = False
    # Remove any whitespace
    coordinate = coordinate_string.strip()

    pattern = r"^\s*(\d+)-(\d+)-([\d.]+)\s*([NESW])\s*$"
    match = re.match(pattern, coordinate, re.IGNORECASE)

    if match:
        deg = match.group(1)
        deg = atoi(deg)
        if not deg:
            deg = None

        min = match.group(2)
        min = atoi(min)
        if not min:
            min = None

        sec = match.group(3)
        sec = atof(sec)
        if not sec:
            sec = None

        declination = match.group(4)

        if any(var is None for var in (deg, min, sec, declination)):
            return False

        result = dmsToDecimal(deg, min, sec, declination)
    return result


# This is an alias of the above to work with the translated code.
def coordinateToDecimal(coordinate_string: str) -> float | bool:
    return dmsStringToDecimal(coordinate_string)


# This is an alias of dmsToDecimal to work with the translated code.
def coordinateToDecimal2(
    degree: str, minute: str, second: str, declination: str
) -> float | bool:
    deg = atoi(degree)
    if not deg:
        deg = None

    min = atoi(minute)
    if not min:
        min = None

    sec = atof(second)
    if not sec:
        sec = None

    if any(var is None for var in (deg, min, sec, declination)):
        return False

    return dmsToDecimal(deg, min, sec, declination)


# This is an alias of dmsStringToDecimal to work with the translated code.
def coordinateToDecimal3(coordinate_string: str) -> float | bool:
    return dmsStringToDecimal(coordinate_string)


# Convert a latitude or longitude in CIFP format to its decimal equivalent
def coordinateToDecimalCifpFormat(cifp_string: str) -> float | bool:
    declination = cifp_string[:1]
    degrees = None
    minutes = None
    seconds = None

    if declination in ["N", "S"]:
        degrees = int(cifp_string[1:3])
        minutes = int(cifp_string[3:5])
        seconds = int(cifp_string[5:]) / 100

    if declination in ["E", "W"]:
        degrees = int(cifp_string[1:4])
        minutes = int(cifp_string[4:6])
        seconds = int(cifp_string[6:]) / 100

    return dmsToDecimal(degrees, minutes, seconds, declination)
