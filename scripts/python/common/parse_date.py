import re
import sys
from datetime import datetime


# A utility that parses common date formats in Hadoop data
def parse_date(date_str) -> str:
    # Unifies date patterns like the ones below into "%Y-%m-%d"
    # partition_name: stdt=20231016 -> date: 2023-10-16
    # partition_name: wdt=2024-06-2 -> date: 2024-06-02
    # partition_name: dt=2024-06-25 -> date: 2024-06-25
    # partition_name: dt=202404     -> date: 2024-04-01
    patterns = [
        (r"^\d{4}-\d{2}-\d{2}$", "%Y-%m-%d"),  # YYYY-MM-DD
        (r"^\d{6}$", "%Y%m"),                 # YYYYMM
        (r"^\d{8}$", "%Y%m%d"),               # YYYYMMDD
        (r"^\d{4}-\d{2}-\d{1}$", "%Y-%m-%d")  # YYYY-MM-D
    ]

    for pattern, date_format in patterns:
        if re.match(pattern, date_str):
            try:
                return datetime.strptime(date_str, date_format).strftime("%Y-%m-%d")
            except ValueError:
                pass

    raise ValueError(f"Unrecognized date format: {date_str}")


if __name__ == '__main__':
    file_name = sys.argv[1]

    output_file_name = f"{file_name}-parsed.csv"
    with open(output_file_name, mode="w") as output_file:
        with open(file_name, mode="r") as f:
            lines = f.readlines()
            for line in lines:
                s_lines = line.split(sep=",")
                if len(s_lines) == 3:
                    date = s_lines[0].strip()
                    parsed_date = date
                    try:
                        parsed_date = parse_date(date)
                    except ValueError:
                        print(f"Unable to parse date {date}")
                    partition_name = s_lines[1].strip()
                    partition_size = s_lines[2].strip()
                    output_file.write(f"{parsed_date},{partition_name},{partition_size}\n")

