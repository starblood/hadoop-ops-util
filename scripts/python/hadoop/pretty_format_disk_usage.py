import sys


def bytes_to_human_readable(num_bytes):
    """
    Converts a given number of bytes into a human-readable unit (KB, MB, GB, TB, PB).
    :param num_bytes: Number in bytes
    :return: Human-readable string representation
    """
    # Define units
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = num_bytes
    unit_index = 0

    # Divide the size by changing units
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1

    # Display up to two decimal places
    return f"{size:.2f} {units[unit_index]}"


def main(file_name: str):
    total_size = 0
    with open("disk_usage.csv", mode='w') as outfile:
        outfile.write(f"date,size,size_h,accumulated_size,accumulated_size_h\n")
        with open(file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                s_lines = line.split(',')
                if len(s_lines) == 2:
                    date = s_lines[0]
                    size = int(s_lines[1])
                    total_size += size
                    size_h = bytes_to_human_readable(size)
                    total_size_h = bytes_to_human_readable(total_size)
                    outfile.write(f"{date},{size},{size_h},{total_size},{total_size_h}\n")


if __name__ == '__main__':
    file_name = sys.argv[1]
    main(file_name)
