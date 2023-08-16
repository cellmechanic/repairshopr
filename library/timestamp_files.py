"""make timestamp files"""
import time


def update_last_ran(timestamp_file):
    """update the last time the script ran file, pass in the file"""
    try:
        with open(timestamp_file, "w", encoding="utf-8") as file:
            file.write(str(int(time.time())))
        print("Timestamp updated successfully to: ", int(time.time()))
    except IOError as error:
        print(f"Error updating timestamp: {error}")


def check_last_ran(timestamp_file):
    """check the passed timestamp file to see the last time this ran"""
    try:
        with open(timestamp_file, "r", encoding="utf-8") as file:
            last_run_timestamp_str = file.read().strip()
            if last_run_timestamp_str:
                last_run_timestamp = int(last_run_timestamp_str)
                print(f"Last ran @ {last_run_timestamp_str} in unix")
                last_run_time_str = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(last_run_timestamp)
                )
                print(f"Last ran @ {last_run_time_str}")
                return int(last_run_timestamp_str)
            else:
                print("no timestamp found, using 0 as time")
                return int(0)
    except FileNotFoundError:
        print("no timestamp file found, using 0 as time, assuming rebuild")
        return 0
