import argparse
import logging
import os
from pathlib import Path
import time


class ColorHandler(logging.StreamHandler):
    # https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
    GRAY8 = "38;5;8"
    GRAY7 = "38;5;7"
    ORANGE = "33"
    RED = "31"
    WHITE = "37"

    def emit(self, record):
        # We don't use white for any logging, to help distinguish from user print statements
        level_color_map = {
            logging.DEBUG: self.GRAY8,
            logging.INFO: self.WHITE,
            logging.WARNING: self.ORANGE,
            logging.ERROR: self.RED,
        }

        csi = f"{chr(27)}["  # control sequence introducer
        color = level_color_map.get(record.levelno, self.WHITE)

        self.stream.write(f"{csi}{color}m{record.msg}{csi}m\n")
        # self.stream.write(f"%(levelname)s: %(message)s")


def setup_logging(log_level="INFO", log_folder=None, include_time=False):
    date_time_str = time.strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_folder}/log_{date_time_str}.log"
    os.makedirs(f"{log_folder}") if not os.path.exists(f"{log_folder}") else None
    log_format = (
        "%(asctime)s %(levelname)s: %(message)s"
        if include_time
        else "%(levelname)s: %(message)s"
    )
    logging.basicConfig(
        level=getattr(logging, log_level),
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        format=log_format,
    )
    logging.info(f"log_file:  {log_file} log_level: {log_level}")
    return log_file


def prepare_parameters_and_logging(
    log_level="INFO",
    log_folder="./logs/",
    arguments=None,
    skip_main_to_screen=True,
):
    parser = argparse.ArgumentParser()
    parser.add_argument("--signature", type=str, default=time.strftime("%Y%m%d_%H%M%S"))
    if arguments is not None:
        for arg in arguments:
            # arg[0] is the name of the argument
            # arg[1] is the type of the argument
            # arg[2] is the default value of the argument
            parser.add_argument(arg[0], type=arg[1], default=arg[2])
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=log_level,
        help="Set the logging level (default is DEBUG).",
    )

    args = parser.parse_args()
    filename_full = os.path.abspath(__file__)
    filename = filename_full.split("/")[-1].split(".")[0]
    log_file = f"{log_folder}/{filename}_{args.signature}.log"
    log_file_latest = f"{log_folder}/{filename}.log"
    os.makedirs(f"{log_folder}") if not os.path.exists(f"{log_folder}") else None
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        handlers=[
            logging.FileHandler(log_file),
            logging.FileHandler(log_file_latest),
            logging.StreamHandler(),
        ],
        format="%(levelname)s: %(message)s",
    )

    # Put into logging the file itself for debugging
    logging.info(f"Running file: {filename_full}")
    logging.info(f"log_file: {log_file}, skip_main_to_screen={skip_main_to_screen}")
    if not skip_main_to_screen:
        logging.info(f"Log file:\n{'start_of_running_file'.upper()}\n")
        f"{Path(filename_full).read_text()}\n{'end_of_running_file'.upper()}"

    for arg, value in vars(args).items():
        logging.info(f"{__file__.split('/')[-1]}> {arg}: {value}")
    return args


def tst_prepare_parameters_and_logging():
    args = prepare_parameters_and_logging(
        log_level="DEBUG",
        log_folder="./logs/",
        arguments=[
            ("--qqq1", str, "default_qqq1_value"),
            ("--qqq2", str, "default_qqq2_value"),
            ("--qqq3", str, "default_qqq3_value"),
            ("--qqq4", str, "default_qqq4_value"),
            ("--qqq5", str, "default_qqq5_value"),
            ("--qqq6", str, "default_qqq6_value"),
            ("--qqq7", str, "default_qqq7_value"),
            ("--qqq8", str, "default_qqq8_value"),
        ],
    )
    print(args)


if __name__ == "__main__":
    tst_prepare_parameters_and_logging()
