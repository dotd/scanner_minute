import logging
from ScannerMinute.src import logging_utils
from ScannerMinute.src import aws_utils


def tst_manage_instances(region="us-east-1"):
    logging_utils.setup_logging(log_level="INFO", include_time=True)
    result = aws_utils.manage_instances(region=region)
    logging.info(f"Result: {result}")
    return result


if __name__ == "__main__":
    tst_manage_instances()
