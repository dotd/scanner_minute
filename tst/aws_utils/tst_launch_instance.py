import logging
from ScannerMinute.src import logging_utils
from ScannerMinute.src import aws_utils


def tst_launch_instance(
    image_id="ami-02dfbd4ff395f2a1b",  # Ubuntu ami-0b6c6ebed2801a5cb
    instance_type="t2.micro",
    key_name="scanner-minute-key",
    region="us-east-1",
    disk_size_gb=None,
):
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    logging.info(f"Launching instance from {image_id}...")
    instance_ids = aws_utils.launch_instance(
        image_id=image_id,
        instance_type=instance_type,
        key_name=key_name,
        region=region,
        disk_size_gb=disk_size_gb,
    )
    logging.info(f"Launched: {instance_ids}")
    return instance_ids


if __name__ == "__main__":
    # tst_aws_connection()
    # tst_list_images()
    # tst_create_key_pair()
    # tst_create_security_group()
    # tst_list_instance_types()
    tst_launch_instance(disk_size_gb=None)
    # tst_list_running_instances()
    # tst_manage_instances()
