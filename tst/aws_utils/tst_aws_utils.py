import logging
from ScannerMinute.src import logging_utils
from ScannerMinute.src.aws_utils import (
    check_aws_connection,
    list_images,
    launch_instance,
    create_key_pair,
    create_security_group,
    list_instance_types,
    list_running_instances,
)


def tst_aws_connection():
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    logging.info("Testing AWS connection...")
    info = check_aws_connection()

    if info:
        logging.info(f"Account ID: {info['account_id']}")
        logging.info(f"ARN:        {info['arn']}")
        logging.info(f"User ID:    {info['user_id']}")
    else:
        logging.error("AWS connection test failed.")


def tst_list_images(region="us-east-1"):
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    # List your own AMIs
    logging.info(f"Listing own AMIs in {region}...")
    own_images = list_images(region=region, owners=["self"])
    logging.info(f"Own AMIs: {len(own_images)}")

    # List recent Amazon Linux 2023 AMIs
    logging.info(f"Listing Amazon Linux 2023 AMIs in {region}...")
    amazon_images = list_images(
        region=region,
        owners=["amazon"],
        filters=[
            {"Name": "name", "Values": ["al2023-ami-2023*-x86_64"]},
            {"Name": "state", "Values": ["available"]},
        ],
    )

    # List recent Ubuntu 22.04 AMIs
    logging.info(f"Listing Ubuntu 22.04 AMIs in {region}...")
    ubuntu_images = list_images(
        region=region,
        owners=["099720109477"],  # Canonical
        filters=[
            {
                "Name": "name",
                "Values": ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"],
            },
            {"Name": "state", "Values": ["available"]},
        ],
    )

    return own_images, amazon_images, ubuntu_images


def tst_launch_instance(
    image_id="ami-02dfbd4ff395f2a1b",
    instance_type="t2.micro",
    key_name="scanner-minute-key",
    region="us-east-1",
):
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    logging.info(f"Launching instance from {image_id}...")
    instance_ids = launch_instance(
        image_id=image_id,
        instance_type=instance_type,
        key_name=key_name,
        region=region,
    )
    logging.info(f"Launched: {instance_ids}")
    return instance_ids


def tst_create_key_pair():
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    pem_path = create_key_pair()

    if pem_path:
        logging.info(f"Key pair saved to: {pem_path}")
    else:
        logging.error("Failed to create key pair.")
    return pem_path


def tst_create_security_group():
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    logging.info("Creating security group...")
    sg_id = create_security_group()

    if sg_id:
        logging.info(f"Security group ID: {sg_id}")
    else:
        logging.error("Failed to create security group.")
    return sg_id


def tst_list_instance_types(region="us-east-1"):
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    # List only t2 and t3 families to keep output short
    logging.info(f"Listing t2/t3 instance types in {region}...")
    types = list_instance_types(
        region=region,
        filters=[{"Name": "instance-type", "Values": ["t2.*", "t3.*"]}],
    )
    logging.info(f"Found {len(types)} instance types.")
    return types


def tst_list_running_instances(region="us-east-1"):
    import json

    logging_utils.setup_logging(log_level="INFO", include_time=True)

    logging.info(f"Listing running instances in {region}...")
    instances = list_running_instances(region=region)
    logging.info(f"Total running: {len(instances)}")
    for inst in instances:
        logging.info(f"\n{json.dumps(inst, indent=2)}")
    return instances


if __name__ == "__main__":
    # tst_aws_connection()
    # tst_list_images()
    # tst_create_key_pair()
    # tst_create_security_group()
    # tst_list_instance_types()
    # tst_launch_instance()
    tst_list_running_instances()
