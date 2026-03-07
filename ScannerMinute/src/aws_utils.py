import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from ScannerMinute.definitions import PROJECT_ROOT_DIR


def check_aws_connection():
    """
    Verify that AWS credentials are configured and working.
    Creates a boto3 session, checks the caller identity via STS,
    and logs the account info.

    Returns:
        dict with keys: account_id, arn, user_id — or None if connection failed.
    """
    try:
        session = boto3.Session()
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        info = {
            "account_id": identity["Account"],
            "arn": identity["Arn"],
            "user_id": identity["UserId"],
        }
        logging.info(
            f"AWS connection OK | account={info['account_id']} | arn={info['arn']}"
        )
        return info
    except NoCredentialsError:
        logging.error(
            "AWS credentials not found. Configure via aws configure, env vars, or IAM role."
        )
        return None
    except (BotoCoreError, ClientError) as e:
        logging.error(f"AWS connection failed: {e}")
        return None


def list_images(region="us-east-1", owners=None, filters=None):
    """
    List available AMIs.

    Parameters:
        region: str — AWS region
        owners: list[str] — e.g. ["self"], ["amazon"], ["099720109477"] (Ubuntu).
                Defaults to ["self"] (your own AMIs).
        filters: list[dict] — additional filters, e.g.
                [{"Name": "name", "Values": ["*ubuntu*"]}]

    Returns:
        list of dicts with keys: image_id, name, state, creation_date, description
    """
    if owners is None:
        owners = ["self"]
    if filters is None:
        filters = []

    ec2 = boto3.client("ec2", region_name=region)
    response = ec2.describe_images(Owners=owners, Filters=filters)

    images = []
    for img in response["Images"]:
        images.append(
            {
                "image_id": img["ImageId"],
                "name": img.get("Name", ""),
                "state": img.get("State", ""),
                "creation_date": img.get("CreationDate", ""),
                "description": img.get("Description", ""),
            }
        )

    images.sort(key=lambda x: x["creation_date"], reverse=True)
    logging.info(f"Found {len(images)} AMIs in {region}")
    for img in images:
        logging.info(
            f"  {img['image_id']} | {img['name']} | {img['state']} | {img['creation_date']}"
        )
    return images


def launch_instance(
    image_id,
    instance_type="t2.micro",
    key_name=None,
    security_group_ids=None,
    region="us-east-1",
    min_count=1,
    max_count=1,
    tag_name="scanner-minute",
):
    """
    Launch an EC2 instance from a given AMI.

    Parameters:
        image_id: str — AMI ID to launch
        instance_type: str — e.g. "t2.micro", "t3.medium"
        key_name: str — SSH key pair name (optional)
        security_group_ids: list[str] — security group IDs (optional)
        region: str — AWS region
        min_count: int — minimum number of instances
        max_count: int — maximum number of instances
        tag_name: str — Name tag for the instance

    Returns:
        list of instance IDs launched
    """
    ec2 = boto3.client("ec2", region_name=region)

    kwargs = {
        "ImageId": image_id,
        "InstanceType": instance_type,
        "MinCount": min_count,
        "MaxCount": max_count,
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": tag_name}],
            }
        ],
    }
    if key_name:
        kwargs["KeyName"] = key_name
    if security_group_ids:
        kwargs["SecurityGroupIds"] = security_group_ids

    response = ec2.run_instances(**kwargs)

    instance_ids = [inst["InstanceId"] for inst in response["Instances"]]
    logging.info(
        f"Launched {len(instance_ids)} instance(s) from {image_id}: {instance_ids}"
    )
    return instance_ids


def create_key_pair(
    key_name="scanner-minute-key",
    region="us-east-1",
    save_dir=f"{PROJECT_ROOT_DIR}/api_keys",
):
    """
    Create an EC2 key pair for SSH access and save the private key to disk.

    Parameters:
        key_name: str — name for the key pair
        region: str — AWS region
        save_dir: str — directory to save the .pem file

    Returns:
        str — path to the saved .pem file, or None if creation failed
    """
    import os
    import stat

    ec2 = boto3.client("ec2", region_name=region)

    try:
        response = ec2.create_key_pair(KeyName=key_name, KeyType="rsa", KeyFormat="pem")
    except ClientError as e:
        logging.error(f"Failed to create key pair '{key_name}': {e}")
        return None

    os.makedirs(save_dir, exist_ok=True)
    pem_path = os.path.join(save_dir, f"{key_name}.pem")
    with open(pem_path, "w") as f:
        f.write(response["KeyMaterial"])

    # chmod 400 so SSH accepts the key
    os.chmod(pem_path, stat.S_IRUSR)

    logging.info(
        f"Created key pair '{key_name}' | fingerprint={response['KeyFingerprint']} | saved to {pem_path}"
    )
    return pem_path
