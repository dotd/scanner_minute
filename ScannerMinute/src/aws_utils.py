import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


from ScannerMinute.definitions import PROJECT_ROOT_DIR

INSTANCE_TYPE_VALUES = ["t2.*", "t3.*", "p3*", "p4*", "g4*", "g5*"]


INSTALL_SCRIPT = """
# Update packages
sudo apt update

# Install Node.js 20.x (LTS)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Verify

########################################################
# Download the latest Miniconda installer
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# Run installer with all defaults accepted automatically
bash Miniconda3-latest-Linux-x86_64.sh -b

# Initialize conda for your shell
~/miniconda3/bin/conda init bash

# Reload shell config
source ~/.bashrc

# Verify
conda --version
node --version
npm --version
"""


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
            f"[check_aws_connection] AWS connection OK | account={info['account_id']} | arn={info['arn']}"
        )
        return info
    except NoCredentialsError:
        logging.error(
            "[check_aws_connection] AWS credentials not found. Configure via aws configure, env vars, or IAM role."
        )
        return None
    except (BotoCoreError, ClientError) as e:
        logging.error(f"[check_aws_connection] AWS connection failed: {e}")
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
    logging.info(f"[list_images] Found {len(images)} AMIs in {region}")
    for img in images:
        logging.info(
            f"[list_images]   {img['image_id']} | {img['name']} | {img['state']} | {img['creation_date']}"
        )
    return images


def launch_instance(
    image_id,
    instance_type="t2.micro",
    key_name=None,
    security_group_ids=["scanner-minute-sg"],
    region="us-east-1",
    min_count=1,
    max_count=1,
    suffix=None,
    disk_size_gb=None,
    volume_type="gp3",
):
    """
    Launch an EC2 instance from a given AMI.

    The instance Name tag is auto-generated as:
        scanner-minute_YYYYMMDD_HHMMSS          (if suffix is None)
        scanner-minute_YYYYMMDD_HHMMSS_<suffix>  (if suffix is provided)
    using the local time at launch.

    Parameters:
        image_id: str — AMI ID to launch
        instance_type: str — e.g. "t2.micro", "t3.medium"
        key_name: str — SSH key pair name (optional)
        security_group_ids: list[str] — security group IDs (optional)
        region: str — AWS region
        min_count: int — minimum number of instances
        max_count: int — maximum number of instances
        suffix: str — optional user-defined string appended to the Name tag
        disk_size_gb: int — root volume size in GiB (None = AMI default, typically 8 GiB)
        volume_type: str — EBS volume type, e.g. "gp3", "gp2", "io1" (default "gp3")

    Returns:
        list of dicts with keys: instance_id, public_ip, name
    """
    from datetime import datetime

    ec2 = boto3.client("ec2", region_name=region)

    tag_name = f"scanner-minute_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if suffix:
        tag_name = f"{tag_name}_{suffix}"

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
    if disk_size_gb is not None:
        kwargs["BlockDeviceMappings"] = [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "VolumeSize": disk_size_gb,
                    "VolumeType": volume_type,
                    "DeleteOnTermination": True,
                },
            }
        ]

    response = ec2.run_instances(**kwargs)

    instance_ids = [inst["InstanceId"] for inst in response["Instances"]]
    logging.info(
        f"[launch_instance] Launched {len(instance_ids)} instance(s) from {image_id}: {instance_ids} | name={tag_name}"
    )

    # Wait for instances to be running and get public IPs
    logging.info("[launch_instance] Waiting for instances to enter running state...")
    waiter = ec2.get_waiter("instance_running")
    waiter.wait(InstanceIds=instance_ids)

    desc = ec2.describe_instances(InstanceIds=instance_ids)
    results = []
    for reservation in desc["Reservations"]:
        for inst in reservation["Instances"]:
            info = {
                "instance_id": inst["InstanceId"],
                "public_ip": inst.get("PublicIpAddress"),
                "name": tag_name,
            }
            results.append(info)
            logging.info(
                f"[launch_instance]   {info['instance_id']} | {info['name']} | public_ip={info['public_ip']}"
            )
            if info["public_ip"] and key_name:
                pem_path = f"api_keys/{key_name}.pem"
                logging.info(
                    f"[launch_instance]   SSH: ssh -i {pem_path} ec2-user@{info['public_ip']}"
                )

    return results


def run_command_on_instance(
    instance_id, command, region="us-east-1", timeout_seconds=60
):
    """
    Run a shell command on an EC2 instance via SSM Run Command and return the output.

    Requires the SSM Agent to be running on the instance and the instance to have
    an IAM role with AmazonSSMManagedInstanceCore policy attached.

    Parameters:
        instance_id: str — EC2 instance ID
        command: str — shell command to execute
        region: str — AWS region
        timeout_seconds: int — max seconds to wait for the command to finish

    Returns:
        dict with keys: status, stdout, stderr
    """
    import time

    ssm = boto3.client("ssm", region_name=region)

    response = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellCommand",
        Parameters={"commands": [command]},
        TimeoutSeconds=timeout_seconds,
    )
    command_id = response["Command"]["CommandId"]
    logging.info(
        f"[run_command] Sent command '{command}' to {instance_id} | command_id={command_id}"
    )

    # Poll for completion
    for _ in range(timeout_seconds):
        time.sleep(1)
        try:
            result = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id,
            )
        except ssm.exceptions.InvocationDoesNotExist:
            continue

        if result["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            output = {
                "status": result["Status"],
                "stdout": result.get("StandardOutputContent", ""),
                "stderr": result.get("StandardErrorContent", ""),
            }
            logging.info(f"[run_command] Status={output['status']}")
            if output["stdout"]:
                logging.info(f"[run_command] stdout:\n{output['stdout']}")
            if output["stderr"]:
                logging.info(f"[run_command] stderr:\n{output['stderr']}")
            return output

    logging.error(f"[run_command] Timed out waiting for command {command_id}")
    return {"status": "TimedOut", "stdout": "", "stderr": ""}


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
    # If key exists, say it exists, and you need to delete it first, and just return without creating it again.
    response = ec2.describe_key_pairs(KeyNames=[key_name])
    if response["KeyPairs"]:
        logging.info(f"[create_key_pair] Key pair '{key_name}' already exists")
        return None

    try:
        response = ec2.create_key_pair(KeyName=key_name, KeyType="rsa", KeyFormat="pem")
    except ClientError as e:
        logging.error(f"[create_key_pair] Failed to create key pair '{key_name}': {e}")
        return None

    os.makedirs(save_dir, exist_ok=True)
    pem_path = os.path.join(save_dir, f"{key_name}.pem")
    with open(pem_path, "w") as f:
        f.write(response["KeyMaterial"])

    # chmod 400 so SSH accepts the key
    os.chmod(pem_path, stat.S_IRUSR)

    logging.info(
        f"[create_key_pair] Created key pair '{key_name}' | fingerprint={response['KeyFingerprint']} | saved to {pem_path}"
    )
    return pem_path


def create_security_group(
    group_name="scanner-minute-sg",
    description="Security group for scanner-minute: SSH, HTTP, HTTPS",
    region="us-east-1",
    vpc_id=None,
):
    """
    Create a security group that allows SSH (22), HTTP (80), and HTTPS (443) inbound
    from anywhere, and all outbound traffic.

    Parameters:
        group_name: str — name for the security group
        description: str — description
        region: str — AWS region
        vpc_id: str — VPC ID (if None, uses the default VPC)

    Returns:
        str — security group ID, or None if creation failed
    """
    ec2 = boto3.client("ec2", region_name=region)

    # Use default VPC if none specified
    if vpc_id is None:
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])
        if not vpcs["Vpcs"]:
            logging.error(
                "[create_security_group] No default VPC found. Specify vpc_id explicitly."
            )
            return None
        vpc_id = vpcs["Vpcs"][0]["VpcId"]

    # Check if group already exists in this VPC
    try:
        existing = ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [group_name]},
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        )
        if existing["SecurityGroups"]:
            sg_id = existing["SecurityGroups"][0]["GroupId"]
            logging.info(
                f"[create_security_group] Security group '{group_name}' already exists: {sg_id}"
            )
            return sg_id
    except ClientError as e:
        logging.error(
            f"[create_security_group] Error checking existing security groups: {e}"
        )
        return None

    # Create the security group
    try:
        response = ec2.create_security_group(
            GroupName=group_name,
            Description=description,
            VpcId=vpc_id,
        )
        sg_id = response["GroupId"]
    except ClientError as e:
        logging.error(
            f"[create_security_group] Failed to create security group '{group_name}': {e}"
        )
        return None

    # Add inbound rules: SSH, HTTP, HTTPS
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 80,
                    "ToPort": 80,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "HTTP"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "HTTPS"}],
                },
            ],
        )
    except ClientError as e:
        logging.error(
            f"[create_security_group] Failed to set ingress rules for {sg_id}: {e}"
        )

    # Tag it
    try:
        ec2.create_tags(
            Resources=[sg_id],
            Tags=[{"Key": "Name", "Value": group_name}],
        )
    except ClientError as e:
        logging.warning(
            f"[create_security_group] Failed to tag security group {sg_id}: {e}"
        )

    logging.info(
        f"[create_security_group] Created security group '{group_name}' ({sg_id}) in VPC {vpc_id} | "
        f"Inbound: SSH(22), HTTP(80), HTTPS(443)"
    )
    return sg_id


def list_instance_types(region="us-east-1", filters=None):
    """
    List available EC2 instance types with specs and on-demand pricing.

    Parameters:
        region: str — AWS region
        filters: list[dict] — optional filters for describe_instance_types, e.g.
                [{"Name": "instance-type", "Values": ["t2.*", "t3.*"]}]

    Returns:
        list of dicts sorted by vcpus then memory, with keys:
            instance_type, vcpus, memory_mib, memory_gib, storage,
            network, architecture, price_per_hour
    """
    import json

    ec2 = boto3.client("ec2", region_name=region)
    pricing = boto3.client(
        "pricing", region_name="us-east-1"
    )  # pricing API only in us-east-1

    # Collect instance type info with pagination
    kwargs = {}
    if filters:
        kwargs["Filters"] = filters

    instance_types = []
    paginator = ec2.get_paginator("describe_instance_types")
    for page in paginator.paginate(**kwargs):
        for it in page["InstanceTypes"]:
            instance_types.append(
                {
                    "instance_type": it["InstanceType"],
                    "vcpus": it["VCpuInfo"]["DefaultVCpus"],
                    "memory_mib": it["MemoryInfo"]["SizeInMiB"],
                    "memory_gib": round(it["MemoryInfo"]["SizeInMiB"] / 1024, 1),
                    "storage": it.get("InstanceStorageInfo", {}).get(
                        "TotalSizeInGB", "EBS-only"
                    ),
                    "network": it.get("NetworkInfo", {}).get("NetworkPerformance", ""),
                    "architecture": it.get("ProcessorInfo", {}).get(
                        "SupportedArchitectures", []
                    ),
                    "price_per_hour": None,
                }
            )

    # Map AWS region to pricing API location name
    region_names = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "eu-west-1": "EU (Ireland)",
        "eu-west-2": "EU (London)",
        "eu-central-1": "EU (Frankfurt)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
        "ap-northeast-1": "Asia Pacific (Tokyo)",
    }
    location = region_names.get(region)

    if location:
        for it in instance_types:
            try:
                price_response = pricing.get_products(
                    ServiceCode="AmazonEC2",
                    Filters=[
                        {
                            "Type": "TERM_MATCH",
                            "Field": "instanceType",
                            "Value": it["instance_type"],
                        },
                        {"Type": "TERM_MATCH", "Field": "location", "Value": location},
                        {
                            "Type": "TERM_MATCH",
                            "Field": "operatingSystem",
                            "Value": "Linux",
                        },
                        {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
                        {
                            "Type": "TERM_MATCH",
                            "Field": "preInstalledSw",
                            "Value": "NA",
                        },
                        {
                            "Type": "TERM_MATCH",
                            "Field": "capacitystatus",
                            "Value": "Used",
                        },
                    ],
                    MaxResults=1,
                )
                if price_response["PriceList"]:
                    price_data = json.loads(price_response["PriceList"][0])
                    on_demand = price_data["terms"]["OnDemand"]
                    for term in on_demand.values():
                        for dim in term["priceDimensions"].values():
                            it["price_per_hour"] = float(dim["pricePerUnit"]["USD"])
                            break
                        break
            except Exception:
                pass  # pricing not available for this type

    instance_types.sort(key=lambda x: (x["vcpus"], x["memory_mib"]))

    logging.info(
        f"[list_instance_types] Found {len(instance_types)} instance types in {region}"
    )
    for it in instance_types:
        price_str = (
            f"${it['price_per_hour']:.4f}/hr"
            if it["price_per_hour"] is not None
            else "N/A"
        )
        logging.info(
            f"[list_instance_types]   {it['instance_type']:20s} | {it['vcpus']:3d} vCPUs | "
            f"{it['memory_gib']:8.1f} GiB | {str(it['storage']):>12s} | "
            f"{it['network']:25s} | {price_str}"
        )
    return instance_types


def list_running_instances(region="us-east-1"):
    """
    List all running and stopped EC2 instances with key details.

    Parameters:
        region: str — AWS region

    Returns:
        list of dicts with keys: instance_id, name, instance_type, state,
            public_ip, private_ip, key_name, security_groups, launch_time,
            availability_zone, ami_id, volumes
    """
    ec2 = boto3.client("ec2", region_name=region)

    response = ec2.describe_instances(
        Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}]
    )

    # Collect all volume IDs to describe in one call
    all_volume_ids = []
    raw_instances = []
    for reservation in response["Reservations"]:
        for inst in reservation["Instances"]:
            vol_ids = [
                bdm["Ebs"]["VolumeId"]
                for bdm in inst.get("BlockDeviceMappings", [])
                if "Ebs" in bdm
            ]
            all_volume_ids.extend(vol_ids)
            raw_instances.append(inst)

    # Fetch volume details in bulk
    volume_map = {}
    if all_volume_ids:
        vol_response = ec2.describe_volumes(VolumeIds=all_volume_ids)
        for vol in vol_response["Volumes"]:
            volume_map[vol["VolumeId"]] = {
                "volume_id": vol["VolumeId"],
                "size_gb": vol["Size"],
                "volume_type": vol["VolumeType"],
                "state": vol["State"],
            }

    instances = []
    for inst in raw_instances:
        name = ""
        for tag in inst.get("Tags", []):
            if tag["Key"] == "Name":
                name = tag["Value"]
                break

        sg_list = [
            f"{sg['GroupName']}({sg['GroupId']})"
            for sg in inst.get("SecurityGroups", [])
        ]

        volumes = []
        for bdm in inst.get("BlockDeviceMappings", []):
            if "Ebs" in bdm:
                vol_id = bdm["Ebs"]["VolumeId"]
                vol_info = volume_map.get(vol_id, {})
                volumes.append(
                    {
                        "device": bdm["DeviceName"],
                        "volume_id": vol_id,
                        "size_gb": vol_info.get("size_gb"),
                        "volume_type": vol_info.get("volume_type"),
                    }
                )

        info = {
            "instance_id": inst["InstanceId"],
            "name": name,
            "instance_type": inst["InstanceType"],
            "state": inst["State"]["Name"],
            "public_ip": inst.get("PublicIpAddress"),
            "private_ip": inst.get("PrivateIpAddress"),
            "key_name": inst.get("KeyName"),
            "security_groups": sg_list,
            "launch_time": inst["LaunchTime"].isoformat(),
            "availability_zone": inst["Placement"]["AvailabilityZone"],
            "ami_id": inst["ImageId"],
            "volumes": volumes,
        }
        instances.append(info)

    instances.sort(key=lambda x: x["launch_time"])

    logging.info(
        f"[list_running_instances] Found {len(instances)} instance(s) in {region}"
    )
    for inst in instances:
        vol_str = (
            ", ".join(
                f"{v['device']}:{v['size_gb']}GB({v['volume_type']})"
                for v in inst["volumes"]
            )
            or "none"
        )
        logging.info(
            f"[list_running_instances]   {inst['instance_id']} | {inst['state']:8s} | {inst['name']:20s} | {inst['instance_type']:12s} | "
            f"public_ip={inst['public_ip']} | private_ip={inst['private_ip']} | "
            f"volumes={vol_str} | "
            f"key={inst['key_name']} | sg={inst['security_groups']} | "
            f"az={inst['availability_zone']} | ami={inst['ami_id']} | "
            f"launched={inst['launch_time']}"
        )

    return instances


def manage_instances(
    region="us-east-1",
    instance_type_values=["t2.micro", "t2.small", "t3.micro", "t3.small", "g5.2xlarge"],
):
    """
    Interactive loop to manage running and stopped EC2 instances.

    For each instance, the available actions depend on its state:
      - running → STOP or TERMINATE
      - stopped → START or TERMINATE

    The menu is numbered sequentially per instance (2 options each),
    followed by bulk actions and exit (0).

    The loop continues after each action until the user chooses 0 or no instances remain.

    Parameters:
        region: str — AWS region

    Returns:
        list of dicts, each with keys: action ("stop"|"start"|"terminate"),
              instance_ids (list of affected instance IDs)
    """
    actions_taken = []
    instance_types = list_instance_types(
        region=region,
        filters=[{"Name": "instance-type", "Values": instance_type_values}],
    )

    while True:
        instances = list_running_instances(region=region)
        n = len(instances)

        # Build menu: 2 options per instance + bulk options + launch
        print("\n=== Manage Instances ===\n")
        menu = {}
        idx = 1

        if n == 0:
            print("  No running or stopped instances found.\n")

        # Fetch AMI names in bulk for all instances
        ami_ids = list({inst["ami_id"] for inst in instances if inst.get("ami_id")})
        ami_name_map = {}
        if ami_ids:
            ec2_client = boto3.client("ec2", region_name=region)
            try:
                ami_resp = ec2_client.describe_images(ImageIds=ami_ids)
                for img in ami_resp["Images"]:
                    ami_name_map[img["ImageId"]] = img.get("Name", img["ImageId"])
            except ClientError:
                pass

        for inst in instances:
            vol_str = (
                ", ".join(
                    f"{v['size_gb']}GB({v['volume_type']})" for v in inst["volumes"]
                )
                or "no disk"
            )
            ami_name = ami_name_map.get(
                inst.get("ami_id", ""), inst.get("ami_id", "unknown")
            )
            label = f"{inst['instance_id']} | {inst['name']} | {ami_name} | {inst['instance_type']} | {inst['state']} | {vol_str}"
            if inst["state"] == "running":
                if inst["public_ip"]:
                    print(
                        f"        SSH: ssh -o StrictHostKeyChecking=no -i api_keys/scanner-minute-key.pem ubuntu@{inst['public_ip']}"
                    )
                print(f"  {idx:3d}. STOP        {label}")
                menu[idx] = ("stop", inst)
                idx += 1
                print(f"  {idx:3d}. TERMINATE    {label}")
                menu[idx] = ("terminate", inst)
                idx += 1
                print(f"  {idx:3d}. RUN COMMAND  {label}")
                menu[idx] = ("run_command", inst)
                idx += 1
            elif inst["state"] == "stopped":
                print(f"  {idx:3d}. START        {label}")
                menu[idx] = ("start", inst)
                idx += 1
                print(f"  {idx:3d}. TERMINATE    {label}")
                menu[idx] = ("terminate", inst)
                idx += 1

        if n > 0:
            running_ids = [
                inst["instance_id"] for inst in instances if inst["state"] == "running"
            ]
            stopped_ids = [
                inst["instance_id"] for inst in instances if inst["state"] == "stopped"
            ]
            all_ids = [inst["instance_id"] for inst in instances]

            print()
            if running_ids:
                print(f"  {idx:3d}. STOP ALL running ({len(running_ids)})")
                menu[idx] = ("stop_all", running_ids)
                idx += 1
            if stopped_ids:
                print(f"  {idx:3d}. START ALL stopped ({len(stopped_ids)})")
                menu[idx] = ("start_all", stopped_ids)
                idx += 1
            print(f"  {idx:3d}. TERMINATE ALL ({len(all_ids)})")
            menu[idx] = ("terminate_all", all_ids)
            idx += 1
        print(f"  {idx:3d}. LAUNCH NEW INSTANCE")
        menu[idx] = ("launch", None)
        print(f"    0. Exit\n")

        # Get user choice
        try:
            choice = int(input("Enter your choice: "))
        except (ValueError, EOFError):
            logging.info("[manage_instances] Invalid input. Try again.")
            continue

        if choice == 0:
            logging.info("[manage_instances] Exiting instance manager.")
            break

        if choice not in menu:
            logging.info(f"[manage_instances] Invalid choice: {choice}. Try again.")
            continue

        ec2 = boto3.client("ec2", region_name=region)
        action, target = menu[choice]

        if action == "stop":
            ids = [target["instance_id"]]
            logging.info(f"[manage_instances] Stopping {ids[0]} ({target['name']})...")
            ec2.stop_instances(InstanceIds=ids)
            logging.info(f"[manage_instances] Stop request sent for {ids[0]}")
            actions_taken.append({"action": "stop", "instance_ids": ids})

        elif action == "start":
            ids = [target["instance_id"]]
            logging.info(f"[manage_instances] Starting {ids[0]} ({target['name']})...")
            ec2.start_instances(InstanceIds=ids)
            logging.info(f"[manage_instances] Start request sent for {ids[0]}")
            actions_taken.append({"action": "start", "instance_ids": ids})

        elif action == "terminate":
            ids = [target["instance_id"]]
            logging.info(
                f"[manage_instances] Terminating {ids[0]} ({target['name']})..."
            )
            ec2.terminate_instances(InstanceIds=ids)
            logging.info(f"[manage_instances] Terminate request sent for {ids[0]}")
            actions_taken.append({"action": "terminate", "instance_ids": ids})

        elif action == "stop_all":
            logging.info(
                f"[manage_instances] Stopping ALL {len(target)} running instances..."
            )
            ec2.stop_instances(InstanceIds=target)
            logging.info(f"[manage_instances] Stop request sent for {target}")
            actions_taken.append({"action": "stop", "instance_ids": target})

        elif action == "start_all":
            logging.info(
                f"[manage_instances] Starting ALL {len(target)} stopped instances..."
            )
            ec2.start_instances(InstanceIds=target)
            logging.info(f"[manage_instances] Start request sent for {target}")
            actions_taken.append({"action": "start", "instance_ids": target})

        elif action == "terminate_all":
            logging.info(
                f"[manage_instances] Terminating ALL {len(target)} instances..."
            )
            ec2.terminate_instances(InstanceIds=target)
            logging.info(f"[manage_instances] Terminate request sent for {target}")
            actions_taken.append({"action": "terminate", "instance_ids": target})

        elif action == "run_command":
            try:
                cmd = input(
                    f"  Command to run on {target['instance_id']} ({target['name']}): "
                ).strip()
            except EOFError:
                continue
            if not cmd:
                logging.info("[manage_instances] No command entered. Skipping.")
                continue
            result = run_command_on_instance(
                instance_id=target["instance_id"],
                command=cmd,
                region=region,
            )
            print(f"\n  Status: {result['status']}")
            if result["stdout"]:
                print(f"  --- stdout ---\n{result['stdout']}")
            if result["stderr"]:
                print(f"  --- stderr ---\n{result['stderr']}")
            actions_taken.append(
                {
                    "action": "run_command",
                    "instance_ids": [target["instance_id"]],
                    "command": cmd,
                    "result": result,
                }
            )

        elif action == "launch":
            AMI_CHOICES = {
                "1": ("ami-02dfbd4ff395f2a1b", "AWS Linux"),
                "2": ("ami-0b6c6ebed2801a5cb", "Ubuntu"),
            }
            print("\n  Select AMI:")
            print("    1. AWS Linux  (ami-02dfbd4ff395f2a1b)")
            print("    2. Ubuntu     (ami-0b6c6ebed2801a5cb)")
            try:
                ami_choice = input("  AMI choice (1/2): ").strip()
            except EOFError:
                continue
            if ami_choice not in AMI_CHOICES:
                logging.info("[manage_instances] Invalid AMI choice. Skipping launch.")
                continue

            ami_id, ami_label = AMI_CHOICES[ami_choice]
            suffix = (
                input(f"  Name suffix (optional, press Enter to skip): ").strip()
                or None
            )
            print("\n  Select instance type:")
            for i, it in enumerate(instance_types, 1):
                price_str = (
                    f"${it['price_per_hour']:.4f}/hr"
                    if it["price_per_hour"] is not None
                    else "N/A"
                )
                print(
                    f"    {i:3d}. {it['instance_type']:20s} | {it['vcpus']:3d} vCPUs | "
                    f"{it['memory_gib']:8.1f} GiB | {price_str}"
                )
            it_input = input(
                "  Instance type [1 or type name, default=t2.micro]: "
            ).strip()
            if not it_input:
                instance_type = "t2.micro"
            elif it_input.isdigit() and 1 <= int(it_input) <= len(instance_types):
                instance_type = instance_types[int(it_input) - 1]["instance_type"]
            else:
                instance_type = it_input
            print(f"  → Using instance type: {instance_type}")
            disk_input = input(
                "  Disk size in GB (optional, press Enter for default): "
            ).strip()
            disk_size_gb = int(disk_input) if disk_input else None

            logging.info(
                f"[manage_instances] Launching {ami_label} ({ami_id}) as {instance_type}..."
            )
            results = launch_instance(
                image_id=ami_id,
                instance_type=instance_type,
                key_name="scanner-minute-key",
                region=region,
                suffix=suffix,
                disk_size_gb=disk_size_gb,
            )
            launched_ids = [r["instance_id"] for r in results]
            actions_taken.append(
                {
                    "action": "launch",
                    "instance_ids": launched_ids,
                }
            )

            # Run install script on Ubuntu instances after launch
            if ami_label == "Ubuntu":
                logging.info(
                    "[manage_instances] Running INSTALL_SCRIPT on launched Ubuntu instance(s)..."
                )
                for r in results:
                    install_result = run_command_on_instance(
                        instance_id=r["instance_id"],
                        command=INSTALL_SCRIPT,
                        region=region,
                        timeout_seconds=300,
                    )
                    print(f"\n  Install on {r['instance_id']}: {install_result['status']}")
                    if install_result["stdout"]:
                        print(f"  --- stdout ---\n{install_result['stdout']}")
                    if install_result["stderr"]:
                        print(f"  --- stderr ---\n{install_result['stderr']}")

    return actions_taken
