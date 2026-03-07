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
        f"Launched {len(instance_ids)} instance(s) from {image_id}: {instance_ids} | name={tag_name}"
    )

    # Wait for instances to be running and get public IPs
    logging.info("Waiting for instances to enter running state...")
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
                f"  {info['instance_id']} | {info['name']} | public_ip={info['public_ip']}"
            )
            if info["public_ip"] and key_name:
                pem_path = f"api_keys/{key_name}.pem"
                logging.info(
                    f"  SSH: ssh -i {pem_path} ec2-user@{info['public_ip']}"
                )

    return results


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
        logging.info(f"Key pair '{key_name}' already exists")
        return None

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
            logging.error("No default VPC found. Specify vpc_id explicitly.")
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
            logging.info(f"Security group '{group_name}' already exists: {sg_id}")
            return sg_id
    except ClientError as e:
        logging.error(f"Error checking existing security groups: {e}")
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
        logging.error(f"Failed to create security group '{group_name}': {e}")
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
        logging.error(f"Failed to set ingress rules for {sg_id}: {e}")

    # Tag it
    try:
        ec2.create_tags(
            Resources=[sg_id],
            Tags=[{"Key": "Name", "Value": group_name}],
        )
    except ClientError as e:
        logging.warning(f"Failed to tag security group {sg_id}: {e}")

    logging.info(
        f"Created security group '{group_name}' ({sg_id}) in VPC {vpc_id} | "
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

    logging.info(f"Found {len(instance_types)} instance types in {region}")
    for it in instance_types:
        price_str = (
            f"${it['price_per_hour']:.4f}/hr"
            if it["price_per_hour"] is not None
            else "N/A"
        )
        logging.info(
            f"  {it['instance_type']:20s} | {it['vcpus']:3d} vCPUs | "
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
                volumes.append({
                    "device": bdm["DeviceName"],
                    "volume_id": vol_id,
                    "size_gb": vol_info.get("size_gb"),
                    "volume_type": vol_info.get("volume_type"),
                })

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

    logging.info(f"Found {len(instances)} instance(s) in {region}")
    for inst in instances:
        vol_str = ", ".join(
            f"{v['device']}:{v['size_gb']}GB({v['volume_type']})" for v in inst["volumes"]
        ) or "none"
        logging.info(
            f"  {inst['instance_id']} | {inst['state']:8s} | {inst['name']:20s} | {inst['instance_type']:12s} | "
            f"public_ip={inst['public_ip']} | private_ip={inst['private_ip']} | "
            f"volumes={vol_str} | "
            f"key={inst['key_name']} | sg={inst['security_groups']} | "
            f"az={inst['availability_zone']} | ami={inst['ami_id']} | "
            f"launched={inst['launch_time']}"
        )

    return instances


def manage_instances(region="us-east-1"):
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

    while True:
        instances = list_running_instances(region=region)
        n = len(instances)

        if n == 0:
            logging.info("No running or stopped instances to manage.")
            break

        # Build menu: 2 options per instance + 4 bulk options
        print("\n=== Manage Instances ===\n")
        menu = {}
        idx = 1
        for inst in instances:
            vol_str = ", ".join(
                f"{v['size_gb']}GB({v['volume_type']})" for v in inst["volumes"]
            ) or "no disk"
            label = f"{inst['instance_id']} | {inst['name']} | {inst['instance_type']} | {inst['state']} | {vol_str}"
            if inst["state"] == "running":
                print(f"  {idx:3d}. STOP       {label}")
                menu[idx] = ("stop", inst)
                idx += 1
                print(f"  {idx:3d}. TERMINATE   {label}")
                menu[idx] = ("terminate", inst)
                idx += 1
            elif inst["state"] == "stopped":
                print(f"  {idx:3d}. START      {label}")
                menu[idx] = ("start", inst)
                idx += 1
                print(f"  {idx:3d}. TERMINATE   {label}")
                menu[idx] = ("terminate", inst)
                idx += 1

        running_ids = [inst["instance_id"] for inst in instances if inst["state"] == "running"]
        stopped_ids = [inst["instance_id"] for inst in instances if inst["state"] == "stopped"]
        all_ids = [inst["instance_id"] for inst in instances]

        print()
        bulk_start = idx
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
        print(f"    0. Exit\n")

        # Get user choice
        try:
            choice = int(input("Enter your choice: "))
        except (ValueError, EOFError):
            logging.info("Invalid input. Try again.")
            continue

        if choice == 0:
            logging.info("Exiting instance manager.")
            break

        if choice not in menu:
            logging.info(f"Invalid choice: {choice}. Try again.")
            continue

        ec2 = boto3.client("ec2", region_name=region)
        action, target = menu[choice]

        if action == "stop":
            ids = [target["instance_id"]]
            logging.info(f"Stopping {ids[0]} ({target['name']})...")
            ec2.stop_instances(InstanceIds=ids)
            logging.info(f"Stop request sent for {ids[0]}")
            actions_taken.append({"action": "stop", "instance_ids": ids})

        elif action == "start":
            ids = [target["instance_id"]]
            logging.info(f"Starting {ids[0]} ({target['name']})...")
            ec2.start_instances(InstanceIds=ids)
            logging.info(f"Start request sent for {ids[0]}")
            actions_taken.append({"action": "start", "instance_ids": ids})

        elif action == "terminate":
            ids = [target["instance_id"]]
            logging.info(f"Terminating {ids[0]} ({target['name']})...")
            ec2.terminate_instances(InstanceIds=ids)
            logging.info(f"Terminate request sent for {ids[0]}")
            actions_taken.append({"action": "terminate", "instance_ids": ids})

        elif action == "stop_all":
            logging.info(f"Stopping ALL {len(target)} running instances...")
            ec2.stop_instances(InstanceIds=target)
            logging.info(f"Stop request sent for {target}")
            actions_taken.append({"action": "stop", "instance_ids": target})

        elif action == "start_all":
            logging.info(f"Starting ALL {len(target)} stopped instances...")
            ec2.start_instances(InstanceIds=target)
            logging.info(f"Start request sent for {target}")
            actions_taken.append({"action": "start", "instance_ids": target})

        elif action == "terminate_all":
            logging.info(f"Terminating ALL {len(target)} instances...")
            ec2.terminate_instances(InstanceIds=target)
            logging.info(f"Terminate request sent for {target}")
            actions_taken.append({"action": "terminate", "instance_ids": target})

    return actions_taken
