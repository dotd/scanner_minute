import argparse
import logging
from ScannerMinute.src import logging_utils
from ScannerMinute.src.aws_utils import launch_instance, run_command_on_instance
from ScannerMinute.src.aws_instance_utils import (
    get_remote_url,
    get_git_credentials,
    ensure_projects_dir,
    git_clone_repo,
)


def tst_full_flow(
    instance_id=None,
    image_id="ami-0b6c6ebed2801a5cb",  # Ubuntu
    instance_type="t2.micro",
    key_name="scanner-minute-key",
    region="us-east-1",
):
    logging_utils.setup_logging(log_level="INFO", include_time=True)

    # 1) Launch instance (skip if instance_id provided)
    if instance_id:
        logging.info(
            f"=== Step 1: Skipping launch, using existing instance {instance_id} ==="
        )
    else:
        logging.info("=== Step 1: Launch instance ===")
        results = launch_instance(
            image_id=image_id,
            instance_type=instance_type,
            key_name=key_name,
            region=region,
        )
        instance_id = results[0]["instance_id"]
        logging.info(f"Instance launched: {instance_id}")

    # 2) Get remote URL
    logging.info("=== Step 2: Get remote URL ===")
    remote_url = get_remote_url()
    logging.info(f"Remote URL: {remote_url}")

    # 3) Show git credentials
    logging.info("=== Step 3: Get git credentials ===")
    user, token = get_git_credentials()
    if user and token:
        logging.info(f"User: {user}, Token: {token[:4]}...{token[-4:]}")
    else:
        logging.info("No git credentials found in api_keys/git_token.txt")

    # 4) Ensure projects dir
    logging.info("=== Step 4: Ensure ~/projects dir ===")
    result = ensure_projects_dir(instance_id, region=region)
    logging.info(f"mkdir result: {result['status']}")

    # 5) Clone repo
    logging.info("=== Step 5: Clone repo ===")
    result = git_clone_repo(instance_id, region=region)
    logging.info(f"Clone result: {result['status']}")
    if result["stdout"]:
        logging.info(f"stdout: {result['stdout']}")

    # 6) List repo contents
    logging.info("=== Step 6: ls /home/ubuntu/projects ===")
    result = run_command_on_instance(
        instance_id=instance_id,
        command="ls -la /home/ubuntu/projects/",
        region=region,
    )
    logging.info(f"ls result:\n{result['stdout']}")

    return instance_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--instance_id",
        type=str,
        default="i-0fd9d2b46b2bcfd2c",
        help="Existing EC2 instance ID (skips launch)",
    )
    args = parser.parse_args()
    tst_full_flow(instance_id=args.instance_id)
