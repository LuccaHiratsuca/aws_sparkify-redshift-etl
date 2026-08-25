"""Infrastructure as Code for the Sparkify data warehouse.

Provisions and tears down everything the pipeline needs on AWS:

* an IAM role that grants Redshift read-only access to S3,
* a Redshift cluster with the shape described in the ``[DWH]`` config section,
* an ingress rule on the cluster's security group so the ETL scripts can
  connect from outside the VPC.

Doing this in code rather than by hand in the console is what makes the
environment reproducible -- and, just as importantly, makes it easy to delete
the cluster the moment the load is done so it stops costing money.

Usage:
    python iac.py create      # provision, then write HOST and ARN to the config
    python iac.py status      # show the current cluster state
    python iac.py delete      # tear the cluster (and optionally the role) down

Add ``--config <path>`` to any command to use a different config file.
"""

import argparse
import re
import sys
import time

import boto3
from botocore.exceptions import ClientError

from utils import DEFAULT_CONFIG_PATH, load_config

S3_READ_ONLY_POLICY = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

REDSHIFT_TRUST_POLICY = (
    '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", '
    '"Principal": {"Service": "redshift.amazonaws.com"}, '
    '"Action": "sts:AssumeRole"}]}'
)

# How long to wait for the cluster to become available / disappear.
POLL_SECONDS = 30
POLL_ATTEMPTS = 40


# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

def build_clients(config):
    """Create the boto3 clients needed to manage the warehouse.

    Args:
        config (configparser.ConfigParser): Config with [AWS] and [DWH].

    Returns:
        tuple: An ``iam``, ``redshift`` and ``ec2`` client/resource.

    Raises:
        ValueError: If the AWS credentials are missing from the config.
    """
    key = config.get("AWS", "KEY")
    secret = config.get("AWS", "SECRET")
    region = config.get("DWH", "REGION")

    if not key or not secret:
        raise ValueError(
            "Set KEY and SECRET in the [AWS] section before running iac.py."
        )

    session = boto3.Session(
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=region,
    )
    return (
        session.client("iam"),
        session.client("redshift"),
        session.resource("ec2"),
    )


# ---------------------------------------------------------------------------
# Config write-back
# ---------------------------------------------------------------------------

def update_config_value(config_path, section, key, value):
    """Set a single key in an ini file, preserving comments and layout.

    ``configparser`` cannot write a file back without discarding its comments,
    so the target line is rewritten in place instead.

    Args:
        config_path (str): Path to the config file.
        section (str): Section name, without brackets.
        key (str): Key to set.
        value (str): New value.

    Returns:
        bool: True if the key was found and updated.
    """
    with open(config_path, encoding="utf-8") as handle:
        lines = handle.readlines()

    in_section = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_section = stripped[1:-1].strip().upper() == section.upper()
            continue
        if not in_section:
            continue
        match = re.match(r"^(\s*)([A-Za-z0-9_]+)(\s*)=", line)
        if match and match.group(2).upper() == key.upper():
            lines[index] = f"{match.group(1)}{match.group(2)}{match.group(3)}= {value}\n"
            with open(config_path, "w", encoding="utf-8") as handle:
                handle.writelines(lines)
            print(f"  wrote [{section}] {key} to {config_path}")
            return True

    print(f"  WARNING: could not find [{section}] {key} in {config_path}")
    return False


# ---------------------------------------------------------------------------
# IAM role
# ---------------------------------------------------------------------------

def create_iam_role(iam, role_name):
    """Create (or reuse) an IAM role letting Redshift read from S3.

    Args:
        iam: A boto3 IAM client.
        role_name (str): Name of the role to create.

    Returns:
        str: The ARN of the role.
    """
    try:
        iam.create_role(
            RoleName=role_name,
            Description="Allows Redshift clusters to call read-only AWS "
                        "services on your behalf.",
            AssumeRolePolicyDocument=REDSHIFT_TRUST_POLICY,
            Path="/",
        )
        print(f"  created IAM role {role_name}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"  IAM role {role_name} already exists, reusing it")

    iam.attach_role_policy(RoleName=role_name, PolicyArn=S3_READ_ONLY_POLICY)
    print("  attached AmazonS3ReadOnlyAccess")

    arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    print(f"  role ARN is {arn}")
    return arn


def delete_iam_role(iam, role_name):
    """Detach the S3 policy from a role and delete it.

    Args:
        iam: A boto3 IAM client.
        role_name (str): Name of the role to delete.
    """
    try:
        iam.detach_role_policy(RoleName=role_name, PolicyArn=S3_READ_ONLY_POLICY)
        iam.delete_role(RoleName=role_name)
        print(f"  deleted IAM role {role_name}")
    except iam.exceptions.NoSuchEntityException:
        print(f"  IAM role {role_name} does not exist, nothing to delete")


# ---------------------------------------------------------------------------
# Redshift cluster
# ---------------------------------------------------------------------------

def create_cluster(redshift, config, role_arn):
    """Launch the Redshift cluster described by the ``[DWH]`` config section.

    Args:
        redshift: A boto3 Redshift client.
        config (configparser.ConfigParser): Config with [DWH] and [CLUSTER].
        role_arn (str): ARN of the role granting S3 read access.
    """
    dwh, cluster = config["DWH"], config["CLUSTER"]
    identifier = dwh["DWH_CLUSTER_IDENTIFIER"]

    if not cluster.get("DB_PASSWORD"):
        raise ValueError(
            "Set DB_PASSWORD in the [CLUSTER] section before creating the "
            "cluster. Redshift requires 8-64 characters with at least one "
            "uppercase letter, one lowercase letter and one digit."
        )

    params = {
        "ClusterIdentifier": identifier,
        "ClusterType": dwh["DWH_CLUSTER_TYPE"],
        "NodeType": dwh["DWH_NODE_TYPE"],
        "DBName": cluster["DB_NAME"],
        "MasterUsername": cluster["DB_USER"],
        "MasterUserPassword": cluster["DB_PASSWORD"],
        "IamRoles": [role_arn],
        "PubliclyAccessible": True,
    }
    # NumberOfNodes is only valid (and only required) for multi-node clusters.
    if dwh["DWH_CLUSTER_TYPE"] == "multi-node":
        params["NumberOfNodes"] = int(dwh["DWH_NUM_NODES"])

    try:
        redshift.create_cluster(**params)
        print(f"  creating cluster {identifier} "
              f"({dwh['DWH_CLUSTER_TYPE']}, {dwh['DWH_NODE_TYPE']})")
    except redshift.exceptions.ClusterAlreadyExistsFault:
        print(f"  cluster {identifier} already exists, reusing it")


def describe_cluster(redshift, identifier):
    """Fetch the properties of a cluster.

    Args:
        redshift: A boto3 Redshift client.
        identifier (str): The cluster identifier.

    Returns:
        dict | None: Cluster properties, or None if it does not exist.
    """
    try:
        return redshift.describe_clusters(
            ClusterIdentifier=identifier
        )["Clusters"][0]
    except redshift.exceptions.ClusterNotFoundFault:
        return None


def wait_for_available(redshift, identifier):
    """Block until the cluster reports the ``available`` status.

    Args:
        redshift: A boto3 Redshift client.
        identifier (str): The cluster identifier.

    Returns:
        dict: The cluster properties once it is available.

    Raises:
        TimeoutError: If the cluster is not available in time.
    """
    for attempt in range(1, POLL_ATTEMPTS + 1):
        props = describe_cluster(redshift, identifier)
        status = props["ClusterStatus"] if props else "not found"
        print(f"  [{attempt}/{POLL_ATTEMPTS}] cluster status: {status}")
        if status == "available":
            return props
        time.sleep(POLL_SECONDS)

    raise TimeoutError(
        f"Cluster {identifier} was not available after "
        f"{POLL_ATTEMPTS * POLL_SECONDS // 60} minutes."
    )


def authorize_ingress(ec2, props, port):
    """Open the cluster's TCP port to the internet on its security group.

    Args:
        ec2: A boto3 EC2 resource.
        props (dict): Cluster properties from ``describe_cluster``.
        port (int): The TCP port Redshift listens on.
    """
    groups = props.get("VpcSecurityGroups") or []
    if not groups:
        print("  no VPC security group on the cluster, skipping ingress rule")
        return

    security_group = ec2.SecurityGroup(id=groups[0]["VpcSecurityGroupId"])
    try:
        # The resource already identifies the group by id. Passing GroupName as
        # well is rejected for security groups in a non-default VPC.
        security_group.authorize_ingress(
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=port,
            ToPort=port,
        )
        print(f"  opened TCP {port} on {security_group.group_name}")
    except ClientError as error:
        if error.response["Error"]["Code"] == "InvalidPermission.Duplicate":
            print(f"  TCP {port} is already open on "
                  f"{security_group.group_name}")
        else:
            raise


def delete_cluster(redshift, identifier):
    """Delete a cluster without taking a final snapshot.

    Args:
        redshift: A boto3 Redshift client.
        identifier (str): The cluster identifier.

    Returns:
        bool: True if a delete was issued.
    """
    try:
        redshift.delete_cluster(
            ClusterIdentifier=identifier,
            SkipFinalClusterSnapshot=True,
        )
        print(f"  deleting cluster {identifier}")
        return True
    except redshift.exceptions.ClusterNotFoundFault:
        print(f"  cluster {identifier} does not exist, nothing to delete")
        return False


def wait_for_deletion(redshift, identifier):
    """Block until the cluster no longer exists.

    Args:
        redshift: A boto3 Redshift client.
        identifier (str): The cluster identifier.
    """
    for attempt in range(1, POLL_ATTEMPTS + 1):
        props = describe_cluster(redshift, identifier)
        if props is None:
            print(f"  cluster {identifier} is gone")
            return
        print(f"  [{attempt}/{POLL_ATTEMPTS}] cluster status: "
              f"{props['ClusterStatus']}")
        time.sleep(POLL_SECONDS)

    print("  WARNING: cluster still present. Confirm in the AWS console that "
          "it is deleted so it stops incurring charges.")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def command_create(config, config_path):
    """Provision the role and cluster, then persist HOST and ARN to the config.

    Args:
        config (configparser.ConfigParser): The loaded configuration.
        config_path (str): Path the configuration was loaded from.
    """
    iam, redshift, ec2 = build_clients(config)
    identifier = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    port = int(config.get("CLUSTER", "DB_PORT"))

    print("IAM role")
    role_arn = create_iam_role(iam, config.get("DWH", "DWH_IAM_ROLE_NAME"))

    print("\nRedshift cluster")
    create_cluster(redshift, config, role_arn)
    props = wait_for_available(redshift, identifier)
    endpoint = props["Endpoint"]["Address"]
    print(f"  endpoint is {endpoint}")

    print("\nNetworking")
    authorize_ingress(ec2, props, port)

    print("\nUpdating configuration")
    update_config_value(config_path, "CLUSTER", "HOST", endpoint)
    update_config_value(config_path, "IAM_ROLE", "ARN", role_arn)

    print("\nCluster is ready. Next: python create_tables.py")


def command_status(config):
    """Print the current state of the cluster.

    Args:
        config (configparser.ConfigParser): The loaded configuration.
    """
    _, redshift, _ = build_clients(config)
    identifier = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

    props = describe_cluster(redshift, identifier)
    if props is None:
        print(f"Cluster {identifier} does not exist.")
        return

    endpoint = props.get("Endpoint") or {}
    for label, value in [
        ("identifier", props["ClusterIdentifier"]),
        ("status", props["ClusterStatus"]),
        ("node type", props["NodeType"]),
        ("nodes", props.get("NumberOfNodes", 1)),
        ("database", props["DBName"]),
        ("endpoint", endpoint.get("Address", "(not assigned yet)")),
        ("port", endpoint.get("Port", "-")),
        ("vpc", props.get("VpcId", "-")),
    ]:
        print(f"  {label:<12} {value}")


def command_delete(config, config_path, keep_role, wait):
    """Tear the warehouse down and clear the derived config values.

    Args:
        config (configparser.ConfigParser): The loaded configuration.
        config_path (str): Path the configuration was loaded from.
        keep_role (bool): Leave the IAM role in place.
        wait (bool): Block until the cluster has actually disappeared.
    """
    iam, redshift, _ = build_clients(config)
    identifier = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

    print("Redshift cluster")
    deleted = delete_cluster(redshift, identifier)
    if deleted and wait:
        wait_for_deletion(redshift, identifier)

    if keep_role:
        print("\nKeeping the IAM role (--keep-role)")
    else:
        print("\nIAM role")
        delete_iam_role(iam, config.get("DWH", "DWH_IAM_ROLE_NAME"))

    print("\nUpdating configuration")
    update_config_value(config_path, "CLUSTER", "HOST", "")
    if not keep_role:
        update_config_value(config_path, "IAM_ROLE", "ARN", "")

    print("\nTeardown issued. Verify in the AWS console that the cluster is "
          "gone so it stops incurring charges.")


def parse_args():
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "command",
        choices=["create", "status", "delete"],
        help="Infrastructure action to perform.",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to the configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--keep-role",
        action="store_true",
        help="On delete, leave the IAM role in place for the next cluster.",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="On delete, issue the request without waiting for it to finish.",
    )
    return parser.parse_args()


def main():
    """Dispatch the requested infrastructure command."""
    args = parse_args()
    config = load_config(args.config)

    if args.command == "create":
        command_create(config, args.config)
    elif args.command == "status":
        command_status(config)
    else:
        command_delete(
            config,
            args.config,
            keep_role=args.keep_role,
            wait=not args.no_wait,
        )


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, TimeoutError, ClientError) as error:
        sys.exit(f"iac.py failed: {error}")
