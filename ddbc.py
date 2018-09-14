import argparse
import itertools
import logging
import multiprocessing
import sys
from time import sleep

import boto3

spinner = itertools.cycle(['-', '/', '|', '\\'])
localDynamoHost = 'http://192.168.99.100:8000'


def copy(src, dst, client):
    """
    Copy items from source to destination
    :param src: Source table
    :param dst: Destination table
    :param client: Db client
    :return: 0 if successful
    """
    logger.info('Copying data over from {} to {}'.format(src, dst))
    pool_size = 4  # tested with 4, took 5 minutes to copy 150,000+ items
    pool = []
    for i in range(pool_size):
        worker = multiprocessing.Process(
            target=copy_items,
            kwargs={
                'src': src,
                'dst': dst,
                'client': client,
                'segment': i,
                'total_segments': pool_size
            }
        )
        pool.append(worker)
        worker.start()

    for process in pool:
        process.join()
    return 0


def copy_items(src, dst, client, segment, total_segments):
    logger.debug('Processing data copy segment {} out of {}'.format(segment, total_segments))
    # copy over item
    item_count = 0
    paginator = client.get_paginator('scan')
    for page in paginator.paginate(TableName=src, Select='ALL_ATTRIBUTES',
                                   ReturnConsumedCapacity='NONE',
                                   ConsistentRead=True,
                                   Segment=segment,
                                   TotalSegments=total_segments,
                                   PaginationConfig={"PageSize": 25}):
        batch = []
        for item in page['Items']:
            item_count += 1
            batch.append({
                'PutRequest': {
                    'Item': item
                }
            })
        if item_count is not 0:
            client.batch_write_item(
                RequestItems={
                    dst: batch
                }
            )
        logger.debug("Processed segment {}, put {} items".format(segment, item_count))
    return 0


def create_table(src, dst, client):
    keyword_args = build_keywords(src, dst, client)
    logger.info('Sending request for table creation')
    client.create_table(**keyword_args)
    logger.info("Waiting for the new table {} to become active".format(dst))
    sleep(5)
    while client.describe_table(TableName=dst)['Table']['TableStatus'] != 'ACTIVE':
        sys.stdout.write(next(spinner))
        sys.stdout.flush()
        sleep(0.1)
        sys.stdout.write('\b')
    logger.info("New table {} to is now active".format(dst))
    return 0


def build_keywords(src, dst, client):
    """
    Build table configuration
    :param src: Source table
    :param dst: Destination table
    :param client: Db client
    :return: Dictionary for keyword args for boto3 create_table method
    """
    # get source table and its schema
    logger.debug('Retrieving schema from source table {}'.format(src))
    table_schema = client.describe_table(TableName=src)["Table"]
    logger.debug('Fetched schema from source table {}'.format(src))
    # create keyword args for copy table
    keyword_args = {
        'TableName': dst,
        'KeySchema': table_schema['KeySchema'],
        'AttributeDefinitions': table_schema['AttributeDefinitions']
    }
    global_secondary_indexes = []
    local_secondary_indexes = []
    if table_schema.get("GlobalSecondaryIndexes"):
        logger.debug('Found global secondary indexes, fetching index information')
        for item in table_schema["GlobalSecondaryIndexes"]:
            index = {}
            for k, v in item.items():
                if k in ["IndexName", "KeySchema", "Projection"]:
                    index[k] = v
            global_secondary_indexes.append(index)
    if table_schema.get("LocalSecondaryIndexes"):
        logger.debug('Found local secondary indexes, fetching index information')
        for item in table_schema["LocalSecondaryIndexes"]:
            index = {}
            for k, v in item.iteritems():
                if k in ["IndexName", "KeySchema", "Projection"]:
                    index[k] = v
            local_secondary_indexes.append(index)
    if global_secondary_indexes:
        keyword_args["GlobalSecondaryIndexes"] = global_secondary_indexes
    if local_secondary_indexes:
        keyword_args["LocalSecondaryIndexes"] = local_secondary_indexes
    # comment below to have same read/write capacity as original table
    keyword_args["ProvisionedThroughput"] = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
    logger.info('Setting provisioned throughput to 1 for both read and write')
    if table_schema.get('StreamSpecification'):
        logger.debug('Found streaming specification, fetching stream information')
        keyword_args['StreamSpecification'] = table_schema['StreamSpecification']
    logger.info('Activating at rest encryption')
    keyword_args['SSESpecification'] = {'Enabled': True}
    return keyword_args


def generate_args():
    """
    Generate program arguments
    :return: Argument parser
    """
    parser = argparse.ArgumentParser(
        prog='ddbc',
        description='DynamoDB cloning tool.',
        fromfile_prefix_chars='@')
    parser.add_argument('--verbose', '-v', action='count', help='Increase output verbosity, supports up to 3 levels')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1.1')
    parser.add_argument('--profile', action='store', help='AWS profile to use', default='default')
    parser.add_argument('--region', action='store', help='AWS profile to use, default is fetched from profile')
    parser.add_argument('--from', required=True, action='store', help='Table to clone')
    parser.add_argument('--to', required=True, action='store', help='Destination table')
    parser.add_argument('--tags', required=False, action='store', help='Dictionary of new table tags')
    parser.add_argument('--local', required=False, dest='is_local', action='store_true', help='Local DynamoDB endpoint')
    data_clone_option = parser.add_mutually_exclusive_group(required=False)
    data_clone_option.add_argument('--include-data', required=False, dest='clone_data', action='store_true',
                                   help='Copy data as well, false per default')
    data_clone_option.add_argument('--no-include-data', required=False, dest='clone_data', action='store_false',
                                   help='Do not copy data, true per default')
    parser.set_defaults(clone_data=False)
    return parser


def validate_args(src, dst, client):
    """
    Validate source and destinations
    :param src: Source table
    :param dst: Destination table
    :param client: Db client
    :return: 0 if successful
    """
    logger.debug('Validating source and destination tables')
    try:
        client.describe_table(TableName=src)["Table"]
    except client.exceptions.ResourceNotFoundException:
        logger.error("Source table {} does not exist. Exiting...".format(src))
        raise ValueError('None existing source {}'.format(src))
    logger.info('Found source table {}'.format(src))
    try:
        client.describe_table(TableName=dst)["Table"]
        logger.error('Existing destination table {}'.format(dst))
        raise ValueError('Existing destination table {}'.format(dst))
    except client.exceptions.ResourceNotFoundException:
        logger.debug('Destination table was not found. This is expected.')
    return 0


def setup_autoscaling(table, client):
    logger.info('Setting up autoscaling, using default min 1 and max 1000 for read/write provisioning')
    scalable_dimensions = {'dynamodb:table:ReadCapacityUnits': [1, 1000],
                           'dynamodb:table:WriteCapacityUnits': [1, 1000]}
    for scalable_dimension, capacity in scalable_dimensions.items():
        client.register_scalable_target(ServiceNamespace="dynamodb",
                                        ResourceId="table/{}".format(table),
                                        ScalableDimension=scalable_dimension,
                                        MinCapacity=capacity[0],
                                        MaxCapacity=capacity[1])
    # create scaling policy, without them the auto scaling will not be applied
    metrics_and_dimension = {'DynamoDBReadCapacityUtilization': 'dynamodb:table:ReadCapacityUnits',
                             'DynamoDBWriteCapacityUtilization': 'dynamodb:table:WriteCapacityUnits'}
    percent_of_use_to_aim_for = 70.0
    scale_out_cooldown_in_seconds = 60
    scale_in_cooldown_in_seconds = 60
    for metric, dimension in metrics_and_dimension.items():
        logger.debug('Setting up scaling policy for metric {} with dimension {}'.format(metric, dimension))
        client.put_scaling_policy(ServiceNamespace="dynamodb",
                                  ResourceId="table/{}".format(table),
                                  PolicyType='TargetTrackingScaling',
                                  PolicyName="Scale{}".format(metric),
                                  ScalableDimension=dimension,
                                  TargetTrackingScalingPolicyConfiguration={
                                      'TargetValue': percent_of_use_to_aim_for,
                                      'PredefinedMetricSpecification': {
                                          'PredefinedMetricType': metric
                                      },
                                      'ScaleOutCooldown': scale_out_cooldown_in_seconds,
                                      'ScaleInCooldown': scale_in_cooldown_in_seconds
                                  })
    logger.info('Finished setting up autoscaling policies')
    return 0


def setup_tags(client):
    logger.warning('Tags are not implemented yet')
    return 0


def init(args):
    """
    Initialize script with configuring logger and boto
    :param args: Script arguments
    :return: 0 if successful
    """
    log_level = logging.ERROR
    if args.verbose == 1:
        log_level = logging.WARN
    elif args.verbose == 2:
        log_level = logging.INFO
    elif args.verbose and args.verbose > 2:
        log_level = logging.DEBUG
    logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return 0


def configure_db_client(args):
    """
    Configure database client
    :param args: Script arguments
    :return: db client
    """
    if not args.is_local:
        iam_role = boto3.session.Session(profile_name=args.profile, region_name=args.region)
        return iam_role.client('dynamodb')
    else:
        return boto3.client('dynamodb', endpoint_url=localDynamoHost)


def configure_as_client(args):
    """
    Configure autoscaling client
    :param args: Script arguments
    :return: db client
    """
    iam_role = boto3.session.Session(profile_name=args.profile, region_name=args.region)
    return iam_role.client('application-autoscaling')


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    arguments = generate_args().parse_args()
    init(arguments)
    db_client = configure_db_client(arguments)
    src = getattr(arguments, 'from')
    dst = getattr(arguments, 'to')
    validate_args(src, dst, db_client)
    create_table(src, dst, db_client)
    setup_autoscaling(dst, configure_as_client(arguments))
    setup_tags(db_client)
    if arguments.clone_data:
        copy(src, dst, db_client)
