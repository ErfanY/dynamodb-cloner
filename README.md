# AWS DynamoDB Cloner

### Introduction
`ddbc.py` is a script designed for cloning DynamoDB tables.  

The tool uses AWS boto3 Python library.


### Prerequisites
* `python` interpreter, tested with Python 3.6.5
* `boto3` library, tested with boto3 1.9.4


### Usage

```text
usage: ddbc [-h] [--verbose] [--version] [--profile PROFILE] [--region REGION]
            --from FROM --to TO [--tags TAGS] [--local]
            [--include-data | --no-include-data]

DynamoDB cloning tool.

optional arguments:
  -h, --help         show this help message and exit
  --verbose, -v      Increase output verbosity, supports up to 3 levels
  --version          show program's version number and exit
  --profile PROFILE  AWS profile to use
  --region REGION    AWS profile to use, default is fetched from profile
  --from FROM        Table to clone
  --to TO            Destination table
  --tags TAGS        Dictionary of new table tags
  --local            Local DynamoDB endpoint
  --include-data     Copy data as well, false per default
  --no-include-data  Do not copy data, true per default
```


### Credits
This script was made with help of:
* [Original script](https://github.com/djangofan/dynamodb-copy-table-boto3/blob/master/dynamo_copy_table.py)
* [Autoscaling snippet](https://gist.github.com/knil-sama/e9b9c16bac6a3f6a6e1c80ce18361ab1)