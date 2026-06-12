import boto3
import json

with open("../../week-6/data/aws_config.json") as f:
    config = json.load(f)

REGION = config['region']
sfn = boto3.client('stepfunctions', region_name=REGION)

# Get the failed execution
executions = sfn.list_executions(
    stateMachineArn=[
        sm['stateMachineArn']
        for sm in sfn.list_state_machines()['stateMachines']
        if 'telco-pipeline' in sm['name']
    ][0],
    statusFilter='FAILED',
    maxResults=1
)

if executions['executions']:
    exec_arn = executions[
        'executions'][0]['executionArn']

    history = sfn.get_execution_history(
        executionArn=exec_arn,
        maxResults=20)

    print("Execution error details:")
    for event in history['events']:
        if 'Failed' in event['type']:
            print(f"\nEvent: {event['type']}")
            details = event.get(
                'lambdaFunctionFailedEventDetails',
                event.get(
                    'executionFailedEventDetails',
                    {}))
            print(f"Error:  {details.get('error', 'N/A')}")
            print(f"Cause:  {details.get('cause', 'N/A')[:300]}")