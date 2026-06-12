
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that validates
    data quality of uploaded CSV files.
    """
    print("Data Validator Lambda started!")

    bucket = event.get('bucket', '')
    key = event.get('key', '')

    if not bucket or not key:
        return {
            'statusCode': 400,
            'error': 'Missing bucket or key',
            'status': 'FAILED'
        }

    s3 = boto3.client('s3')

    try:
        response = s3.get_object(
            Bucket=bucket, Key=key)
        content = response[
            'Body'].read().decode('utf-8')
        reader = csv.DictReader(
            io.StringIO(content))
        records = list(reader)

        issues = []
        valid = 0
        invalid = 0

        required_fields = [
            'customerID',
            'MonthlyCharges',
            'Contract']

        for i, record in enumerate(
                records, 1):
            record_issues = []

            for field in required_fields:
                if not record.get(field, ''):
                    record_issues.append(
                        f"Missing {field}")

            try:
                charge = float(
                    record.get(
                        'MonthlyCharges', 0))
                if charge <= 0:
                    record_issues.append(
                        "Invalid charge")
            except ValueError:
                record_issues.append(
                    "Non-numeric charge")

            if record_issues:
                invalid += 1
                issues.append({
                    'row': i,
                    'issues': record_issues
                })
            else:
                valid += 1

        return {
            'statusCode': 200,
            'pipeline': 'DataValidator',
            'timestamp': datetime.now(
                ).strftime(
                '%Y-%m-%d %H:%M:%S'),
            'validation': {
                'total_records': len(records),
                'valid': valid,
                'invalid': invalid,
                'issues_found': len(issues),
                'sample_issues': issues[:3]
            },
            'status': 'PASSED'
            if invalid == 0 else 'WARNINGS'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'error': str(e),
            'status': 'FAILED'
        }
