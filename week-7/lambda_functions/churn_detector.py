
import json
import boto3
import csv
import io
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function that analyses
    customer churn risk from S3 file.
    Triggered manually or by S3 event.
    """
    print("Churn Detector Lambda started!")

    # Get S3 details from event
    bucket = event.get(
        'bucket',
        'lawrence-de-bootcamp-20260611')
    key = event.get(
        'key',
        'raw/customers/sample_customers.csv')

    s3 = boto3.client('s3')

    try:
        # Read CSV from S3
        response = s3.get_object(
            Bucket=bucket, Key=key)
        content = response[
            'Body'].read().decode('utf-8')
        reader = csv.DictReader(
            io.StringIO(content))
        customers = list(reader)

        # Analyse churn risk
        total = len(customers)
        churned = sum(
            1 for c in customers
            if c.get('Churn', '').strip()
            in ['1', 'True', 'Yes'])
        churn_rate = round(
            churned / total * 100, 2
        ) if total > 0 else 0

        # High risk customers
        high_risk = [
            c for c in customers
            if c.get('Contract', '')
            == 'Month-to-month'
            and c.get('Churn', '') != '1'
        ]

        result = {
            'statusCode': 200,
            'pipeline': 'ChurnDetector',
            'timestamp': datetime.now(
                ).strftime(
                '%Y-%m-%d %H:%M:%S'),
            'source': {
                'bucket': bucket,
                'key': key
            },
            'analysis': {
                'total_customers': total,
                'churned': churned,
                'churn_rate_pct': churn_rate,
                'high_risk_retained':
                    len(high_risk)
            },
            'status': 'SUCCESS'
        }

        print(f"Analysis complete: "
              f"{total} customers, "
              f"{churn_rate}% churn rate")
        return result

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e),
            'status': 'FAILED'
        }
