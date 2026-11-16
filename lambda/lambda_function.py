
"""InvoiceProcessor Lambda function.

Trigger: S3 Object Created events for invoice/*.json

1. Reads invoice JSON from S3.
2. Calculates 18% GST and grand total.
3. Sends email only if grand total > Rs 10000.
4. Saves summary for every invoice.
5. Records alert_sent as True or False.
"""

import json
import os
from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")

GST_RATE = 0.18
ALERT_THRESHOLD = 10000
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")


def lambda_handler(event, context):

    results = []

    for record in event["Records"]:

        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        print(f"New invoice detected: s3://{bucket}/{key}")

        # Read invoice from S3
        invoice = json.loads(
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        )

        # Calculate GST and grand total
        amount = invoice["amount"]
        gst = round(amount * GST_RATE, 2)
        grand_total = round(amount + gst, 2)

        # Default: no email sent
        alert_sent = False

        # Send email only for high-value invoices
        if grand_total > ALERT_THRESHOLD and SNS_TOPIC_ARN:

            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"HIGH VALUE invoice {invoice['invoice_id']}: Rs {grand_total:.2f}",
                Message=(
                    f"High Value Invoice Alert\n\n"
                    f"Invoice ID: {invoice['invoice_id']}\n"
                    f"Customer: {invoice['customer']}\n"
                    f"Item: {invoice['item']}\n"
                    f"Quantity: {invoice['quantity']}\n"
                    f"Amount: Rs {amount:.2f}\n"
                    f"GST (18%): Rs {gst:.2f}\n"
                    f"Grand Total: Rs {grand_total:.2f}\n"
                )
            )

            alert_sent = True
            print(f"High value email sent for invoice {invoice['invoice_id']}")

        else:
            print(
                f"No email sent for invoice {invoice['invoice_id']}. "
                f"Grand total: Rs {grand_total:.2f}"
            )

        # Create summary for EVERY invoice
        summary = {
            "invoice_id": invoice["invoice_id"],
            "customer": invoice["customer"],
            "item": invoice["item"],
            "amount": amount,
            "gst_18_percent": gst,
            "grand_total": grand_total,
            "alert_sent": alert_sent,
            "source_file": key,
            "processed_by": context.function_name if context else "local-test"
        }

        # Save summary to processed/ folder
        out_key = f"processed/{invoice['invoice_id']}_summary.json"

        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )

        print(f"Summary written: s3://{bucket}/{out_key}")

        results.append(summary)

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
