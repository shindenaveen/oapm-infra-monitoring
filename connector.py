import requests
import csv
from datetime import datetime, timezone
import smtplib
import os 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


PROMETHEUS_URL = ""
# Define the desired labels to extract from Prometheus metrics
DESIRED_LABELS = ['cluster', 'connector', 'env', 'server']
# Define fieldnames for the CSV, combining standard fields with the desired labels
FIELDNAMES = ['timestamp', 'value', 'alert_type'] + DESIRED_LABELS

# Email Configuration
SENDER_EMAIL = ""
RECEIVER_EMAILS_STR = "" 
SMTP_SERVER_HOST = ""
SMTP_PORT = 25

# --- Define all queries, their alert types, and output filenames in one place for easy management ---
QUERIES_TO_RUN = {
    'queue_capacity_low': {
        'query': 'Q1',
        'filename': 'Event Queue Back pressure.csv'
    },
    'logged_errors_increase': {
        'query': 'Q2',
        'filename': 'Debezium worker task errors.csv'
    },
    'offset_commit_failure': {
        'query': 'Q3',
        'filename': 'Debezium offset commit failures.csv'
    }
}

def fetch_and_process_query(query_string, alert_type_label):
    """
    Sends a query to Prometheus and formats any results found.
    Assumes the query is already filtered to return only series that meet the alert condition.
    """
    alerts = []
    print(f"INFO: Fetching data for query: {query_string}")
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query_string})
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
        prometheus_response = response.json()
        if prometheus_response.get('status') != 'success':
            print(f"WARNING: Prometheus query '{query_string}' failed.")
            print(f"Response: {prometheus_response}")
            return []

        data = prometheus_response.get('data', {}).get('result', [])
        if not data:
            print(f"INFO: No data returned for query: {query_string} (no alerts triggered).")
            return []

        for item in data:
            try:
                # item['value'] is typically [timestamp, value_string]
                metric_value_str = item['value'][1]
                metric_value_float = float(metric_value_str)

                all_labels = item.get('metric', {})

                # Build the alert dictionary with a flat structure
                alert_data = {
                    'timestamp': datetime.now(timezone.utc).isoformat(), # Timestamp of when the script processed this alert
                    'value': metric_value_float,
                    'alert_type': alert_type_label
                }
                 # Add label values to the dictionary, using None for missing labels
                for label in DESIRED_LABELS:
                    alert_data[label] = all_labels.get(label)

                alerts.append(alert_data)
            except (IndexError, ValueError, TypeError, KeyError) as e:
                print(f"WARNING: Could not parse item for query '{query_string}': {item}. Error: {e}")
                continue
        
        print(f"INFO: Found {len(alerts)} alerts for query '{query_string}'.")
        return alerts

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Request to Prometheus failed for query '{query_string}': {e}")
        return []
    except ValueError as e:  # Includes JSONDecodeError
        print(f"ERROR: Could not decode JSON response from Prometheus for query '{query_string}': {e}")
        if 'response' in locals() and hasattr(response, 'text'):
            print(f"Response text: {response.text}")
        return []

def write_alerts_to_csv(filename, alerts_data):
    """
    Writes a list of alert data to a specified CSV file. Overwrites the file if it exists.
    """
    if not alerts_data:
        print(f"INFO: No data to write for {filename}.")
        return
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(alerts_data)
        print(f"INFO: Alerts successfully written to {filename}")
    except IOError as e:
        print(f"ERROR: Could not write to CSV file {filename}. Error: {e}")

def send_report_email(subject, body_html, attachment_filepaths):
    """
    Sends an email with the specified subject, body, and a list of attachments.
    """
    print(f"INFO: Preparing to send email report to {RECEIVER_EMAILS_STR}...")
    message = MIMEMultipart('related')
    message['From'] = SENDER_EMAIL
    message['To'] = RECEIVER_EMAILS_STR
    message['Subject'] = subject

    message.attach(MIMEText(body_html, 'html'))

     # Attach all CSV files from the list
    for filepath in attachment_filepaths:
        if os.path.exists(filepath):
            try:
                with open(filepath, "rb") as attachment_file:
                    part = MIMEApplication(attachment_file.read(), _subtype="csv")
                part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(filepath))
                message.attach(part)
                print(f"INFO: Attached {filepath} to the email.")
            except Exception as e:
                print(f"ERROR: Could not attach file {filepath}. Error: {e}")
        else:
            print(f"WARNING: Attachment file {filepath} not found. Sending email without attachment.")

    try:
        receivers_list = [r.strip() for r in RECEIVER_EMAILS_STR.split(',')]
        if not receivers_list or not receivers_list[0]:
            print("ERROR: No recipients specified or recipient string is empty. Email not sent.")
            return
        with smtplib.SMTP(SMTP_SERVER_HOST, SMTP_PORT) as smtp_server:

            smtp_server.sendmail(SENDER_EMAIL, receivers_list, message.as_string())
        print(f"INFO: Email sent successfully to {RECEIVER_EMAILS_STR}.")
    except smtplib.SMTPException as e:
        print(f"ERROR: Failed to send email (SMTPException): {e}")
    except Exception as e:
        print(f"ERROR: Failed to send email (General Exception): {e}")

# --- Main script execution ---
all_alerts_data = []
generated_csv_files = []

# 1. Process Queue Capacity Query
for alert_type, config in QUERIES_TO_RUN.items():
    alerts = fetch_and_process_query(config['query'], alert_type)
    
    if alerts:
        # Add to the master list for the final count
        all_alerts_data.extend(alerts)
        
        # Write to its specific CSV file
        csv_filename = config['filename']
        write_alerts_to_csv(csv_filename, alerts)
        
        # Add the generated file to the list for attachment
        generated_csv_files.append(csv_filename)

# 2. Process Logged Errors Query
# Prepare email content AFTER all data is processed
num_total_alerts = len(all_alerts_data)

# Send email ONLY if there are alerts
if num_total_alerts > 0:
    email_subject = f"Debezium Monitoring Alert - {num_total_alerts} issue(s) found across {len(generated_csv_files)} categories"
    
    # Build a more detailed email body with a list of found issues
    summary_list_html = "".join([f"<li><b>{os.path.splitext(os.path.basename(f))[0]}:</b> See attached file <code>{os.path.basename(f)}</code></li>" for f in generated_csv_files])
    email_body_html = f"""
    <div>
    The Debezium monitoring script detected issues at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}.
    <br><br>
    <b>Summary of Alerts:</b>
    <ul>
    {summary_list_html}
    </ul>
    <br>
    A total of <b>{num_total_alerts}</b> alert(s) were found. Please see the attached CSV files for details.
    <br><br>
    Thanks,<br>
    OAPM Monitoring Script
    </div>
    """
    send_report_email(email_subject, email_body_html, generated_csv_files)
else:
    print("INFO: No alerts found. Skipping email notification.")