import requests
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --- Configuration ---
# API endpoint for Prometheus query
API_URL = ""
# The threshold for the process count
THRESHOLD = 80

# --- Email Configuration ---
SENDER_EMAIL = ""
# Comma-separated string for one or more recipients
RECEIVER_EMAILS_STR = "" 
SMTP_SERVER_HOST = ""
SMTP_PORT = 2577
EMAIL_SUBJECT = f"ALERT: High Session Usage Detected in Oracle DB (Threshold > {THRESHOLD}%)"

def send_alert_email(subject, body_html, sender, recipients_str, smtp_host, smtp_port):
    """
    Constructs and sends an email alert.
    This logic is adapted from connector.py for consistency.
    """
    # Check for recipients
    receivers_list = [r.strip() for r in recipients_str.split(',') if r.strip()]
    if not receivers_list:
        logging.error("No recipients specified or recipient string is empty. Email not sent.")
        return

    logging.info(f"Preparing to send email report to {recipients_str}...")
    
    # Using 'related' for consistency with connector.py
    message = MIMEMultipart('related')
    message['From'] = sender
    message['To'] = recipients_str
    message['Subject'] = subject

    message.attach(MIMEText(body_html, 'html'))

    # The smtplib.SMTPException is caught in the main function's try/except block,
    # so we let it propagate up.
    with smtplib.SMTP(smtp_host, smtp_port) as smtp_server:
        smtp_server.sendmail(sender, receivers_list, message.as_string())
    logging.info(f"Email sent successfully to {recipients_str}.")

# --- Setup Logging ---
# Configures basic logging to print informational messages and errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_alert_table_html(title, alerts):
    """Generates an HTML table for a given list of alerts."""
    if not alerts:
        return ""

    # Define table headers
    table_header = "<tr><th>Database</th><th>Client</th><th>Environment</th><th>Session Count %</th></tr>"
    
    # Create a row for each alert
    table_rows = ""
    for alert in alerts:
        table_rows += f"<tr><td>{alert['database']}</td><td>{alert['client']}</td><td>{alert['env']}</td><td>{alert['value']}%</td></tr>"

    # Combine into a full HTML table with a title
    return f"""
        <h3>{title}</h3>
        <table>
            {table_header}
            {table_rows}
        </table>
    """

def main():
    """
    Main function to fetch metrics, check against a threshold, and send an email alert.
    """
    logging.info("Starting process check...")

    try:
        # Step 1: Fetch data from the API
        logging.info(f"Querying API: {API_URL}")
        response = requests.get(API_URL, timeout=15) # Added a timeout for safety
        response.raise_for_status()  # This will raise an HTTPError for bad responses (4xx or 5xx)

        data = response.json()

        # Check if the API query was successful
        if data.get('status') != 'success':
            logging.error(f"API returned a non-success status: {data.get('status')}")
            return

        # Step 2: Filter results that are above the threshold
        results = data.get('data', {}).get('result', [])
        all_alerts = []
        for item in results:
            # The value is the second element in the 'value' list: [timestamp, "value_string"]
            process_count = float(item['value'][1])
            if process_count >= THRESHOLD:
                metric_details = item['metric']
                all_alerts.append({
                    'database': metric_details.get('database', 'N/A'),
                    'client': metric_details.get('client', 'N/A'),
                    'env': metric_details.get('env', 'N/A'),
                    'value': int(process_count)
                })

        # Step 3: If there are no alerts, do nothing and exit
        if not all_alerts:
            logging.info(f"No process counts found above the threshold of {THRESHOLD}.")
            return

        logging.warning(f"Found {len(all_alerts)} instances exceeding the threshold. Preparing alert email.")

         # Step 4: Separate alerts into PRD and NPD
        prd_alerts = []
        npd_alerts = []
        for alert in all_alerts:
            # A simple check: if 'prd' is in the environment name, it's production.
            if 'prd' in alert.get('env', '').lower():
                prd_alerts.append(alert)
            else:
                npd_alerts.append(alert)

        # Step 5: Create the HTML for the email body
        html_style = """
        <style>
            body { font-family: sans-serif; font-size: 14px; }
            table { 
                border-collapse: collapse; 
                width: 60%; 
                margin: 20px 0; 
                font-size: 12px;
            }
            th, td { 
                border: 1px solid #dddddd; 
                text-align: left; 
                padding: 4px;
            }
            th { background-color: #f2f2f2; font-weight: bold; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            h2 { color: #D9534F; }
            h3 { margin-top: 25px; color: #333; }
        </style>
        """

        # Generate HTML tables for each environment type
        prd_table_html = create_alert_table_html("Production Environments (PRD)", prd_alerts)
        npd_table_html = create_alert_table_html("Non-Production Environments (NPD)", npd_alerts)

        html_body = f"""
        <html><head>{html_style}</head>
        <body>
            <h2>High Session Usage Alert</h2>
           <p>The following databases have reported session usage reaching or exceeding the threshold of <b>{THRESHOLD}%</b>.</p>
            {prd_table_html}
            {npd_table_html}
            <br>
            <div>Thanks,</div>
            <div>Oapm Team</div>
        </body></html>
        """

       # Step 6: Send the email using the modular function
        send_alert_email(
            subject=EMAIL_SUBJECT,
            body_html=html_body,
            sender=SENDER_EMAIL,
            recipients_str=RECEIVER_EMAILS_STR,
            smtp_host=SMTP_SERVER_HOST,
            smtp_port=SMTP_PORT
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to connect to API: {e}")
    except smtplib.SMTPException as e:
        logging.error(f"Failed to send email: {e}")
    except (KeyError, IndexError, ValueError) as e:
        logging.error(f"Failed to parse API response. Check response format. Error: {e}")

if __name__ == "__main__":
    main()

