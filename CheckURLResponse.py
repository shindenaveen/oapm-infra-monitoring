import os
import re
import requests
import ast
import psycopg2
from tabulate import tabulate
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --- Configuration ---
BASE_PATH = "<path>"
PRD_BATCH_DIR_PREFIX = "prd_apm_batch"

# Common patterns for backup files to ignore
BACKUP_FILE_PATTERNS = [
    '.bak',  # e.g., filename.py.bak
    '.orig', # e.g., filename.py.orig
    '.old',  # e.g., filename.py.old
    '.tmp',  # e.g., filename.py.tmp
]

# Database Configuration (NEW)
DB_CONFIG = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

# Email Configuration (NEW)
EMAIL_CONFIG = {
    "SENDER_EMAIL": "",
    "RECEIVER_EMAILS_STR": "",
    "SMTP_SERVER_HOST": "10.50.4.94",
    "SMTP_PORT": 25,
    "EMAIL_SUBJECT": "ALERT: URL Health Check Failure Detected"
}

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_backup_file(filename):
    """
    Checks if a filename matches common backup patterns, including date-formatted backups.
    e.g., filename.py.20250623
    """
    # Check for date-formatted backup,
    if re.search(r'\.py\.\d{8}$', filename):
        return True
    if filename.endswith('~'):  # e.g., file.py~
        return True
    if filename.startswith('#') and filename.endswith('#'):  # e.g., #file.py#
        return True
    for pattern in BACKUP_FILE_PATTERNS:
        if filename.lower().endswith(pattern):
            return True
    return False

def find_python_files_in_batch_dirs(base_path):
    """
    Walks through the base_path, finds directories starting with PRD_BATCH_DIR_PREFIX,
    and yields paths to valid Python files within them.
    """
    for root, _, files in os.walk(base_path):
        # Check if the current directory's name starts with the batch prefix
        if os.path.basename(root).startswith(PRD_BATCH_DIR_PREFIX):
            for file in files:
                if file.endswith(".py") and not is_backup_file(file):
                    yield os.path.join(root, file)

def extract_info_from_url(url):
    """
    Extracts Client and Environment from a URL based on a specific path structure.
    - Client: The segment after the 3rd forward slash ('/').
    - Env: The first 3 letters of the segment after the 5th forward slash.
    Client is 'TXDAS' and Env is 'PRD'.
    """
    try:
        parts = url.split('/')
        # Example: ['https:', '', 'domain.com', 'CLIENT', 'GROK', 'ENV_SRC', 'metrics']
        # We need at least 6 parts for parts[5] to be valid. (indices 0 through 5)
        if len(parts) > 5:
            client = parts[3]
            env_source = parts[5]
            env = env_source[:3].upper()
            return client, env
    except (IndexError, TypeError):
        # This can happen if the URL does not have the expected number of path segments.
        pass # Silently fail and return the default values below.
    return "N/A", "N/A"

def extract_grok_data(filepath):
    """
    Extracts URLs, Client, and Environment from the 'grok' variable in a Python file.
    Assumes grok is structured as: [["url1", "url2", ...], "ClientName", "EnvName", ...]
    # Regex to find 'grok = [...]'. re.DOTALL allows '.' to match newlines.
    # Improved regex to find 'grok = [...]', handling potential missing closing brackets
    # and extracting URLs starting with "https" and ending with "metrics".
    # re.DOTALL allows '.' to match newlines, and the non-greedy qualifier '?' is used.
    # The URL extraction is now part of the regex.
    """
    # Regex to find 'grok = [...]'. re.DOTALL allows '.' to match newlines.
    grok_pattern = re.compile(r"grok\s*=\s*(\[.*?\])", re.DOTALL)

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            match = grok_pattern.search(content)
            if match:
                grok_str = match.group(1)
                try:
                    # Safely evaluate the string as a Python literal (list)
                    # Instead of directly evaluating, we'll manually parse to handle errors
                    # and extract URLs with specific criteria.
                    parts = grok_str[1:-1].split(",")  # Split the list content by commas, remove brackets
                    if len(parts) >= 3:
                        urls_part = parts[0].strip()
                        client = parts[1].strip().replace('"', '').replace("'", "") if len(parts) > 1 else None
                        env = parts[2].strip().replace('"', '').replace("'", "") if len(parts) > 2 else None

                        # Extract URLs from the first part using regex
                        url_matches = re.findall(r'"(https://.*?metrics)"', urls_part)
                        urls = url_matches if url_matches else []

                        if urls and client and env:
                            return urls, client, env
                        else:
                           logging.warning(f"Incomplete or malformed grok data in {filepath}. Could not extract all required information.")
                    else:
                        logging.warning(f"grok variable in {filepath} does not have the expected number of elements.")
                except (ValueError, SyntaxError) as e:
                    logging.warning(f"Could not parse grok variable in {filepath} due to syntax error: {e}")
            else:
                # logging.info(f"No 'grok' variable found in {filepath}") # Uncomment for more verbosity
                pass
    except IOError as e:
        print(f"Error reading file {filepath}: {e}")
    return None, None, None

def check_url_status(url):
    """
    Checks the HTTP status of a given URL.
    Returns the status code as a string or an error message.
    """
    try:
        response = requests.get(url, timeout=10) # 10-second timeout to prevent hanging
        return str(response.status_code)
    except requests.exceptions.Timeout:
        return "Timeout"
    except requests.exceptions.ConnectionError:
        return "Connection Error"
    except requests.exceptions.RequestException as e:
        return f"Request Error: {type(e).__name__}"
    except Exception as e:
        return f"Unknown Error: {type(e).__name__}"
    
# --- New Email and Reporting Functions ---

def send_alert_email(subject, body_html, sender, recipients_str, smtp_host, smtp_port):
    """
    Constructs and sends an email alert.
    Logic adapted from SessionUsage.py for consistency.
    """
    receivers_list = [r.strip() for r in recipients_str.split(',') if r.strip()]
    if not receivers_list:
        logging.error("No recipients specified; email not sent.")
        return

    logging.info(f"Preparing to send email report to {recipients_str}...")
    
    message = MIMEMultipart('related')
    message['From'] = sender
    message['To'] = recipients_str
    message['Subject'] = subject

    message.attach(MIMEText(body_html, 'html'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp_server:
            smtp_server.sendmail(sender, receivers_list, message.as_string())
        logging.info(f"Email sent successfully to {recipients_str}.")
    except smtplib.SMTPException as e:
        logging.error(f"Failed to send email: {e}")

def create_failure_report_html(alerts):
    """
    Generates an HTML table for a given list of URL failure alerts.
    """
    if not alerts:
        return ""

    table_header = "<tr><th>Client</th><th>Environment</th><th>URL</th><th>Status</th></tr>"
    
    table_rows = ""
    for alert in alerts:
        # alert is a dict with keys: 'client', 'env', 'url', 'status'
        table_rows += f"<tr><td>{alert['client']}</td><td>{alert['env']}</td><td>{alert['url']}</td><td>{alert['status']}</td></tr>"

    return f"""
        <h3>Failed URL Checks</h3>
        <table>
            {table_header}
            {table_rows}
        </table>
    """

def fetch_and_email_failures(conn):
    """
    Fetches URLs with non-200 status from the DB and sends an email alert.
    """
    logging.info("Checking for URL failures to report...")
    failed_urls = []
    try:
        with conn.cursor() as cur:
            # This query gets the LATEST status for each unique URL and then filters
            # for those that do not have a '200' status.
            cur.execute("""
                WITH latest_status AS (
                    SELECT client, env, url, status,
                           ROW_NUMBER() OVER(PARTITION BY url ORDER BY check_timestamp DESC) as rn
                    FROM public.apm_url_response_status
                )
                SELECT client, env, url, status FROM latest_status WHERE rn = 1 AND status <> '200';
            """)
            rows = cur.fetchall()
            for row in rows:
                failed_urls.append({'client': row[0], 'env': row[1], 'url': row[2], 'status': row[3]})

    except psycopg2.Error as e:
        logging.error(f"Could not fetch failed URLs from database: {e}")
        return

    if not failed_urls:
        logging.info("No new URL failures found. No alert will be sent.")
        return

    logging.warning(f"Found {len(failed_urls)} URL(s) with a non-200 status. Preparing alert email.")

    # Create the HTML for the email body
    html_style = """<style>body { font-family: sans-serif; font-size: 14px; } table { border-collapse: collapse; width: 90%; margin: 20px 0; font-size: 12px; } th, td { border: 1px solid #dddddd; text-align: left; padding: 4px; } th { background-color: #f2f2f2; font-weight: bold; } tr:nth-child(even) { background-color: #f9f9f9; } h2 { color: #D9534F; font-size: 20px; } h3 { margin-top: 25px; color: #333; }</style>"""
    failure_table_html = create_failure_report_html(failed_urls)
    html_body = f"""<html><head>{html_style}</head><body><h2>URL Health Check Failure Alert</h2><p>The following URLs have reported a status other than <b>200 OK</b> on the most recent check.</p>{failure_table_html}<br><div>Thanks,</div><div>Oapm Team</div></body></html>"""

    # Send the email
    send_alert_email(subject=EMAIL_CONFIG["EMAIL_SUBJECT"], body_html=html_body, sender=EMAIL_CONFIG["SENDER_EMAIL"], recipients_str=EMAIL_CONFIG["RECEIVER_EMAILS_STR"], smtp_host=EMAIL_CONFIG["SMTP_SERVER_HOST"], smtp_port=EMAIL_CONFIG["SMTP_PORT"])
    
def create_url_status_table(conn):
    """
    Creates the apm_url_response_status table if it doesn't exist.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS apm_url_response_status (
                    id SERIAL PRIMARY KEY,
                    client TEXT NOT NULL,
                    env TEXT NOT NULL,
                    url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    check_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            logging.info("Table 'apm_url_response_status' ensured to exist.")
    except psycopg2.Error as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise # Re-raise to stop execution if table creation fails

def insert_url_status(conn, client, env, url, status):
    """
    Inserts a single URL status record into the database.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO apm_url_response_status (client, env, url, status)
                VALUES (%s, %s, %s, %s);
            """, (client, env, url, status))
            conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Error inserting data for URL {url}: {e}")
        conn.rollback()

# --- Main Execution ---

def main():
    results = []
    conn = None # Initialize connection to None
    logging.info(f"Starting URL status check in batch directories under: {BASE_PATH}")

    if not os.path.isdir(BASE_PATH):
        logging.error(f"Base path '{BASE_PATH}' does not exist or is not a directory. Please ensure the path is correct and accessible.")
        return

    try:
        # Establish database connection
        conn = psycopg2.connect(**DB_CONFIG)
        create_url_status_table(conn) # Ensure table exists

        for py_file in find_python_files_in_batch_dirs(BASE_PATH):
            logging.info(f"Processing file: {py_file}")
            urls, client, env = extract_grok_data(py_file)
            if urls:
                for url in urls:
                    status = check_url_status(url)
                    # Derive Client and Env from the URL itself.
                    client_from_url, env_from_url = extract_info_from_url(url)
                    
                    # Store in results for console output
                    results.append({"Client": client_from_url, "Env": env_from_url, "URL": url, "Status": status})
                    
                    # Insert into database
                    insert_url_status(conn, client_from_url, env_from_url, url, status)
         # After processing all files and inserting data, check for failures and send report
        fetch_and_email_failures(conn)
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

    if results:
        headers = ["Client", "Env", "URL", "Status"]
        table_data = [[row[h] for h in headers] for row in results]
        logging.info("--- URL Status Report (Console Output) ---")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
    else:
        logging.info("No URLs found or processed from the specified directories and files.")

if __name__ == "__main__":
    main()
