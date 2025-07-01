# OAPM – Infrastructure Monitoring Automation

A Python-based infrastructure monitoring suite for automating:
- Oracle DB session usage monitoring
- URL health checks
- Debezium lag and error tracking with CSV reporting

## Features
- Automated client-wise health checks and intelligent alerts
- Prometheus API-based monitoring
- Cron-ready for scheduled runs
- Email notifications with HTML and CSV reports

## Tools & Tech
Python, Prometheus, Grafana, Cron, CSV, SMTP

## Scripts
- `SessionUsage.py`: Monitors Oracle DB sessions and sends alerts when thresholds exceed.
- `CheckURLResponse.py`: Scans URLs from batch directories, records their HTTP status, and reports failures.
- `connector.py`: Tracks Debezium lag, task errors, and offset commit failures with CSV-based reporting.

## Setup
1. Configure API URLs and email settings in each script.
2. Run the scripts as standalone or schedule them using Cron.

## Example Cron Entry
```bash
*/30 * * * * /<path>
