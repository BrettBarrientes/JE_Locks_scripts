import datetime
from datetime import datetime # Importing the datetime module for working with dates and times
from slack_sdk.webhook import WebhookClient
import requests # Importing the Requests library for making HTTP requests
import json
import redis # Importing the Redis library for interacting with Redis database
import logging
import data 

def log_write(log_message, logfile):
    logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s.%(msecs)06d: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    logging.info(log_message)

    logging.shutdown()



# Specify the path to the log file
logfile = data.path

# Log messages
log_write(f"Starting...", logfile)
log_write(f"Getting locks...", logfile)

# Specify the Redis host
redis_host = data.host

# Connect to Redis
r = redis.Redis(host=redis_host)

# Retrieve the number of lease history locks
lease_history_locks = sum(1 for key in r.scan_iter("Edit_*_L*"))

# Retrieve the number of journal entry locks
journal_entry_locks = sum(1 for key in r.scan_iter("Edit_*_J*"))

# Retrieve the total number of locks
total_locks = sum(1 for key in r.scan_iter("Edit_*"))

log_write("The number of locks on Redis is... (Drum roll please):", logfile)
log_write(f"{total_locks} locks", logfile)
log_write(f"{lease_history_locks} lease history locks", logfile)
log_write(f"{journal_entry_locks} journal entry locks", logfile)

log_write(f"Sending to Slack...", logfile)

# Set the webhook URL
slack_channel_uri = data.slack_webhook_uri
channel_name = "#locks"
user_name = "Lock Bot"

# Replace the username name in the webhook URL
slack_channel_uri = slack_channel_uri.replace("lock_bot", user_name)

# Create a WebhookClient object
webhook = WebhookClient(slack_channel_uri)

# Retrieve the current datetime without microseconds
current_datetime = datetime.now().replace(microsecond=0)

# Format the datetime as a string
formatted_datetime = current_datetime.strftime("%m/%d/%Y %H:%M:%S")

# Define the template for the Slack notification 
body_template = {
    "channel": "CHANNELNAME",
    "username": "Lock Bot",
    "text": "The number of locks on redis is... (Drum roll please):"
      "\n" "*NUMBER_OF_LOCKS* leases are currently locked."
      "\n" "Lease History locks: *LEASE_HISTORY_LOCKS*"
      "\n" "Journal Entry locks: *JOURNAL_ENTRY_LOCKS*"
      "\n" "Time: DATETIME."
} 

body = json.dumps(body_template)
body = body.replace("NUMBER_OF_LOCKS", str(total_locks)) # Replace placeholders with lock counts
body = body.replace("DATETIME", formatted_datetime) # Replace placeholder with current date and time
body = body.replace("CHANNELNAME", channel_name) # Replace placeholder with Slack channel name
body = body.replace("LEASE_HISTORY_LOCKS", str(lease_history_locks)) # Replace placeholder with lease history lock
body = body.replace("JOURNAL_ENTRY_LOCKS", str(journal_entry_locks)) # Replace placeholder with journal entry lock

# Send the message using the webhook
headers = {'Content-Type': 'application/json'} # Specify the headers for the HTTP request
response = requests.post(slack_channel_uri, data=body, headers=headers) # Send the Slack notification

log_write(f"Finished.", logfile)  # Write a log message indicating the completion of the script

print('Locks count processed and sent to Slack.')