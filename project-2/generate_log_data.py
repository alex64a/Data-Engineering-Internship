import random
import datetime
import os

# Define constants for generating log data
NUM_LOGS = 1000  # Number of log entries to generate per file
COUNTRY_CODES = ["1101", "1102", "1103", "1104", "1105", "1106"]  # Example country codes
REGIONS = ["1", "2", "3", "4", "5", "6"]  # Regions
PRODUCTS = ["22752", "21730", "22633", "22632", "84879", "22745", "22748", "22749", "22310", "84969", "22623", "22622"]

# Create a folder named "logs" if it doesn't exist
if not os.path.exists("logs"):
    os.makedirs("logs")

def generate_invoice_date():
    """Generate a random invoice date within the last 30 days."""
    today = datetime.datetime.now()
    random_days = random.randint(0, 30)
    return (today - datetime.timedelta(days=random_days)).strftime("%d/%m/%Y %H:%M")

def generate_log_entry():
    """Generate a single log entry as a line of text."""
    invoice_no = random.randint(536365, 536999)  # Example invoice number range
    stock_code = random.choice(PRODUCTS)
    quantity = random.randint(1, 100)
    invoice_date = generate_invoice_date()
    customer_id = random.randint(13047, 17850)  # Example customer ID range
    country_code = random.choice(COUNTRY_CODES)
    region = random.choice(REGIONS)
    country = f"{country_code}-{region}"
    
    # Format the log entry as a comma-separated line of text
    return f"{invoice_no},{stock_code},{quantity},{invoice_date},{customer_id},{country}"

def generate_logs(num_logs, output_file):
    """Generate log data and write it to a text file."""
    with open(output_file, mode="w") as file:
        # Generate and write log entries
        for _ in range(num_logs):
            log_entry = generate_log_entry()
            file.write(log_entry + "\n")

    print(f"Generated {num_logs} log entries in {output_file}")

if __name__ == "__main__":
    # Generate a unique filename using the current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"logs/logs_{timestamp}.txt"  # Save as a text file
    
    # Generate logs
    generate_logs(NUM_LOGS, output_file)