

from com.azure.messaging.eventhubs import EventHubClientBuilder, EventHubProducerClient, EventDataBatch, EventData
from com.azure.identity import ClientSecretCredentialBuilder

# Define credentials (Replace with your actual values)
tenantId = "YOUR_TENANT_ID"
clientId = "YOUR_CLIENT_ID"
clientSecret = "YOUR_CLIENT_SECRET"
namespace = "YOUR_EVENT_HUB_NAMESPACE"  # e.g., "myeventhubnamespace"
eventHubName = "YOUR_EVENT_HUB_NAME"    # e.g., "myeventhub"

# Authenticate using ClientSecretCredential
credential = ClientSecretCredentialBuilder() \
    .tenantId(tenantId) \
    .clientId(clientId) \
    .clientSecret(clientSecret) \
    .build()

# Create EventHubProducerClient
producer = EventHubClientBuilder() \
    .fullyQualifiedNamespace(namespace + ".servicebus.windows.net") \
    .eventHubName(eventHubName) \
    .credential(credential) \
    .buildProducerClient()

# Create batch
batch = producer.createBatch()

# Add events
batch.tryAdd(EventData("Event from JMeter - 1"))
batch.tryAdd(EventData("Event from JMeter - 2"))
batch.tryAdd(EventData("Event from JMeter - 3"))

# Send batch
producer.send(batch)
log.info("Events sent successfully to Azure Event Hub")

# Close the producer
producer.close()





import com.azure.messaging.eventhubs.*
import com.azure.identity.ClientSecretCredential
import com.azure.identity.ClientSecretCredentialBuilder

// Environment Variables (Replace with your actual values)
String tenantId = "YOUR_TENANT_ID"
String clientId = "YOUR_CLIENT_ID"
String clientSecret = "YOUR_CLIENT_SECRET"
String namespace = "YOUR_EVENT_HUB_NAMESPACE" // e.g., myeventhubnamespace
String eventHubName = "YOUR_EVENT_HUB_NAME" // e.g., myeventhub

// Authenticate using ClientSecretCredential
ClientSecretCredential credential = new ClientSecretCredentialBuilder()
    .tenantId(tenantId)
    .clientId(clientId)
    .clientSecret(clientSecret)
    .build()

// Create EventHubProducerClient
EventHubProducerClient producer = new EventHubClientBuilder()
    .fullyQualifiedNamespace(namespace + ".servicebus.windows.net")
    .eventHubName(eventHubName)
    .credential(credential)
    .buildProducerClient()

// Create batch
EventDataBatch batch = producer.createBatch()

// Add events
batch.tryAdd(new EventData("Event from JMeter - 1"))
batch.tryAdd(new EventData("Event from JMeter - 2"))
batch.tryAdd(new EventData("Event from JMeter - 3"))

// Send batch
producer.send(batch)
log.info("Events sent successfully to Azure Event Hub")

// Close the producer
producer.close()




@cross_origin()
@jwt_required()
def post(self):
    """
    Assign or update shift timings for employees.
    :return: JSON response indicating success or failure.
    """
    try:
        data = request.get_json()
        employees = data.get('employees')

        if not employees:
            return jsonify({"error": "No employee data provided"}), 400

        with session_scope() as session:
            shift_entries = []

            for emp in employees:
                emp_id = emp.get("emp_id")
                from_date = datetime.strptime(emp.get("start_date"), DATE_FORMAT).date()
                to_date = datetime.strptime(emp.get("end_date"), DATE_FORMAT).date()
                week_off = emp.get("week_off", [])

                delta = (to_date - from_date).days + 1

                for i in range(delta):
                    current_date = from_date + timedelta(days=i)
                    day_id = current_date.weekday()

                    if day_id in week_off:
                        start_time = "00:00:00"
                        end_time = "00:00:00"
                    else:
                        start_time = emp.get("start_time", {}).get(str(day_id), "00:00:00")
                        end_time = emp.get("end_time", {}).get(str(day_id), "00:00:00")

                    # Calculate contracted hours
                    if start_time != "00:00:00" and end_time != "00:00:00":
                        contracted_hrs = round(
                            (datetime.strptime(end_time, TIME_FORMAT) - datetime.strptime(start_time, TIME_FORMAT)).total_seconds() / 3600, 2
                        )
                    else:
                        contracted_hrs = 0.0

                    # Check if a shift already exists for this employee and shift date
                    existing_shift = session.query(EmployeeShiftInfo).filter_by(
                        emp_id=emp_id,
                        shift_date=current_date
                    ).first()

                    if existing_shift:
                        # Update the existing shift
                        existing_shift.day_id = day_id
                        existing_shift.is_week_off = "Y" if day_id in week_off else "N"
                        existing_shift.start_time = datetime.strptime(start_time, TIME_FORMAT) if start_time != "00:00:00" else None
                        existing_shift.end_time = datetime.strptime(end_time, TIME_FORMAT) if end_time != "00:00:00" else None
                        existing_shift.emp_contracted_hours = contracted_hrs
                    else:
                        # Insert new shift
                        shift_entries.append(
                            EmployeeShiftInfo(
                                emp_id=emp_id,
                                day_id=day_id,
                                is_week_off="Y" if day_id in week_off else "N",
                                start_time=datetime.strptime(start_time, TIME_FORMAT) if start_time != "00:00:00" else None,
                                end_time=datetime.strptime(end_time, TIME_FORMAT) if end_time != "00:00:00" else None,
                                time_zone_id=None,
                                emp_contracted_hours=contracted_hrs,
                                shift_date=current_date
                            )
                        )

            # Bulk insert new records if any
            if shift_entries:
                session.bulk_save_objects(shift_entries)

            session.commit()

            return jsonify({"message": "Shift(s) assigned or updated successfully!", "status": "success"}), 200

    except Exception as e:
        return jsonify({
            "error": "An unexpected error occurred.",
            "details": str(e)
        }), 500




To run the job every Monday at 12:02 AM, you just need to modify the APScheduler job trigger in the start_scheduler() function.

Updated Job Schedule

def start_scheduler():
    """Starts the APScheduler to run the task every Monday at 12:02 AM"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(copy_previous_week_efforts, 'cron', day_of_week='mon', hour=0, minute=2)  # Runs every Monday at 12:02 AM
    scheduler.start()
    print("Scheduler started: Running copy_previous_week_efforts every Monday at 12:02 AM")


---

Updated Date Calculations

We need to adjust the week calculations so that we copy shifts from last Monday to last Sunday into the new Monday to Sunday.

Updated copy_previous_week_efforts Function

from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import sessionmaker
from models import EmployeeShiftInfo, EmployeeInfo
from database import session_scope

def copy_previous_week_efforts():
    """Efficiently copies last week's efforts to the current week for all employees"""
    
    with session_scope("DESIGNER") as session:
        today = datetime.today()
        last_monday = today - timedelta(days=today.weekday() + 7)  # Last Monday
        last_sunday = last_monday + timedelta(days=6)  # Last Sunday
        current_monday = last_sunday + timedelta(days=1)  # Current Monday
        current_sunday = current_monday + timedelta(days=6)  # Current Sunday

        print(f"Copying shifts from {last_monday} - {last_sunday} to {current_monday} - {current_sunday}")

        # Step 1: Fetch all employee IDs in batches
        employees = session.query(EmployeeInfo.emplid).all()
        employee_ids = [emp.emplid for emp in employees]

        # Step 2: Fetch all shifts for the last week
        last_week_shifts = session.query(EmployeeShiftInfo).filter(
            EmployeeShiftInfo.emp_id.in_(employee_ids),
            EmployeeShiftInfo.shift_date.between(last_monday, last_sunday)
        ).all()

        # Step 3: Fetch all shifts for the current week
        current_week_shifts = session.query(EmployeeShiftInfo).filter(
            EmployeeShiftInfo.emp_id.in_(employee_ids),
            EmployeeShiftInfo.shift_date.between(current_monday, current_sunday)
        ).all()

        # Step 4: Use dictionaries for quick lookups
        last_week_shift_map = {(shift.emp_id, shift.shift_date): shift for shift in last_week_shifts}
        current_week_shift_dates = {(shift.emp_id, shift.shift_date) for shift in current_week_shifts}

        batch_insert = []
        default_start_time = "09:00:00"
        default_end_time = "17:00:00"

        for emp_id in employee_ids:
            for i in range(7):  # Loop through 7 days of the current week
                new_shift_date = current_monday + timedelta(days=i)
                
                if (emp_id, new_shift_date) in current_week_shift_dates:
                    continue  # Skip if shift already exists

                # Copy shift from last week if exists, otherwise assign default
                prev_shift_date = new_shift_date - timedelta(days=7)
                prev_shift = last_week_shift_map.get((emp_id, prev_shift_date))

                batch_insert.append(EmployeeShiftInfo(
                    emp_id=emp_id,
                    shift_date=new_shift_date,
                    start_time=prev_shift.start_time if prev_shift else default_start_time,
                    end_time=prev_shift.end_time if prev_shift else default_end_time,
                    is_week_off=prev_shift.is_week_off if prev_shift else "N"
                ))

        # Step 5: Bulk insert only if there are new shifts to add
        if batch_insert:
            session.bulk_save_objects(batch_insert)
            session.commit()
            print(f"{len(batch_insert)} shift records inserted.")

def start_scheduler():
    """Starts the APScheduler to run the task every Monday at 12:02 AM"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(copy_previous_week_efforts, 'cron', day_of_week='mon', hour=0, minute=2)  # Runs every Monday at 12:02 AM
    scheduler.start()
    print("Scheduler started: Running copy_previous_week_efforts every Monday at 12:02 AM")

if __name__ == "__main__":
    start_scheduler()
    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


---

Key Changes

✅ Job now runs every Monday at 12:02 AM
✅ Correctly calculates last Monday–Sunday efforts to copy into the new week
✅ Efficient batch processing for large employee datasets

This ensures the job copies last week’s efforts into the new week properly without duplication. Let me know if you need any tweaks!







from flask import Flask, jsonify
import os
import json
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from apscheduler.schedulers.background import BackgroundScheduler
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

app = Flask(__name__)

# Environment Variables
KEY_VAULT_NAME_GEN = os.getenv("KEY_VAULT_NAME_GEN")
KEY_VAULT_NAME_SEC = os.getenv("KEY_VAULT_NAME_SEC")  # Another KeyVault
SECRET_NAMES = os.getenv("SECRETS_REQUIRED").split(",")

# Azure Key Vault Clients
credential = DefaultAzureCredential()
client_gen = SecretClient(vault_url=f"https://{KEY_VAULT_NAME_GEN}.vault.azure.net/", credential=credential)
client_sec = SecretClient(vault_url=f"https://{KEY_VAULT_NAME_SEC}.vault.azure.net/", credential=credential)

# AES Encryption Settings
AES_KEY = os.getenv("AES_KEY")  # Use a secure key from env
AES_NONCE = os.getenv("AES_NONCE")  # Use a secure nonce from env

# Function to Encrypt Data
def encrypt_data(data, key, nonce):
    cipher = Cipher(algorithms.AES(key.encode()), modes.GCM(nonce.encode()))
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(data.encode()) + encryptor.finalize()
    return base64.b64encode(encrypted_data).decode()

# Scheduler Function
def check_and_encrypt_client_secret():
    """ Checks expiry of 'clsscrt' in second KeyVault and encrypts it if within 20 days. """
    try:
        secret_properties = client_sec.get_secret("clsscrt").properties
        expiry_date = secret_properties.expires_on

        if expiry_date is None:
            print("No expiration date found for 'clsscrt'. Skipping...")
            return

        days_remaining = (expiry_date - datetime.utcnow()).days
        print(f"Days remaining for 'clsscrt' expiry: {days_remaining}")

        if days_remaining <= 20:
            print("Less than 20 days remaining. Updating encrypted secret...")
            
            # Get and Encrypt the Secret
            clsscrt_secret = client_sec.get_secret("clsscrt").value
            encrypted_secret = encrypt_data(clsscrt_secret, AES_KEY, AES_NONCE)

            # Store Encrypted Secret in First Key Vault
            client_gen.set_secret("enc_client_secret", encrypted_secret)
            print("Encrypted secret updated in Key Vault.")

    except Exception as e:
        print(f"Error in checking/encrypting secret: {str(e)}")

# Schedule the Task (Runs daily at midnight)
scheduler = BackgroundScheduler()
scheduler.add_job(check_and_encrypt_client_secret, "cron", hour=0, minute=0)
scheduler.start()

# API Endpoint to Get Secrets
@app.get("/get_configs")
async def get_keyvault_data():
    """ Fetch secrets from Key Vault and return as JSON. """
    secret_dict = {}
    
    for secret_name in SECRET_NAMES:
        try:
            secret = client_gen.get_secret(secret_name.strip())
            secret_dict[secret_name] = secret.value
        except Exception as e:
            secret_dict[secret_name] = f"Error: {str(e)}"

    return jsonify(secret_dict)

if __name__ == "__main__":
    app.run(debug=True)
















from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, WebDriverException
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def validate_unsupported_media_size():
    """
    This method verifies that an unsupported media size cannot be assigned to a dedicated tray.
    """

    # Test Data
    url = "http://example.com"  # Replace with the actual URL
    tray_locator = "tray-selector"  # Replace with the actual locator for the tray dropdown
    unsupported_media_size = "Unsupported Size"  # Replace with the actual unsupported size
    error_message_locator = "error-message"  # Replace with the actual locator for the error message
    expected_error_message = "Unsupported media size cannot be assigned to this tray."  # Replace with the actual expected error message

    # Initialize WebDriver
    driver = None

    try:
        logging.info("Launching the browser...")
        driver = webdriver.Chrome()  # Ensure the correct WebDriver is installed and configured
        driver.maximize_window()

        logging.info(f"Navigating to the URL: {url}")
        driver.get(url)

        logging.info("Locating the tray dropdown...")
        tray_dropdown = driver.find_element(By.ID, tray_locator)

        logging.info(f"Selecting the unsupported media size: {unsupported_media_size}")
        tray_dropdown.send_keys(unsupported_media_size)
        tray_dropdown.send_keys(Keys.RETURN)

        logging.info("Verifying the error message...")
        error_message_element = driver.find_element(By.ID, error_message_locator)
        actual_error_message = error_message_element.text

        assert actual_error_message == expected_error_message, (
            f"Test Failed: Expected error message '{expected_error_message}', but got '{actual_error_message}'"
        )

        logging.info("Test Passed: The correct error message is displayed for unsupported media size.")

    except NoSuchElementException as e:
        logging.error(f"Test Failed: Element not found - {e}")

    except AssertionError as e:
        logging.error(e)

    except WebDriverException as e:
        logging.error(f"Test Failed: WebDriver exception occurred - {e}")

    except Exception as e:
        logging.error(f"Test Failed: An unexpected exception occurred - {e}")

    finally:
        if driver:
            logging.info("Closing the browser...")
            driver.quit()

# Execute the test
validate_unsupported_media_size()



from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timezone
from Crypto.Cipher import AES
import base64

app = Flask(__name__)

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://your-keyvault-name.vault.azure.net"
SECRET_NAME = "client-secret-name"
ENCRYPTED_SECRET_NAME = "encrypted-client-secret"
LAST_ENCRYPTION_TIMESTAMP = "last-encryption-timestamp"  # Store last encryption time

# Initialize Key Vault Client
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

def check_and_encrypt_secret():
    """
    Function to check secret expiry and re-encrypt if necessary.
    """
    try:
        # Fetch secret properties
        secret = client.get_secret(SECRET_NAME)
        expires_on = secret.properties.expires_on

        # Check if expiry is within 20 days
        if expires_on:
            days_remaining = (expires_on - datetime.now(timezone.utc)).days
            if days_remaining <= 20:
                print(f"Secret expires in {days_remaining} days. Checking re-encryption status...")

                # Fetch last encryption timestamp (if exists)
                try:
                    last_encryption_time = client.get_secret(LAST_ENCRYPTION_TIMESTAMP).value
                    last_encryption_time = datetime.fromisoformat(last_encryption_time).replace(tzinfo=timezone.utc)
                except:
                    last_encryption_time = None  # No timestamp exists

                # Encrypt only if it hasn't been done already within this 20-day window
                if last_encryption_time and last_encryption_time >= expires_on - timedelta(days=20):
                    print("Secret has already been encrypted within this window. Skipping.")
                    return

                # Fetch the actual secret value
                secret_value = secret.value

                # Fetch encryption key & nonce from Key Vault
                encryption_key = base64.b64decode(client.get_secret("encryption-key").value)
                nonce = base64.b64decode(client.get_secret("encryption-nonce").value)

                # Encrypt secret using AES-256-GCM
                cipher = AES.new(encryption_key, AES.MODE_GCM, nonce=nonce)
                encrypted_bytes, tag = cipher.encrypt_and_digest(secret_value.encode())

                # Convert to Base64 for storage
                encrypted_value = base64.b64encode(encrypted_bytes).decode()

                # Store encrypted secret back in Key Vault
                client.set_secret(ENCRYPTED_SECRET_NAME, encrypted_value)

                # Store the last encryption timestamp
                client.set_secret(LAST_ENCRYPTION_TIMESTAMP, datetime.now(timezone.utc).isoformat())

                print(f"Updated encrypted value stored in {ENCRYPTED_SECRET_NAME}. Encryption timestamp updated.")
            else:
                print("Secret expiry is not within 20 days. No action taken.")
        else:
            print("Secret does not have an expiration date.")
    except Exception as e:
        print(f"Error during secret check: {e}")

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(check_and_encrypt_secret, "interval", hours=24)  # Run every 24 hours
scheduler.start()

@app.route("/")
def home():
    return "Flask API with Scheduler Running..."

if __name__ == "__main__":
    try:
        app.run(debug=True, use_reloader=False)  # Use reloader=False to prevent duplicate scheduler execution
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()








def decrypt_value(encrypted_obj, key_b64, nonce_b64):
    """Decrypt a single value using AES-256-GCM"""
    key = base64.b64decode(key_b64)
    nonce = base64.b64decode(nonce_b64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    decrypted_text = cipher.decrypt_and_verify(
        base64.b64decode(encrypted_obj["ciphertext"]),
        base64.b64decode(encrypted_obj["auth_tag"])
    )

    return decrypted_text.decode()

# Decrypt all values using the encrypted_store dictionary
decrypted_data = {key: decrypt_value(value, aes_key, aes_nonce) for key, value in encrypted_store.items()}

# Print decrypted values
print("\n🔓 Decrypted Data Dictionary:")
print(json.dumps(decrypted_data, indent=4))

Got it! You want to:

1. Encrypt each of the 5 values (ClientID, TenantID, ClientSecret, ServiceName, ServicePassword) individually using AES-256-GCM with a single key and nonce.


2. Store the encrypted values in Azure Key Vault instead of plain text.


3. Later, retrieve and decrypt each value individually using the same key and nonce in an API call.



🔹 Steps to Achieve This:

1. Generate AES-256 Key & Nonce → This key and nonce should be stored securely (not in Key Vault).


2. Encrypt each value separately and store the encrypted version in Azure Key Vault.


3. Retrieve & Decrypt each value in your API using the same key and nonce.




---

🔐 Step 1: Generate AES-256 Key & Nonce

import os
import base64
from Crypto.Random import get_random_bytes

def generate_aes_key_nonce():
    """Generate a 256-bit AES key and a nonce, then print/store them securely"""
    key = get_random_bytes(32)  # AES-256 requires a 32-byte key
    nonce = get_random_bytes(16)  # Nonce for AES-GCM (must be unique per encryption)

    print("AES Key (Base64):", base64.b64encode(key).decode())  
    print("Nonce (Base64):", base64.b64encode(nonce).decode())

    # Store these values securely (Environment Variables, Secure Storage, etc.)
    return key, nonce

if __name__ == "__main__":
    generate_aes_key_nonce()

🔹 Output Example (Save These Securely)

AES Key (Base64): dGhpc2lzYXRlc3RrZXkzM0JZVEU=
Nonce (Base64): c29tZWZha2Vub25jZVRlc3Q=

✅ Store these in a secure place (not in Key Vault but in a secure config or environment variable).
✅ The same key and nonce will be used for all five values.


---

🔐 Step 2: Encrypt Individual Values

import base64
import json
from Crypto.Cipher import AES

# Store the generated Key & Nonce here (from Step 1)
AES_KEY_BASE64 = "dGhpc2lzYXRlc3RrZXkzM0JZVEU="  # Replace with actual key
NONCE_BASE64 = "c29tZWZha2Vub25jZVRlc3Q="  # Replace with actual nonce

def encrypt_value(plain_text):
    """Encrypt a single value using AES-256-GCM"""
    key = base64.b64decode(AES_KEY_BASE64)
    nonce = base64.b64decode(NONCE_BASE64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, auth_tag = cipher.encrypt_and_digest(plain_text.encode())

    return {
        "ciphertext": base64.b64encode(ciphertext).decode(),
        "auth_tag": base64.b64encode(auth_tag).decode()
    }

# Example: Encrypting the five sensitive values
sensitive_data = {
    "ClientID": "12345-xyz-client",
    "TenantID": "67890-abc-tenant",
    "ClientSecret": "MySuperSecureSecret",
    "ServiceName": "SecureAPIService",
    "ServicePassword": "UltraSafePassword"
}

encrypted_data = {key: encrypt_value(value) for key, value in sensitive_data.items()}

# Print Encrypted Values (Store these in Key Vault)
print("\n🔐 Encrypted Data (Store in Key Vault):")
print(json.dumps(encrypted_data, indent=4))


---

🔹 Step 3: Store in Azure Key Vault

Once the values are encrypted, store them in Azure Key Vault using the Azure CLI or SDK.

az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedClientID" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedTenantID" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedClientSecret" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedServiceName" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedServicePassword" --value "CIPHERTEXT_FROM_SCRIPT"


---

🔓 Step 4: Decrypt Individual Values in API

When retrieving values from Key Vault, use this decryption function:

import base64
from Crypto.Cipher import AES

# Load AES Key & Nonce (same as before)
AES_KEY_BASE64 = "dGhpc2lzYXRlc3RrZXkzM0JZVEU="  # Use same stored key
NONCE_BASE64 = "c29tZWZha2Vub25jZVRlc3Q="  # Use same stored nonce

def decrypt_value(ciphertext, auth_tag):
    """Decrypt a single value using AES-256-GCM"""
    key = base64.b64decode(AES_KEY_BASE64)
    nonce = base64.b64decode(NONCE_BASE64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    decrypted_text = cipher.decrypt_and_verify(base64.b64decode(ciphertext), base64.b64decode(auth_tag))

    return decrypted_text.decode()

# Example: Decrypting one retrieved value (replace with values from Key Vault)
encrypted_client_id = {
    "ciphertext": "CIPHERTEXT_FROM_KEY_VAULT",
    "auth_tag": "AUTHTAG_FROM_KEY_VAULT"
}

decrypted_client_id = decrypt_value(encrypted_client_id["ciphertext"], encrypted_client_id["auth_tag"])

print("\n🔓 Decrypted Client ID:", decrypted_client_id)


---

🔹 How It Works

✅ Encryption:

Each value is encrypted individually using the same key and nonce.

The ciphertext and auth tag are stored in Azure Key Vault.


✅ Decryption (API):

Retrieve ciphertext and auth tag from Key Vault.

Use same AES key and nonce to decrypt the values.



---

🔹 Example API Call Flow

Your API will:

1. Fetch Encrypted Data from Azure Key Vault.


2. Decrypt using decrypt_value().


3. Return Decrypted Values in API Response.



Example API Call:

from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/get-secrets", methods=["GET"])
def get_secrets():
    """Fetch encrypted values from Key Vault & decrypt them"""
    decrypted_values = {
        "ClientID": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "TenantID": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "ClientSecret": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "ServiceName": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "ServicePassword": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT")
    }
    return jsonify(decrypted_values)

if __name__ == "__main__":
    app.run(debug=True)


---

🔹 Summary

✔ AES-256-GCM encryption with a single key & nonce
✔ Stores encrypted values in Azure Key Vault
✔ Decryption in API using the same key & nonce
✔ Individually encrypts & decrypts each value

This ensures strong encryption while keeping secrets safe in Azure Key Vault. Let me know if you need changes! 🚀










from flask import Flask, Blueprint, jsonify
import requests
import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# Flask Blueprint
api_bp = Blueprint("api", __name__)

# API URL to call
API_URL = "https://example.com/api/data"

def fetch_api_data():
    """Function to call API every Sunday and process response."""
    print(f"Running API call at {datetime.datetime.now()}...")
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            print("API Response:", data)
            # Process data here (e.g., save to DB)
        else:
            print(f"Error: {response.status_code}")
    except Exception as e:
        print("API Call Failed:", str(e))

# Set up scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_api_data, "cron", day_of_week="sun", hour=0, minute=0)  # Runs every Sunday at 00:00
scheduler.start()

@api_bp.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Flask API with Cron Job Running!"})

# Register Blueprint
app.register_blueprint(api_bp, url_prefix="/api")

if __name__ == "__main__":
    app.run(debug=True)





Yes, you are absolutely right! The earlier approach is not fully secure because the client can generate a token without any protection. Instead, we need a more secure approach, where:

1. The server generates and stores the secret key in memory (or an environment variable).


2. The client also stores the secret key securely (in memory, not hardcoded in code).


3. Before making a request, the client encodes a new token using the secret key.


4. The server decodes and verifies it before granting access.


5. Tokens are not issued by an open endpoint—only pre-configured systems with the key can generate them.




---

Improved Secure JWT Implementation

This method ensures:

Clients cannot get tokens without the secret key.

Tokens expire automatically.

Each request includes a freshly signed JWT for verification.



---

Step 1: Server (server.py)

from fastapi import FastAPI, Depends, HTTPException, Header
import jwt
import time
import os

app = FastAPI()

# Store secret key in an environment variable or memory (DO NOT expose)
SECRET_KEY = os.getenv("JWT_SECRET", "my-very-secure-secret-key")  
ALGORITHM = "HS256"
TOKEN_EXPIRY_SECONDS = 300  # Token expires in 5 minutes

def verify_jwt(token: str = Header(None)):
    """Verify JWT token received in headers."""
    if not token:
        raise HTTPException(status_code=403, detail="Missing token")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=403, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=403, detail="Invalid token")

    return True  # Authentication successful

@app.get("/protected")
def protected_route(auth: bool = Depends(verify_jwt)):
    """A protected endpoint that requires a valid JWT token."""
    return {"message": "Access granted! JWT verified."}

@app.get("/")
def public_route():
    """A public endpoint."""
    return {"message": "Welcome to the Secure API!"}


---

Step 2: Client (client.py)

import jwt
import time
import os
import requests

BASE_URL = "http://127.0.0.1:8000"

# Client must have the secret key stored securely (in environment or memory)
SECRET_KEY = os.getenv("JWT_SECRET", "my-very-secure-secret-key")  
ALGORITHM = "HS256"

def create_jwt():
    """Generate a JWT token on the client before making a request."""
    expiration_time = int(time.time()) + 300  # Token valid for 5 minutes
    payload = {"exp": expiration_time}
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token

# Generate token in memory
token = create_jwt()

# Make a request to the protected endpoint with the token
headers = {"token": token}
response = requests.get(f"{BASE_URL}/protected", headers=headers)

print("Response:", response.json())


---

Step 3: Run the Secure System

1. Start the FastAPI Server

uvicorn server:app --reload

2. Run the Client

python client.py


---

Why is This Secure?

✅ No Open Token Generation Endpoint → Only pre-configured clients can generate tokens.
✅ Client Stores Secret Key Securely → No unauthorized token generation.
✅ Each Request Includes a Fresh Token → No reuse of stolen tokens.
✅ Server Only Verifies Token → Stateless and efficient authentication.
✅ Environment Variables Used for Keys → Prevents exposing secrets in code.


---

Key Takeaway

Instead of the server issuing tokens, the client encodes its own JWT before sending requests.

The server only verifies tokens, ensuring that only trusted clients can make requests.

This method is stateless, secure, and ensures only pre-approved clients can communicate.


Would this work for your setup? Let me know if you need modifications!









import base64
import win32crypt

def store_encryption_key(encryption_key):
    """Encrypt and store the AES decryption key securely using DPAPI"""
    encrypted_key = win32crypt.CryptProtectData(encryption_key.encode(), None, None, None, None, 0)
    with open("encrypted_key.bin", "wb") as f:
        f.write(base64.b64encode(encrypted_key))

def retrieve_encryption_key():
    """Retrieve and decrypt the AES decryption key securely using DPAPI"""
    with open("encrypted_key.bin", "rb") as f:
        encrypted_key = base64.b64decode(f.read())
    decrypted_key = win32crypt.CryptUnprotectData(encrypted_key, None, None, None, None, 0)
    return decrypted_key[1].decode()

# Example Usage
aes_key = "SuperSecureAESKey123"  # This is your actual decryption key
store_encryption_key(aes_key)  # Store securely
retrieved_key = retrieve_encryption_key()  # Retrieve securely
print(f"Decryption Key: {retrieved_key}")







Here’s a standalone encryption and decryption script using AES-256-GCM, with dummy sensitive data like Client ID, Password, and 13 different proxies stored as a key-value pair with country, country code, and proxy names.


---

🔐 AES-256-GCM Encryption & Decryption

This script: ✅ Encrypts sensitive data using AES-256-GCM.
✅ Uses a secure key from environment variables.
✅ Stores an IV (nonce) and authentication tag.
✅ Decrypts and verifies data at runtime.


---

🔹 Full Python Code

import os
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Securely generate/store AES-256-GCM key in environment variable
AES_KEY_ENV = "AES_256_GCM_KEY"

def generate_and_store_key():
    """Generate a new AES-256 key and store it in an environment variable"""
    key = get_random_bytes(32)  # 256-bit key
    os.environ[AES_KEY_ENV] = base64.b64encode(key).decode()  # Store in env as base64

def get_aes_key():
    """Retrieve AES key from environment variable"""
    key_b64 = os.getenv(AES_KEY_ENV)
    if not key_b64:
        raise ValueError("AES Key not found. Please generate and store it securely.")
    return base64.b64decode(key_b64)

def encrypt_data(plaintext):
    """Encrypt data using AES-256-GCM"""
    key = get_aes_key()
    iv = get_random_bytes(16)  # 16-byte IV (Nonce)
    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext.encode())

    return {
        "ciphertext": base64.b64encode(ciphertext).decode(),
        "iv": base64.b64encode(iv).decode(),
        "auth_tag": base64.b64encode(auth_tag).decode()
    }

def decrypt_data(ciphertext_b64, iv_b64, auth_tag_b64):
    """Decrypt data using AES-256-GCM"""
    key = get_aes_key()
    iv = base64.b64decode(iv_b64)
    auth_tag = base64.b64decode(auth_tag_b64)
    ciphertext = base64.b64decode(ciphertext_b64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    plaintext = cipher.decrypt_and_verify(ciphertext, auth_tag)

    return plaintext.decode()

# ---------- Example Data ----------
sensitive_data = {
    "ClientID": "XYZ-12345-CLIENT",
    "TenantID": "TENANT-56789",
    "ClientSecret": "MySuperSecureSecret!@#",
    "ServiceAccountName": "service-agent-01",
    "ServiceAccountPassword": "AgentPassword$321",
    "Proxies": [
        {"Country": "United States", "Code": "US", "Proxy": "proxy-us.example.com:8080"},
        {"Country": "Canada", "Code": "CA", "Proxy": "proxy-ca.example.com:8080"},
        {"Country": "Germany", "Code": "DE", "Proxy": "proxy-de.example.com:8080"},
        {"Country": "France", "Code": "FR", "Proxy": "proxy-fr.example.com:8080"},
        {"Country": "United Kingdom", "Code": "UK", "Proxy": "proxy-uk.example.com:8080"},
        {"Country": "India", "Code": "IN", "Proxy": "proxy-in.example.com:8080"},
        {"Country": "Australia", "Code": "AU", "Proxy": "proxy-au.example.com:8080"},
        {"Country": "Japan", "Code": "JP", "Proxy": "proxy-jp.example.com:8080"},
        {"Country": "Brazil", "Code": "BR", "Proxy": "proxy-br.example.com:8080"},
        {"Country": "South Africa", "Code": "ZA", "Proxy": "proxy-za.example.com:8080"},
        {"Country": "Russia", "Code": "RU", "Proxy": "proxy-ru.example.com:8080"},
        {"Country": "Mexico", "Code": "MX", "Proxy": "proxy-mx.example.com:8080"},
        {"Country": "Italy", "Code": "IT", "Proxy": "proxy-it.example.com:8080"}
    ]
}

# ---------- Encrypt & Decrypt Data ----------
if __name__ == "__main__":
    # Step 1: Generate and store key (Run only once)
    if not os.getenv(AES_KEY_ENV):
        generate_and_store_key()
        print("AES key generated and stored in environment.")

    # Step 2: Encrypt and store sensitive data
    encrypted_data = {}
    for key, value in sensitive_data.items():
        if isinstance(value, list):  # Handle lists separately (like proxies)
            encrypted_data[key] = [encrypt_data(str(item)) for item in value]
        else:
            encrypted_data[key] = encrypt_data(value)

    print("\n🔐 Encrypted Data:")
    print(encrypted_data)

    # Step 3: Decrypt and retrieve original data
    decrypted_data = {}
    for key, value in encrypted_data.items():
        if isinstance(value, list):
            decrypted_data[key] = [decrypt_data(item["ciphertext"], item["iv"], item["auth_tag"]) for item in value]
        else:
            decrypted_data[key] = decrypt_data(value["ciphertext"], value["iv"], value["auth_tag"])

    print("\n🔓 Decrypted Data:")
    print(decrypted_data)


---

🔹 How It Works?

1️⃣ AES-256-GCM Key Management

The 256-bit encryption key is stored in an environment variable.

Ensures security by keeping the key outside the code/database.


2️⃣ Encryption

Uses AES-256-GCM mode for authenticated encryption.

Generates a unique IV (nonce) for each encryption.

Produces an authentication tag to ensure data integrity.


3️⃣ Storage & Structure

The script encrypts both string values (ClientID, Password, etc.) and list objects (Proxies).

Stores the ciphertext, IV, and authentication tag for each entry.


4️⃣ Decryption

Retrieves encrypted values, verifies authenticity, and decrypts them securely.



---

🔹 Sample Output

🔐 Encrypted Data:
{
    "ClientID": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "TenantID": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "ClientSecret": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "ServiceAccountName": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "ServiceAccountPassword": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "Proxies": [
        {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
        {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
        ...
    ]
}

🔓 Decrypted Data:
{
    "ClientID": "XYZ-12345-CLIENT",
    "TenantID": "TENANT-56789",
    "ClientSecret": "MySuperSecureSecret!@#",
    "ServiceAccountName": "service-agent-01",
    "ServiceAccountPassword": "AgentPassword$321",
    "Proxies": [
        {"Country": "United States", "Code": "US", "Proxy": "proxy-us.example.com:8080"},
        {"Country": "Canada", "Code": "CA", "Proxy": "proxy-ca.example.com:8080"},
        ...
    ]
}


---

🔹 Why Use This?

✅ AES-256-GCM provides both encryption & authentication.
✅ Secure key storage prevents hardcoded keys.
✅ Encrypts both simple values & lists of objects (like proxies).
✅ Tamper-proof authentication ensures integrity.


---

🔹 Next Steps

1️⃣ Store encrypted data in SQLite or Key Vault instead of keeping it in memory.
2️⃣ Use an API to fetch credentials when needed instead of re-encrypting every time.
3️⃣ Rotate encryption keys periodically for better security.


---

🚀 Would you like me to add SQLite storage for this encrypted data?







End-to-End AES-256-GCM Encryption & Decryption in Python

This implementation ensures secure storage of encrypted credentials in an SQLite database, with the AES key stored securely in environment variables.


---

🔹 Steps

1. Generate a secure AES-256-GCM key (stored in an environment variable).


2. Encrypt sensitive data (ClientID, ClientSecret, etc.).


3. Store encrypted data, IV (nonce), and authentication tag in SQLite.


4. Retrieve and decrypt the data securely at runtime.




---

🔐 Full Python Code

import os
import sqlite3
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Securely generate or load the AES-256 key
AES_KEY_ENV = "AES_256_GCM_KEY"

def generate_and_store_key():
    """Generate a new AES-256 key and store it in an environment variable"""
    key = get_random_bytes(32)  # 256-bit key
    os.environ[AES_KEY_ENV] = base64.b64encode(key).decode()  # Store in env as base64

def get_aes_key():
    """Retrieve AES key from environment variable"""
    key_b64 = os.getenv(AES_KEY_ENV)
    if not key_b64:
        raise ValueError("AES Key not found. Please generate and store it securely.")
    return base64.b64decode(key_b64)

def encrypt_data(plaintext):
    """Encrypt data using AES-256-GCM"""
    key = get_aes_key()  # Load AES key
    iv = get_random_bytes(16)  # 16-byte IV (Nonce)
    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext.encode())

    return {
        "ciphertext": base64.b64encode(ciphertext).decode(),
        "iv": base64.b64encode(iv).decode(),
        "auth_tag": base64.b64encode(auth_tag).decode()
    }

def decrypt_data(ciphertext_b64, iv_b64, auth_tag_b64):
    """Decrypt data using AES-256-GCM"""
    key = get_aes_key()
    iv = base64.b64decode(iv_b64)
    auth_tag = base64.b64decode(auth_tag_b64)
    ciphertext = base64.b64decode(ciphertext_b64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    plaintext = cipher.decrypt_and_verify(ciphertext, auth_tag)

    return plaintext.decode()

# ---------- SQLite Storage ----------
DB_FILE = "secure_storage.db"

def init_db():
    """Initialize SQLite database with a table for storing encrypted credentials"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_name TEXT UNIQUE NOT NULL,
            ciphertext TEXT NOT NULL,
            iv TEXT NOT NULL,
            auth_tag TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def store_encrypted_data(key_name, encrypted_data):
    """Store encrypted credentials in the SQLite database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO credentials (key_name, ciphertext, iv, auth_tag)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key_name) DO UPDATE SET 
        ciphertext=excluded.ciphertext, iv=excluded.iv, auth_tag=excluded.auth_tag
    """, (key_name, encrypted_data["ciphertext"], encrypted_data["iv"], encrypted_data["auth_tag"]))
    conn.commit()
    conn.close()

def retrieve_encrypted_data(key_name):
    """Retrieve encrypted credentials from the database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT ciphertext, iv, auth_tag FROM credentials WHERE key_name=?", (key_name,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {"ciphertext": row[0], "iv": row[1], "auth_tag": row[2]}
    return None

# ---------- Example Usage ----------

if __name__ == "__main__":
    # Step 1: Generate and store key (Run only once)
    if not os.getenv(AES_KEY_ENV):
        generate_and_store_key()
        print("AES key generated and stored in environment.")

    # Step 2: Initialize database
    init_db()

    # Step 3: Encrypt and store sensitive data
    credentials = {
        "ClientID": "my-client-id",
        "TenantID": "my-tenant-id",
        "ClientSecret": "my-very-secure-client-secret",
        "ServiceAccountName": "my-service-account",
        "ServiceAccountPassword": "my-secure-password"
    }

    for key_name, value in credentials.items():
        encrypted_data = encrypt_data(value)
        store_encrypted_data(key_name, encrypted_data)
        print(f"Stored encrypted {key_name} in database.")

    # Step 4: Retrieve and decrypt data
    for key_name in credentials.keys():
        encrypted_data = retrieve_encrypted_data(key_name)
        if encrypted_data:
            decrypted_value = decrypt_data(
                encrypted_data["ciphertext"],
                encrypted_data["iv"],
                encrypted_data["auth_tag"]
            )
            print(f"Decrypted {key_name}: {decrypted_value}")


---

🔹 How It Works?

1️⃣ Key Management

Generates a 256-bit AES key and stores it securely in an environment variable.

The key is retrieved at runtime to perform encryption/decryption.


2️⃣ Secure Encryption with AES-256-GCM

Encrypts sensitive data using a securely generated IV (nonce).

Generates an authentication tag to ensure integrity.


3️⃣ SQLite Storage

Stores encrypted credentials, IV, and auth tag securely in an SQLite database.

Uses UPSERT (ON CONFLICT) to update credentials if needed.


4️⃣ Secure Decryption

Retrieves IV, auth tag, and ciphertext from the database.

Decrypts and verifies data, ensuring no tampering occurred.



---

🔹 Why Is This Secure?

✅ AES-256-GCM is strong – It provides both encryption and authentication.
✅ Key is never stored in the database – It is kept only in memory (environment variable).
✅ IV (Nonce) ensures uniqueness – Prevents reuse attacks.
✅ Authentication Tag verifies integrity – Prevents modifications or tampering.
✅ SQLite database is local – No need for external services for runtime storage.


---

🔹 What Needs to Be Stored?


---

🔹 When to Re-fetch Data from API?

🔸 Every 24 hours, check for credential changes by making an API call.
🔸 If event hub connection fails multiple times, re-fetch credentials.
🔸 If API call detects a change, update SQLite database with new encrypted values.


---

🚀 Ready to Use!

🔹 Use this implementation in your agent (EXE) running on user machines.
🔹 Only at runtime, the AES key is retrieved from the environment for decryption.
🔹 Ensures maximum security for ClientID, ClientSecret, and other credentials.


---

🔹 Next Steps?

Would you like a cross-platform version using secure hardware-based key storage?






Summary of AES-256-GCM

AES-256-GCM (Advanced Encryption Standard with 256-bit key in Galois/Counter Mode) is a secure and efficient encryption algorithm. It provides both confidentiality and integrity through authentication, making it stronger than AES-CBC.


---

🔹 Why Use AES-256-GCM?

✅ Strong Security – Uses a 256-bit key, making it resistant to brute-force attacks.
✅ Authentication (Integrity Protection) – Includes a 16-byte authentication tag to detect tampering.
✅ No Padding Needed – Unlike AES-CBC, it doesn't require padding, reducing overhead.
✅ Faster Decryption – More efficient than AES-CBC because it operates in counter mode.


---

🔹 What Needs to Be Stored?

To decrypt data, you need to store:


---

🔹 How It Works?

1️⃣ Generate a 256-bit key (store it securely).
2️⃣ Encrypt data using AES-256-GCM with a random IV (nonce).
3️⃣ Store the encrypted data, IV, and auth tag in the local database.
4️⃣ For decryption, retrieve the IV & auth tag, then decrypt using the AES key.


---

🔹 Example Code Using PyCryptodome

🔐 Encryption

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import base64

# Generate a 256-bit (32-byte) AES key (store this securely)
key = get_random_bytes(32)

# Generate a random IV (Nonce)
iv = get_random_bytes(16)

# Create AES-GCM cipher
cipher = AES.new(key, AES.MODE_GCM, nonce=iv)

# Encrypt data
plaintext = b"Sensitive Data"
ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext)

# Store (ciphertext, IV, and auth tag)
print("Ciphertext:", base64.b64encode(ciphertext).decode())
print("IV:", base64.b64encode(iv).decode())
print("Auth Tag:", base64.b64encode(auth_tag).decode())

🔓 Decryption

# Load key, IV, auth tag, and ciphertext from storage
cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
decrypted_data = cipher.decrypt_and_verify(ciphertext, auth_tag)

print("Decrypted Data:", decrypted_data.decode())


---

🔹 Pros & Cons


---

🔹 When to Use AES-256-GCM?

✅ If you need both encryption & authentication (integrity check)
✅ If high security is required (storing sensitive data)
✅ If performance is a concern (faster than CBC)


---

Final Thoughts

AES-256-GCM is one of the most secure and recommended encryption methods. You must store the AES key securely (in an environment variable) and store the IV + auth tag with the encrypted data for proper decryption.

Would you like a complete implementation with secure storage in SQLite?






AES-256-GCM (Advanced Encryption Standard - Galois/Counter Mode)

AES-256-GCM is a highly secure encryption method that provides both confidentiality and integrity by using a combination of AES encryption and Galois Message Authentication Code (GMAC).


---

🔹 Key Features of AES-256-GCM

1. Strong Security: Uses a 256-bit key, making it resistant to brute-force attacks.


2. Authenticated Encryption: Uses an authentication tag (typically 16 bytes) to ensure data integrity and prevent tampering.


3. No Padding Required: Unlike CBC mode, GCM does not require padding.


4. Fast Performance: GCM mode allows parallel processing, making encryption/decryption faster than CBC mode.


5. IV/Nonce Requirement: Requires a unique IV (Initialization Vector) for each encryption operation (typically 12 or 16 bytes).




---

🔹 What to Store for AES-256-GCM?

Since AES-GCM is authenticated encryption, you need to store three things for decryption:

**❌ Do NOT reuse the same IV for multiple encryptions, as it weakens security






Here’s a structured documentation outline based on your architecture:


---

🔹 Secure Storage and Retrieval of Credentials for Event Hub Agent

1. Credentials to Securely Store

The following credentials need to be securely stored and retrieved:

Client ID

Tenant ID

Client Secret

Service Account Name

Service Account Password



---

2. Encryption and Secure Storage Process

🔹 Step 1: Generate Encryption Key

1. Use AES Encryption (Fernet) to securely encrypt the credentials.


2. Generate an encryption key that will be used for both encryption & decryption.


3. Store this key securely, ensuring it’s not hardcoded in the application.

Store in environment variables, a secure key file, or Azure Key Vault.




Key Generation Code:

from cryptography.fernet import Fernet

# Generate a secure encryption key
key = Fernet.generate_key()
print(f"Store this securely: {key.decode()}")

# Save key to a secure file (DO NOT store in source code)
with open("encryption_key.bin", "wb") as f:
    f.write(key)


---

🔹 Step 2: Encrypt Credentials (Manually for First Time)

1. Encrypt credentials using the generated encryption key.


2. Store the encrypted values in Azure SQL Database.



Encryption Code:

import base64
from cryptography.fernet import Fernet

# Load encryption key
with open("encryption_key.bin", "rb") as f:
    key = f.read()

cipher = Fernet(key)  # Initialize cipher for encryption

# Function to encrypt a value
def encrypt_value(value):
    encrypted_value = cipher.encrypt(value.encode())
    return base64.b64encode(encrypted_value).decode()  # Convert to string

# Encrypt and store credentials
client_id_enc = encrypt_value("your-client-id")
client_secret_enc = encrypt_value("your-client-secret")
tenant_id_enc = encrypt_value("your-tenant-id")
service_account_name_enc = encrypt_value("your-service-account")
service_account_password_enc = encrypt_value("your-password")

print("Encrypted Credentials:")
print(client_id_enc, client_secret_enc, tenant_id_enc, service_account_name_enc, service_account_password_enc)

✅ The encrypted values will be stored manually in Azure SQL Database.


---

🔹 Step 3: API Call to Fetch Credentials from Azure SQL Database

1. When the agent runs on the local machine, it will make an API call to fetch the encrypted credentials stored in Azure SQL Database.


2. After retrieving, it will store them in a local SQLite database.



API Flow:
✅ Agent makes a REST API request to fetch credentials
✅ API responds with encrypted credentials
✅ Agent stores these encrypted credentials in a local SQLite database

Example API Request (Python requests):

import requests

API_URL = "https://your-api.com/get-credentials"
response = requests.get(API_URL)

if response.status_code == 200:
    credentials = response.json()
    print("Fetched Encrypted Credentials:", credentials)
else:
    print("Error fetching credentials")


---

🔹 Step 4: Store Encrypted Credentials in Local SQLite

1. After fetching the credentials via API, the agent stores them locally in an SQLite database.


2. This avoids frequent API calls, improving performance.



Store in SQLite:

import sqlite3

# Connect to local SQLite
conn = sqlite3.connect("secure_storage.db")
cursor = conn.cursor()

# Create table for encrypted credentials
cursor.execute("""
CREATE TABLE IF NOT EXISTS credentials (
    key_name TEXT PRIMARY KEY,
    encrypted_value BLOB
)
""")
conn.commit()

# Function to store credentials
def store_credential(key_name, encrypted_value):
    cursor.execute("INSERT OR REPLACE INTO credentials (key_name, encrypted_value) VALUES (?, ?)", (key_name, encrypted_value))
    conn.commit()

# Store encrypted credentials in SQLite
store_credential("client_id", client_id_enc)
store_credential("client_secret", client_secret_enc)
store_credential("tenant_id", tenant_id_enc)
store_credential("service_account_name", service_account_name_enc)
store_credential("service_account_password", service_account_password_enc)

print("Stored encrypted credentials in local SQLite")
conn.close()


---

🔹 Step 5: Decrypt Credentials at Runtime

1. When the agent needs credentials, it retrieves the encrypted values from SQLite and decrypts them.


2. The decryption key is securely loaded at runtime.



Decryption Code:

import base64

# Function to decrypt values
def decrypt_value(encrypted_value):
    decrypted_value = cipher.decrypt(base64.b64decode(encrypted_value)).decode()
    return decrypted_value

# Retrieve encrypted credentials from SQLite
def get_credential(key_name):
    cursor.execute("SELECT encrypted_value FROM credentials WHERE key_name = ?", (key_name,))
    result = cursor.fetchone()
    return decrypt_value(result[0]) if result else None

# Decrypt credentials at runtime
client_id = get_credential("client_id")
client_secret = get_credential("client_secret")
tenant_id = get_credential("tenant_id")
service_account_name = get_credential("service_account_name")
service_account_password = get_credential("service_account_password")

print("Decrypted Client ID:", client_id)  # Use only for debugging


---

3. Handling Credential Updates & Connectivity Issues

To ensure credentials remain valid, the agent should periodically check for updates and handle connectivity failures.

🔹 Step 6: Periodic API Calls for Credential Updates

✅ Every day, the agent will make an API call to check if credentials have changed.
✅ If there are updates (e.g., client secret change), the agent fetches the new credentials and updates the local SQLite database.

API Call for Credential Check:

import time

def check_for_credential_updates():
    response = requests.get(API_URL)
    if response.status_code == 200:
        new_credentials = response.json()

        # Compare with stored values
        if new_credentials["client_secret"] != get_credential("client_secret"):
            print("Updating credentials...")
            store_credential("client_secret", new_credentials["client_secret"])
        
        print("Credentials are up-to-date")
    else:
        print("Error checking credential updates")

# Run daily check
while True:
    check_for_credential_updates()
    time.sleep(86400)  # Wait 24 hours before next check


---

4. Summary of Secure Credential Management Workflow


---

5. Security Best Practices

✅ Never hardcode the encryption key
✅ Use a secure storage for the encryption key (environment variables, TPM, or Key Vault)
✅ Restrict file access to SQLite DB (chmod 600 or Windows ACLs)
✅ Limit API calls to avoid unnecessary exposure of credentials
✅ Use secure network protocols (HTTPS) for all API calls


---

✅ Does this documentation cover everything you need? Let me know if you need any refinements!











Securely Storing and Retrieving Service Account Credentials Using AES Encryption

Since you want a secure way to store your service principal credentials (client ID, secret, tenant ID, service account, password) in SQLite, I'll guide you through:
✅ AES encryption for strong security
✅ Key storage best practices
✅ Storing encrypted data in SQLite
✅ Securely retrieving and decrypting credentials at runtime


---

🔹 1. AES Encryption Using Fernet (Symmetric Key)

We'll use AES encryption with cryptography.fernet. This ensures your credentials are unreadable without the correct decryption key.

Generate a Secure Encryption Key

🔹 Store this key securely! Never hardcode it in your script. Instead, load it from environment variables, a separate secured file, or a TPM module.

from cryptography.fernet import Fernet

# Generate a key
key = Fernet.generate_key()
print(f"Your AES encryption key (store securely!): {key.decode()}")

# Save to a secure location (DO NOT store in your source code)
with open("encryption_key.bin", "wb") as f:
    f.write(key)


---

🔹 2. Store Encrypted Credentials in SQLite

Here’s how you store encrypted credentials securely in SQLite.

import sqlite3
from cryptography.fernet import Fernet

# Load the encryption key
with open("encryption_key.bin", "rb") as f:
    key = f.read()

cipher = Fernet(key)

# Connect to SQLite database
conn = sqlite3.connect("secure_storage.db")
cursor = conn.cursor()

# Create a table to store encrypted credentials
cursor.execute("""
CREATE TABLE IF NOT EXISTS credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_name TEXT,
    encrypted_value BLOB
)
""")
conn.commit()

# Function to encrypt and store a credential
def store_credential(key_name, value):
    encrypted_value = cipher.encrypt(value.encode())
    cursor.execute("INSERT INTO credentials (key_name, encrypted_value) VALUES (?, ?)", (key_name, encrypted_value))
    conn.commit()

# Storing credentials
store_credential("client_id", "your-client-id")
store_credential("client_secret", "your-client-secret")
store_credential("tenant_id", "your-tenant-id")
store_credential("service_account_name", "your-service-account")
store_credential("service_account_password", "your-password")

print("Credentials stored securely.")
conn.close()


---

🔹 3. Retrieve & Decrypt Credentials at Runtime

At runtime, your application will load the encrypted credentials from SQLite and decrypt them.

# Function to retrieve and decrypt a credential
def get_credential(key_name):
    cursor.execute("SELECT encrypted_value FROM credentials WHERE key_name = ?", (key_name,))
    result = cursor.fetchone()
    if result:
        return cipher.decrypt(result[0]).decode()
    return None

# Fetch credentials securely at runtime
client_id = get_credential("client_id")
client_secret = get_credential("client_secret")
tenant_id = get_credential("tenant_id")
service_account_name = get_credential("service_account_name")
service_account_password = get_credential("service_account_password")

print(f"Decrypted Client ID: {client_id}")  # Use only in testing


---

🔹 4. Security Best Practices

1. 🔑 Never Hardcode the Encryption Key

Store the key in a secure file, environment variable, or Windows Credential Manager.

Example: Load from environment variable in production:

import os
key = os.getenv("ENCRYPTION_KEY").encode()



2. 🛡️ Restrict Access to SQLite File

Windows: Store in %APPDATA% and restrict file access:

icacls secure_storage.db /inheritance:r /grant:r %USERNAME%:F

Linux/macOS:

chmod 600 secure_storage.db



3. 🛠️ Store Credentials in an Encrypted Storage Like Azure Key Vault

If feasible, fetch credentials directly from Azure Key Vault instead of storing them locally.





---

✅ Summary

1. Encrypt credentials with AES (Fernet)


2. Store them securely in SQLite


3. Load them only at runtime and decrypt


4. Secure encryption keys (never hardcode!)


5. Restrict file access to SQLite DB



Would this approach work for you? Let me know if you need any modifications!








import urllib.request
import os

# Get system proxy
proxy = urllib.request.getproxies()

# Use system credentials for proxy authentication
proxy_handler = urllib.request.ProxyHandler(proxy)
auth_handler = urllib.request.HTTPBasicAuthHandler()

# Create an opener using proxy and authentication handlers
opener = urllib.request.build_opener(proxy_handler, auth_handler)
urllib.request.install_opener(opener)

# Verify proxy settings
print(f"System Proxy: {proxy}")





import winreg

def get_windows_proxy():
    key_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:
            proxy_enabled = winreg.QueryValueEx(key, "ProxyEnable")[0]
            proxy_server = winreg.QueryValueEx(key, "ProxyServer")[0] if proxy_enabled else None
            return proxy_server
    except Exception as e:
        return f"Error: {e}"

print(f"System Proxy: {get_windows_proxy()}")








Fixing DuplicateTokenEx Third Argument Issue in Windows Impersonation

I understand your requirement:

You are already logged in using Windows credentials.

You want to impersonate yourself (i.e., the same logged-in user).

You need to correctly pass the third argument (TokenAttributes) to DuplicateTokenEx.

You are getting "argument must be int" or "object is not a PySecurityAttributes object" errors.



---

Why Is This Happening?

The issue occurs because the third argument (TokenAttributes) in DuplicateTokenEx must be either None or a valid SECURITY_ATTRIBUTES object. However, the pywin32 library does not always accept pywintypes.SECURITY_ATTRIBUTES() as a valid structure.

Solution

1. Use None for TokenAttributes (Recommended)

Instead of trying to create a pywintypes.SECURITY_ATTRIBUTES(), just pass None. This should work:

import win32api
import win32security
import win32con

def impersonate_user():
    try:
        # Get current process token
        hToken = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        # Duplicate the token correctly (Use None for TokenAttributes)
        dupToken = win32security.DuplicateTokenEx(
            hToken,
            win32con.MAXIMUM_ALLOWED,
            None,  # Security Attributes (MUST BE None)
            win32security.SecurityImpersonation,
            win32security.TokenImpersonation
        )

        # Impersonate the user
        win32security.ImpersonateLoggedOnUser(dupToken)
        print(f"Now impersonating as: {win32api.GetUserName()}")

        # Revert back
        win32security.RevertToSelf()
        print("Reverted to original user.")

    except Exception as e:
        print(f"Error: {e}")

# Run the function
impersonate_user()

✅ This should work in an enterprise environment where the user is already logged in.


---

2. If None Doesn’t Work, Try with a Valid Handle

If passing None still throws an error, try using hToken as the third argument instead:

dupToken = win32security.DuplicateTokenEx(
    hToken,
    win32con.MAXIMUM_ALLOWED,
    hToken,  # Pass the existing token handle
    win32security.SecurityImpersonation,
    win32security.TokenImpersonation
)

Why?

Some enterprise environments require a handle instead of None for token attributes.



---

3. If You Must Use SECURITY_ATTRIBUTES, Do It Correctly

If your organization requires explicit security attributes, use this:

import pywintypes

security_attributes = pywintypes.SECURITY_ATTRIBUTES()
security_attributes.bInheritHandle = True

dupToken = win32security.DuplicateTokenEx(
    hToken,
    win32con.MAXIMUM_ALLOWED,
    None,  # Keep None here if SEC_ATTR doesn't work
    win32security.SecurityImpersonation,
    win32security.TokenImpersonation
)

⚠ Even though pywintypes.SECURITY_ATTRIBUTES() exists, DuplicateTokenEx doesn’t always require it. Passing None is preferred.


---

Key Fixes Summary

Let me know if this works for your case!







If none of the above worked, let's explore alternative ways to impersonate the currently logged-in user.

Alternative 1: Use LogonUser Instead of DuplicateTokenEx

Since DuplicateTokenEx is causing issues with token attributes, try using LogonUser, which directly returns a usable token:

import win32api
import win32security
import win32con

def impersonate_self():
    try:
        username = win32api.GetUserNameEx(win32con.NameSamCompatible)
        domain = username.split("\\")[0]  # Extract domain (if exists)
        user = username.split("\\")[-1]  # Extract username

        print(f"Trying to impersonate: {username}")

        # You need the current user's password; since you're logged in, try an empty string
        hToken = win32security.LogonUser(
            user,
            domain,
            "",  # Empty password may work in enterprise SSO
            win32con.LOGON32_LOGON_INTERACTIVE,
            win32con.LOGON32_PROVIDER_DEFAULT
        )

        # Impersonate user
        win32security.ImpersonateLoggedOnUser(hToken)
        print(f"Impersonating User: {win32api.GetUserName()}")

        # Revert to self
        win32security.RevertToSelf()
        print("Reverted to original user.")

    except Exception as e:
        print(f"Impersonation failed: {e}")

# Run function
impersonate_self()

✅ Why this might work?

LogonUser gets a new token without needing DuplicateTokenEx.

If your organization supports Single Sign-On (SSO), an empty password ("") may work.



---

Alternative 2: Use CreateProcessWithTokenW to Launch as Impersonated User

If direct impersonation fails, try running a new process under the impersonated user:

import win32api
import win32security
import win32con
import subprocess

def run_as_current_user():
    try:
        hToken = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        dupToken = win32security.DuplicateTokenEx(
            hToken,
            win32con.MAXIMUM_ALLOWED,
            None,
            win32security.SecurityImpersonation,
            win32security.TokenPrimary
        )

        print("Launching new process as impersonated user...")

        subprocess.run("cmd.exe", creationflags=subprocess.CREATE_NEW_CONSOLE, stdin=dupToken)

    except Exception as e:
        print(f"Failed to launch as impersonated user: {e}")

# Run function
run_as_current_user()

✅ Why this might work?

Instead of impersonating inside the same process, we start a new process under the impersonated user.



---

Alternative 3: Use NtSetInformationThread (Low-Level API)

If all else fails, a low-level system call can be used:

import ctypes
import win32api
import win32security
import win32con

def low_level_impersonation():
    try:
        hToken = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        dupToken = win32security.DuplicateTokenEx(
            hToken,
            win32con.MAXIMUM_ALLOWED,
            None,
            win32security.SecurityImpersonation,
            win32security.TokenImpersonation
        )

        # Load NtSetInformationThread from NTDLL
        ntdll = ctypes.WinDLL("ntdll")
        NtSetInformationThread = ntdll.NtSetInformationThread
        NtSetInformationThread.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_ulong]
        NtSetInformationThread.restype = ctypes.c_long

        # Apply impersonation
        thread = ctypes.windll.kernel32.GetCurrentThread()
        status = NtSetInformationThread(thread, 0x11, ctypes.byref(dupToken), ctypes.sizeof(dupToken))

        if status == 0:
            print(f"Successfully impersonated as {win32api.GetUserName()}")
        else:
            print(f"Failed with error code: {status}")

    except Exception as e:
        print(f"Impersonation failed: {e}")

# Run function
low_level_impersonation()

✅ Why this might work?

Uses Windows NTDLL system calls, bypassing standard API restrictions.

This method is used by some security tools and malware (so use responsibly).



---

Final Considerations

1. Check if your script has Admin privileges.

Run PowerShell as Administrator and try whoami /groups to verify.



2. Enterprise policies might block impersonation.

Check secpol.msc → Local Policies → User Rights Assignment → "Impersonate a client after authentication".



3. Try LogonUser first, since it doesn't require DuplicateTokenEx.




---

Let me know which one works for you!







import win32api
import win32security
import win32con

def impersonate_current_user():
    try:
        # Get the current process token
        h_token = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        # Duplicate the token for impersonation
        dup_token = win32security.DuplicateTokenEx(
            h_token,
            win32con.MAXIMUM_ALLOWED,
            None,
            win32security.SecurityImpersonation,
            win32security.TokenPrimary
        )

        # Impersonate the user
        win32security.ImpersonateLoggedOnUser(dup_token)
        print(f"Impersonating User: {win32api.GetUserName()}")

        # Revert to the original user
        win32security.RevertToSelf()
        print("Reverted to original user.")

    except Exception as e:
        print(f"Impersonation failed: {e}")

# Run the function
impersonate_current_user()






import win32security
import win32api
import win32con

def impersonate_user(username, domain, password):
    # Get a handle to the user token
    token = win32security.LogonUser(
        username, domain, password,
        win32con.LOGON32_LOGON_INTERACTIVE,
        win32con.LOGON32_PROVIDER_DEFAULT
    )

    # Impersonate the user
    win32security.ImpersonateLoggedOnUser(token)
    
    # Now any code you run here executes as the impersonated user

    # Revert back to original user
    win32security.RevertToSelf()
    token.Close()

# Example usage (You must have the user’s password)
impersonate_user("USERNAME", "DOMAIN", "PASSWORD")




import requests
from datetime import datetime, timedelta
import urllib
import base64
import hmac
import hashlib

# Event Hub details
namespace = "your-eventhub-namespace"
eventhub_name = "your-eventhub-name"
sas_key_name = "your-sas-key-name"  # Shared Access Policy name
sas_key = "your-sas-key"  # Shared Access Key
eventhub_url = f"https://{namespace}.servicebus.windows.net/{eventhub_name}/messages"

# Generate SAS Token
expiry = int((datetime.utcnow() + timedelta(hours=1)).timestamp())
uri = urllib.parse.quote_plus(f"{namespace}.servicebus.windows.net/{eventhub_name}/messages")
string_to_sign = f"{uri}\n{expiry}"
signature = base64.b64encode(hmac.new(base64.b64decode(sas_key), string_to_sign.encode('utf-8'), hashlib.sha256).digest())
sas_token = f"SharedAccessSignature sr={uri}&sig={urllib.parse.quote_plus(signature)}&se={expiry}&skn={sas_key_name}"

# Event data (Batch of events)
event_data = """
<entry xmlns="http://www.w3.org/2005/Atom">
    <content type="application/xml">
        <EventData>
            <Message>Your Event Data here</Message>
        </EventData>
    </content>
</entry>
"""

# Send HTTP request to Event Hub with PAC Proxy settings
headers = {
    'Authorization': sas_token,
    'Content-Type': 'application/atom+xml;type=entry;charset=utf-8',
    'Host': f'{namespace}.servicebus.windows.net'
}

# Set up the session to respect proxy (if you're using PAC)
session = requests.Session()

# Proxy settings (if required by your environment)
session.proxies = {
    'http': 'http://proxyserver:port',
    'https': 'https://proxyserver:port'
}

# Send the event batch to Event Hub
response = session.post(eventhub_url, headers=headers, data=event_data)

# Check response status
if response.status_code == 201:
    print("Event sent successfully!")
else:
    print(f"Failed to send event. Status Code: {response.status_code}, Response: {response.text}")




import os
import requests
import keyring
from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventDataBatch

# Retrieve the stored password
username = "your_username"
proxy_url = "http://us-proxy-01.systems.us.ongc:80"

# Get password from Windows Credential Manager
password = keyring.get_password("Windows Stored Credentials", username)

if not password:
    raise Exception("Proxy password not found in Windows Credential Manager!")

# Set environment variables with authentication
auth_proxy = f"http://{username}:{password}@us-proxy-01.systems.us.ongc:80"
os.environ['HTTP_PROXY'] = auth_proxy
os.environ['HTTPS_PROXY'] = auth_proxy

# Create a session
session = requests.Session()
session.proxies = {'http': auth_proxy, 'https': auth_proxy}

# Test proxy authentication
try:
    response = session.get("http://your-eventhub-url-or-api")
    print(f"Proxy authentication successful! Status Code: {response.status_code}")
except Exception as e:
    print(f"Failed to authenticate with proxy: {e}")

# Azure Event Hub authentication
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"
eventhub_namespace = "your-eventhub-namespace"
eventhub_name = "your-eventhub-name"

# Initialize ClientSecretCredential
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create EventHubProducerClient
producer_client = EventHubProducerClient(
    fully_qualified_namespace=f"{eventhub_namespace}.servicebus.windows.net",
    eventhub_name=eventhub_name,
    credential=credential
)

# Create a batch, add event data, and send the batch
event_data_batch = producer_client.create_batch()
event_data = "Your event data goes here"
event_data_batch.add(event_data)

# Send the batch of events
try:
    producer_client.send_batch(event_data_batch)
    print("Batch sent successfully!")
except Exception as e:
    print(f"Failed to send batch: {e}")
finally:
    producer_client.close()







import os
import requests
from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventDataBatch
from requests_ntlm import HttpNtlmAuth
import socket
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# Step 1: Set proxy environment variables
proxy_url = "http://us-proxy-01.systems.us.ongc:80"

# Set proxy for HTTP and HTTPS in environment variables
os.environ['HTTP_PROXY'] = proxy_url
os.environ['HTTPS_PROXY'] = proxy_url

# Step 2: Create a session to handle NTLM authentication for the proxy
session = requests.Session()

# Define a custom adapter that uses the current Windows user's credentials (no manual username/password required)
class NTLMAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs['proxy_urls'] = {
            'http': proxy_url,
            'https': proxy_url
        }
        return super().init_poolmanager(*args, **kwargs)

# Attach the NTLM adapter
session.mount('http://', NTLMAdapter())
session.mount('https://', NTLMAdapter())

# Optionally, you can verify if the proxy works by making a request
try:
    # Try accessing a URL that goes through the proxy to see if NTLM authentication works
    response = session.get('http://your-eventhub-url-or-api')  # Replace with a valid URL
    print(response.status_code)
except Exception as e:
    print(f"Error connecting to proxy: {e}")

# Step 3: Use ClientSecretCredential to authenticate with Azure
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"
eventhub_namespace = "your-eventhub-namespace"
eventhub_name = "your-eventhub-name"

# Initialize ClientSecretCredential for Azure Event Hub authentication
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Step 4: Create EventHubProducerClient using the credential
producer_client = EventHubProducerClient(
    fully_qualified_namespace=f"{eventhub_namespace}.servicebus.windows.net",
    eventhub_name=eventhub_name,
    credential=credential
)

# Step 5: Create a batch, add event data, and send the batch
event_data_batch = producer_client.create_batch()

# Add some event data to the batch
event_data = "Your event data goes here"
event_data_batch.add(event_data)

# Send the batch of events
try:
    producer_client.send_batch(event_data_batch)
    print("Batch sent successfully!")
except Exception as e:
    print(f"Failed to send batch: {e}")

# Close the producer client after sending
finally:
    producer_client.close()






import logging
from logging.handlers import RotatingFileHandler

# Define log file path
log_file = "app.log"

# Setup RotatingFileHandler
handler = RotatingFileHandler(
    log_file, mode='a', maxBytes=50 * 1024, backupCount=5, encoding=None, delay=False
)

# Define log format: Time (including milliseconds), Level, and Message
formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

handler.setFormatter(formatter)

# Configure logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Test logging (generates large logs to test rotation)
for i in range(10000):
    logger.info(f"This is log entry number {i}")








import ctypes
import logging
from logging.handlers import RotatingFileHandler

# Define log file path
log_file = "app.log"

# Setup RotatingFileHandler
handler = RotatingFileHandler(
    log_file, mode='a', maxBytes=50 * 1024, backupCount=5, encoding=None, delay=False
)

# Define log format: Time (including milliseconds), Level, and Message
formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

handler.setFormatter(formatter)

# Configure logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Test logging (generates large logs to test rotation)
for i in range(10000):
    logger.info(f"This is log entry number {i}")import time
import win32api
import win32con
import win32gui
import win32process
import win32security
import datetime
from ctypes import wintypes

# Constants for Session Status
WTS_SESSION_LOCK = 0x07
WTS_SESSION_UNLOCK = 0x08
WTS_SESSION_LOGON = 0x01
WTS_SESSION_LOGOFF = 0x02

# Store timestamps for lock and unlock
lock_time = None
unlock_time = None

# Function to handle system session events (lock/unlock)
def handle_wts_event(wparam, lparam):
    global lock_time, unlock_time
    
    if wparam == WTS_SESSION_LOCK:
        # System is locked
        lock_time = datetime.datetime.now()
        print(f"System locked at: {lock_time}")
        
    elif wparam == WTS_SESSION_UNLOCK:
        # System is unlocked
        unlock_time = datetime.datetime.now()
        print(f"System unlocked at: {unlock_time}")
        
        if lock_time:
            # Calculate the duration between lock and unlock
            duration = unlock_time - lock_time
            print(f"Duration locked: {duration}")
        
        # Reset lock_time after unlock
        lock_time = None
        
    elif wparam == WTS_SESSION_LOGON:
        print(f"User logged on at: {datetime.datetime.now()}")
        
    elif wparam == WTS_SESSION_LOGOFF:
        print(f"User logged off at: {datetime.datetime.now()}")

# Register for session notifications
def register_session_notifications(hwnd):
    ctypes.windll.wtsapi32.WTSRegisterSessionNotification(hwnd, 1)

# Create a simple window to handle messages
def create_window():
    wc = win32gui.WNDCLASS()
    wc.lpfnWndProc = handle_wts_event  # Set the handler for the window messages
    wc.lpszClassName = "SessionMonitorClass"
    wc.hInstance = win32api.GetModuleHandle(None)

    class_atom = win32gui.RegisterClass(wc)
    hwnd = win32gui.CreateWindow(class_atom, "Session Monitor Window", 0, 0, 0, 0, 0, 0, 0, 0, wc.hInstance, None)

    register_session_notifications(hwnd)  # Register for session notifications
    win32gui.PumpMessages()  # Start the message loop

# Main function
if __name__ == "__main__":
    print("Monitoring system lock/unlock events...")
    create_window()




import win32gui
import win32con
import win32api
import win32ts
import ctypes
import time

# Define window procedure
def window_proc(hwnd, msg, wparam, lparam):
    try:
        if msg == win32con.WM_WTSSESSION_CHANGE:  # Correct event type
            if wparam == win32ts.WTS_SESSION_LOCK:
                print("🔒 System Locked!")
            elif wparam == win32ts.WTS_SESSION_UNLOCK:
                print("🔓 System Unlocked!")
        return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)
    except Exception as e:
        print(f"Error in WNDPROC handler: {e}")
        return 0  # Always return an integer (prevents crashes)

class WindowsSessionMonitor:
    def __init__(self):
        """Creates a hidden window to listen for lock/unlock events"""
        self.hinst = win32api.GetModuleHandle(None)
        self.class_name = "SessionMonitorWindow"

        # Define window class
        self.wc = win32gui.WNDCLASS()
        self.wc.lpfnWndProc = window_proc
        self.wc.hInstance = self.hinst
        self.wc.lpszClassName = self.class_name
        self.class_atom = win32gui.RegisterClass(self.wc)

        # Create the hidden window
        self.hwnd = win32gui.CreateWindow(self.class_atom, "Session Monitor", 0, 0, 0, 0, 0, 0, 0, 0, self.hinst, None)

        # Register for session notifications
        ctypes.windll.wtsapi32.WTSRegisterSessionNotification(self.hwnd, win32ts.NOTIFY_FOR_ALL_SESSIONS)

    def run(self):
        """Starts the event listener"""
        print("Listening for Windows lock/unlock events...")
        try:
            while True:
                win32gui.PumpMessages()  # Correct message loop
        except KeyboardInterrupt:
            print("Stopping monitoring...")
            ctypes.windll.wtsapi32.WTSUnRegisterSessionNotification(self.hwnd)
            win32gui.DestroyWindow(self.hwnd)

if __name__ == "__main__":
    monitor = WindowsSessionMonitor()
    monitor.run()








import win32gui
import win32con
import win32api
import win32ts
import time

def window_proc(hwnd, msg, wparam, lparam):
    if msg == win32con.WM_WTSSESSION_CHANGE:
        if wparam == win32con.WTS_SESSION_LOCK:
            print("System Locked!")  # Trigger when the system locks
        elif wparam == win32con.WTS_SESSION_UNLOCK:
            print("System Unlocked!")  # Trigger when the system unlocks
    return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)

class WindowsSessionMonitor:
    def __init__(self):
        self.hinst = win32api.GetModuleHandle(None)
        self.class_name = "SessionMonitorWindow"
        self.wc = win32gui.WNDCLASS()
        self.wc.lpfnWndProc = window_proc
        self.wc.hInstance = self.hinst
        self.wc.lpszClassName = self.class_name
        self.class_atom = win32gui.RegisterClass(self.wc)
        self.hwnd = win32gui.CreateWindow(self.class_atom, "Session Monitor", 0, 0, 0, 0, 0, 0, 0, self.hinst, None)
        win32ts.WTSRegisterSessionNotification(self.hwnd, win32ts.NOTIFY_FOR_ALL_SESSIONS)

    def run(self):
        """Starts the event listener"""
        print("Listening for session changes...")
        try:
            while True:
                win32gui.PumpWaitingMessages()
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Stopping monitoring...")
            win32ts.WTSUnRegisterSessionNotification(self.hwnd)
            win32gui.DestroyWindow(self.hwnd)

if __name__ == "__main__":
    monitor = WindowsSessionMonitor()
    monitor.run()








import logging
from logging.handlers import RotatingFileHandler
import os

# Define log file path
log_file = "app.log"

# Setup RotatingFileHandler
handler = RotatingFileHandler(
    log_file, mode='a', maxBytes=50 * 1024, backupCount=5, encoding=None, delay=False
)

# Configure logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Test logging (generates large logs to test rotation)
for i in range(10000):
    logger.info(f"This is log entry number {i}")








import os
import time
from pynput import mouse, keyboard
from datetime import datetime

# Directory and log file path
log_dir = 'Pawan'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Log file size threshold in bytes (10 KB)
MAX_FILE_SIZE = 10 * 1024

# Function to get the current log file name
def get_log_filename():
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')
    return os.path.join(log_dir, f'pawan_{current_time}.log')

# Function to write logs to file
def write_log(message):
    log_filename = get_log_filename()
    
    # Check if the current log file size exceeds the limit
    if os.path.exists(log_filename) and os.path.getsize(log_filename) > MAX_FILE_SIZE:
        log_filename = get_log_filename()  # Create a new log file with the updated name

    # Write the message to the log file
    with open(log_filename, 'a') as f:
        f.write(message + '\n')

# Mouse event handler
def on_click(x, y, button, pressed):
    if pressed:
        write_log(f'Mouse clicked at ({x}, {y}) with {button}')
    else:
        write_log(f'Mouse released at ({x}, {y}) with {button}')

# Keyboard event handler
def on_press(key):
    try:
        write_log(f'Key pressed: {key.char}')
    except AttributeError:
        write_log(f'Special key pressed: {key}')

# Collect events
def start_logging():
    # Start mouse listener
    with mouse.Listener(on_click=on_click) as mouse_listener:
        # Start keyboard listener
        with keyboard.Listener(on_press=on_press) as keyboard_listener:
            print("Logging mouse and keyboard events. Press ESC to stop.")
            # Join listeners to keep the program running
            mouse_listener.start()
            keyboard_listener.join()

if __name__ == '__main__':
    start_logging()











from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventData
import json

# Define your Azure AD application credentials (Service Principal)
tenant_id = 'your-tenant-id'
client_id = 'your-client-id'
client_secret = 'your-client-secret'
eventhub_namespace = 'your-eventhub-namespace'
eventhub_name = 'your-eventhub-name'

# Create a credential object to authenticate using Service Principal
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create the EventHubProducerClient using the credential (OAuth2 token-based authentication)
producer = EventHubProducerClient(
    fully_qualified_namespace=f'{eventhub_namespace}.servicebus.windows.net',
    eventhub_name=eventhub_name,
    credential=credential
)

async def send_event_to_eventhub(event_data):
    try:
        # Create EventData
        event = EventData(event_data)

        # Send event
        async with producer:
            await producer.send_event(event, partition_key="login data")
        print(f"Successfully sent event: {event_data}")
    except Exception as e:
        print(f"Failed to send event: {str(e)}")

# Example of data to be sent
login_data = {"emp_id": 1234, "timestamp": "2025-01-30T12:00:00"}
send_event_to_eventhub(json.dumps(login_data))








from azure.eventhub import EventHubProducerClient, EventData
import json
import asyncio

# SAS token and connection details
fully_qualified_namespace = 'your-eventhub-namespace.servicebus.windows.net'
eventhub_name = 'your-eventhub-name'
sas_token = 'your-SAS-token'  # This is your SAS token

async def send_event_to_eventhub(event_data):
    try:
        print(f"Preparing to send event data: {event_data}")

        # Create producer client with SAS Token
        async with EventHubProducerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=eventhub_name,
            credential=sas_token  # Use the SAS token for authentication
        ) as producer:
            
            # Create EventData
            event = EventData(event_data)

            # Send event
            await producer.send_event(event, partition_key="login data")

            print(f"Successfully sent event to Event Hub: {event_data}")

    except Exception as e:
        # Capture more specific error information
        print(f"Failed to send event. Error: {str(e)}")

# Example event data
login_data = {"emp_id": 1234, "timestamp": "2025-01-30T12:00:00"}
asyncio.run(send_event_to_eventhub(json.dumps(login_data)))






from azure.eventhub import EventHubProducerClient, EventData
import json
import asyncio

# Use the primary connection string
connection_str = 'Endpoint=sb://your-eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=EventSender;SharedAccessKey=your-primary-key;EntityPath=your-eventhub-name'

eventhub_name = "your-eventhub-name"

async def send_event_to_eventhub(event_data):
    try:
        # Debug statement: Before sending the event
        print(f"Preparing to send event data: {event_data}")

        # Using async with for the producer client
        async with EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name) as producer:

            # Create EventData from the JSON event data
            event = EventData(event_data)

            # Send the event to the Event Hub with partition key "login data"
            await producer.send_event(event, partition_key="login data")

            # Debug statement: After successfully sending the event
            print(f"Successfully sent event to Event Hub: {event_data}")

    except Exception as e:
        # Debug statement: On failure
        print(f"Failed to send event: {str(e)}")

# Example: Sending some JSON data
login_data = {"emp_id": 1234, "timestamp": "2025-01-30T12:00:00"}
asyncio.run(send_event_to_eventhub(json.dumps(login_data)))




import ctypes
import ctypes.wintypes as wintypes
import signal
import sys

# Define Windows Constants
WH_KEYBOARD_LL = 13  # Low-level keyboard hook ID
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101
VK_TAB = 0x09
VK_ALT = 0x12

# Global variables
hHook = None  # Hook handle
alt_pressed = False

# Define Hook Procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    """ Callback function for keyboard hook. Detects key combinations. """
    global alt_pressed

    if nCode >= 0:
        vk_code = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents.vkCode
        print(f"[HOOK TRIGGERED] Key Code: {vk_code}, Event Type: {wParam}")

        if wParam == WM_KEYDOWN:
            if vk_code == VK_ALT:
                alt_pressed = True
                print("[INFO] ALT Pressed")
            elif vk_code == VK_TAB and alt_pressed:
                print("[DETECTED] ALT + TAB Pressed!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_ALT:
                alt_pressed = False
                print("[INFO] ALT Released")

    return ctypes.windll.user32.CallNextHookEx(hHook, nCode, wParam, lParam)

# Define Hook Struct
class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [("vkCode", wintypes.DWORD),
                ("scanCode", wintypes.DWORD),
                ("flags", wintypes.DWORD),
                ("time", wintypes.DWORD),
                ("dwExtraInfo", wintypes.ULONG_PTR)]

def set_keyboard_hook():
    """ Sets a global keyboard hook """
    global hHook

    # Define Hook Procedure Type
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    
    # Convert Python function to Windows hook function
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    # Install Hook
    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)
    
    if not hHook:
        print("❌ [ERROR] Failed to set keyboard hook.")
        sys.exit(1)

    print("✅ [SUCCESS] Keyboard Hook Installed.")

    # Message Loop to Keep Hook Active
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Handle CTRL+C for Graceful Exit
def handle_exit(signum, frame):
    """ Handles CTRL+C to properly unhook and exit """
    global hHook
    print("\n[EXIT] CTRL+C Detected. Unhooking keyboard and exiting...")
    
    if hHook:
        ctypes.windll.user32.UnhookWindowsHookEx(hHook)  # Unhook before exiting
    
    sys.exit(0)

# Register Signal Handler for CTRL+C
signal.signal(signal.SIGINT, handle_exit)

# Run the Hook
if __name__ == "__main__":
    print("🔄 [INFO] Running Keyboard Hook. Press CTRL+C to exit.")
    set_keyboard_hook()






import ctypes
import win32con
from ctypes import wintypes

# Constants
WH_KEYBOARD_LL = 13
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

VK_TAB = 0x09
VK_MENU = 0x12  # ALT key
VK_CONTROL = 0x11
VK_SHIFT = 0x10
VK_ESCAPE = 0x1B
VK_DELETE = 0x2E

# Track key states
alt_pressed = False
ctrl_pressed = False
shift_pressed = False

# Structure to extract vkCode
class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [
        ("vkCode", wintypes.DWORD),
        ("scanCode", wintypes.DWORD),
        ("flags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", wintypes.ULONG)
    ]

# Low-level keyboard hook procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    global alt_pressed, ctrl_pressed, shift_pressed

    if nCode >= 0:
        # Fix: Properly cast lParam to prevent integer overflow
        kbd_struct = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents
        vk_code = kbd_struct.vkCode  # Extract virtual key code

        if wParam == WM_KEYDOWN:
            if vk_code == VK_MENU:  # ALT key
                alt_pressed = True
            elif vk_code == VK_CONTROL:
                ctrl_pressed = True
            elif vk_code == VK_SHIFT:
                shift_pressed = True
            elif vk_code == VK_TAB and alt_pressed:
                print("🔹 ALT + TAB detected!")
            elif vk_code == VK_DELETE and ctrl_pressed and alt_pressed:
                print("🔹 CTRL + ALT + DEL detected!")
            elif vk_code == VK_ESCAPE and ctrl_pressed and shift_pressed:
                print("🔹 CTRL + SHIFT + ESC detected!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_MENU:
                alt_pressed = False
            elif vk_code == VK_CONTROL:
                ctrl_pressed = False
            elif vk_code == VK_SHIFT:
                shift_pressed = False

    # Fix: Use the correct types in CallNextHookEx
    return ctypes.windll.user32.CallNextHookEx(0, nCode, wParam, ctypes.byref(kbd_struct))

# Set Windows keyboard hook
def set_keyboard_hook():
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)

    if not hHook:
        print("❌ Failed to set hook")
        return
    
    print("✅ Keyboard hook started. Press ALT + TAB, WIN + TAB, or CTRL + ALT + DEL to test.")

    # Message loop to listen for keyboard events
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Run the hook
if __name__ == "__main__":
    set_keyboard_hook()






import ctypes
import win32con
from ctypes import wintypes

# Constants
WH_KEYBOARD_LL = 13
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

VK_TAB = 0x09
VK_MENU = 0x12  # ALT key
VK_CONTROL = 0x11
VK_SHIFT = 0x10
VK_ESCAPE = 0x1B
VK_DELETE = 0x2E

# Track key states
alt_pressed = False
ctrl_pressed = False
shift_pressed = False

# Structure to extract vkCode
class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [
        ("vkCode", wintypes.DWORD),
        ("scanCode", wintypes.DWORD),
        ("flags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", ctypes.POINTER(wintypes.ULONG))  # Correctly define dwExtraInfo pointer
    ]

# Low-level keyboard hook procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    global alt_pressed, ctrl_pressed, shift_pressed

    if nCode >= 0:
        kbd_struct = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents
        vk_code = kbd_struct.vkCode  # Extract virtual key code

        if wParam == WM_KEYDOWN:
            if vk_code == VK_MENU:  # ALT key
                alt_pressed = True
            elif vk_code == VK_CONTROL:
                ctrl_pressed = True
            elif vk_code == VK_SHIFT:
                shift_pressed = True
            elif vk_code == VK_TAB and alt_pressed:
                print("🔹 ALT + TAB detected!")
            elif vk_code == VK_DELETE and ctrl_pressed and alt_pressed:
                print("🔹 CTRL + ALT + DEL detected!")
            elif vk_code == VK_ESCAPE and ctrl_pressed and shift_pressed:
                print("🔹 CTRL + SHIFT + ESC detected!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_MENU:
                alt_pressed = False
            elif vk_code == VK_CONTROL:
                ctrl_pressed = False
            elif vk_code == VK_SHIFT:
                shift_pressed = False

    return ctypes.windll.user32.CallNextHookEx(None, nCode, wParam, lParam)

# Set Windows keyboard hook
def set_keyboard_hook():
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)

    if not hHook:
        print("❌ Failed to set hook")
        return
    
    print("✅ Keyboard hook started. Press ALT + TAB, WIN + TAB, or CTRL + ALT + DEL to test.")

    # Message loop to listen for keyboard events
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Run the hook
if __name__ == "__main__":
    set_keyboard_hook()

import ctypes
import win32con
import win32api
from ctypes import wintypes

# Constants for Windows Hook
WH_KEYBOARD_LL = 13
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

# Virtual Key Codes
VK_TAB = 0x09
VK_MENU = 0x12  # ALT key
VK_LWIN = 0x5B  # Left Windows key
VK_RWIN = 0x5C  # Right Windows key
VK_ESCAPE = 0x1B
VK_DELETE = 0x2E

# Track key states
alt_pressed = False
win_pressed = False
ctrl_pressed = False

# Hook procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    global alt_pressed, win_pressed, ctrl_pressed

    if nCode == 0:  # Process only valid key events
        vk_code = lParam[0]  # Get virtual key code

        if wParam == WM_KEYDOWN:
            if vk_code == VK_MENU:  # ALT key
                alt_pressed = True
            elif vk_code == VK_TAB and alt_pressed:
                print("🔹 ALT + TAB detected!")
            elif vk_code in [VK_LWIN, VK_RWIN]:  # Windows key
                win_pressed = True
            elif vk_code == VK_LWIN and vk_code == VK_TAB:
                print("🔹 WIN + TAB detected!")
            elif vk_code == VK_DELETE and ctrl_pressed and alt_pressed:
                print("🔹 CTRL + ALT + DEL detected!")
            elif vk_code == VK_ESCAPE and ctrl_pressed and shift_pressed:
                print("🔹 CTRL + SHIFT + ESC detected!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_MENU:
                alt_pressed = False
            elif vk_code in [VK_LWIN, VK_RWIN]:
                win_pressed = False
            elif vk_code == VK_TAB:
                alt_pressed = False  # Reset ALT state after releasing TAB

    return ctypes.windll.user32.CallNextHookEx(None, nCode, wParam, lParam)

# Set up Windows hook
def set_keyboard_hook():
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)

    if not hHook:
        print("❌ Failed to set hook")
        return
    
    print("✅ Keyboard hook started. Press ALT + TAB, WIN + TAB, or CTRL + ALT + DEL to test.")

    # Message loop to keep the hook running
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Run the hook
if __name__ == "__main__":
    set_keyboard_hook()









import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Key Vault and Secret Configuration
KEY_VAULT_NAME = "your-key-vault-name"  # Replace with your Key Vault name
SECRET_NAME = "your-secret-name"  # Replace with your secret name

# Build the Key Vault URL
KEY_VAULT_URL = f"https://{KEY_VAULT_NAME}.vault.azure.net/"

# Use DefaultAzureCredential to authenticate
credential = DefaultAzureCredential()

# Connect to Key Vault
client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# Fetch the secret
try:
    secret = client.get_secret(SECRET_NAME)
    client_secret = secret.value
    print("Client secret retrieved successfully.")
except Exception as e:
    print(f"Error retrieving secret: {e}")

# Use client_secret in your code
print(f"Client Secret: {client_secret}")





Button Workflow:

Default Screen Workflow:

1. From Date and To Date Selection:

The manager selects the start date (From Date) and end date (To Date) for assigning shift timings.



2. Common Shift Button:

Once the dates are selected, the manager clicks Common Shift to assign uniform start and end timings for all employees under the manager or based on department filters (if applied).



3. Save Button:

After assigning the common shift, the manager clicks Save to store the shift details for the selected date range and employees in the database.



4. Reset Button:

If the manager wants to clear the selected dates, departments, or any assigned shifts, they can click Reset to start over.





---

Edit Screen Workflow:

1. From Date and To Date Selection:

The manager selects a From Date and To Date to fetch previously submitted shift details for editing.



2. Get Shift Details Button:

After selecting the date range and applying department filters (if needed), the manager clicks Get Shift Details.

This retrieves the submitted shift details for the selected employees and date range.



3. Editing Shift Details:

Managers can modify the shift timings for multiple employees, individual employees, or specific days.



4. Update Button:

After making changes, the manager clicks Update to save the modified shift timings in the database.



5. Reset Button:

The manager can click Reset to clear the selected filters, dates, or edits and start fresh.





---

View Screen Workflow:

1. Default View:

By default, all employees under the manager are displayed unless department filters are applied.



2. Filter by Department:

Managers can use the department filter to view employees from specific departments.



3. Edit (Pencil Icon):

To modify shift details, the manager clicks the Edit (Pencil Icon) for an individual employee.

This allows day-wise or week-wise shift modifications for that employee.



4. Save Changes:

After making edits, the manager clicks Save Changes to update the shift details for the selected employee.



5. Get Shift Details (if applied):

For specific departments or multiple employees, Get Shift Details can be used to fetch the current shift details.

This ensures the latest information is displayed before any edits are made.





---

Summary:

Default Screen: Focuses on assigning new shifts using Common Shift and Save without retrieving existing data.

Edit Screen: Allows retrieving and editing previously submitted shifts with Get Shift Details and Update.

View Screen: Primarily used for viewing current shifts and making individual modifications with the Edit (Pencil Icon) and Save Changes.










Definitions for Buttons and Filters

1. Default Manager Details:

Purpose: Displays the manager's information by default when the screen is loaded.

Description: Ensures that only the employees reporting to the logged-in manager are shown in the view.



2. Department Filter (Multi-Select):

Purpose: Allows the manager to filter employees based on departments.

Description:

Displays a list of all departments where employees reporting to the manager are assigned.

Managers can select one or multiple departments to narrow down the list of employees for shift management.




3. From Date and To Date:

From Date:

Purpose: Allows the manager to select the start date of the week for which shifts need to be managed.

Description: Ensures that only relevant data starting from the selected week is displayed.


To Date:

Purpose: Allows the manager to select the end date of the week for managing shifts.

Description: Ensures that the data displayed corresponds to the selected date range.




4. Get Shift Details Button:

Purpose: Fetches the shift details for the selected employees or departments.

Availability:

Only available on the EditView Screen.

Used to retrieve the shift details that were previously submitted for a selected date range or department filter.


Description:

Displays all shift details for the selected employees, departments, and date range.

Managers can edit the retrieved shift timings as needed.




5. Common Shift Button:

Purpose: Allows the manager to assign the same shift timings (start and end time) for all employees or selected employees for the entire week.

Description:

Managers can input a common start time and end time.

This common timing is applied to all days of the week.

After applying a common shift, managers can still modify timings for individual employees or specific days if needed.


Flexibility: Enables quick bulk assignment of shifts while retaining the ability to make individual adjustments later.




Button Workflow:

Step 1: Use the Department Filter to select the desired departments or keep it default to view all employees.

Step 2: Use the From Date and To Date fields to define the time range for shift details.

Step 3:

On the EditView Screen, click Get Shift Details to retrieve and edit previously submitted shifts.

On the Shift Assignment Screen, click Common Shift to assign uniform timings across employees.


Step 4: Save changes to update the shift schedules in the database.












EditView Screen Functionality

1. Overview:

The EditView screen allows managers to edit already submitted shift timings for one or multiple employees.

It provides flexibility to modify shift schedules based on department-level or employee-level filters.



2. Default View:

By default, all employees under the logged-in manager are displayed.

If no filters are applied, the system shows all employees with their shift details for the entire week.



3. Filters and Selection:

Managers can apply department filters to focus on specific departments, selecting a single department or multiple departments.

They can also specify a "From Date" and "To Date" to view and edit shift details within a specific time period.



4. Get Shift Details:

After applying filters and selecting a date range, managers click the "Get Shift Details" button to retrieve previously submitted shift timings.

The retrieved data includes shift timings for all days of the week for the selected employees or department(s).



5. Editing Capabilities:

Managers can edit shift timings for:

Individual employees.

Multiple employees simultaneously.

The entire week or specific days.


This ensures comprehensive and precise updates for all scenarios.



6. Save Updates:

Once edits are completed, managers can save the changes.

The updated schedules are stored in the system, ensuring accurate and up-to-date shift records.




Benefits of EditView:

Enables efficient shift management for the entire week.

Allows flexible filtering by department or employees, streamlining the process.

Provides granular editing options to manage individual or multiple employees’ schedules effectively.









View Screen Functionality

1. Default View:

By default, the view screen displays all employees under the logged-in manager.

If no department filter is applied, all employees are shown in the list.



2. Department-Level Filters:

Managers can filter employees based on department(s).

If a department filter is applied, only employees belonging to the selected department(s) will be displayed.



3. Employee Selection:

The view screen allows managers to select an employee or multiple employees from the displayed list.

Managers can focus on specific employees to review their shift details.



4. Shift Details:

For the selected employee(s), the system displays their submitted shift details for the week.

Shift timings for each day are shown, providing an overview of the assigned schedule.



5. Individual Shift Modifications:

Managers can modify shift timings for individual employees or specific days directly from the view screen.

Day-wise adjustments can be made to address specific requirements for an employee.



6. Dynamic Updates:

Any changes made by the manager are saved, ensuring that the shift schedule reflects the latest updates.

Managers can reassign or modify shifts as needed, even after submission.



7. Flexible Management:

The view screen provides a convenient way to monitor and manage shift timings for all employees or those filtered by department.

It ensures managers can make informed decisions and adjust schedules in real time.




This feature enables managers to have a comprehensive and customizable view of employee schedules, offering both high-level and detailed insights to optimize shift management.












Department-Level Filters

1. Filter by Department:

Managers can use department-level filters to manage employees more efficiently.

They can filter the employee list by selecting a single department or multiple departments.



2. Common Shift Assignment within Departments:

After applying the department filter, managers can assign common shift timings (start time and end time) to all employees under the selected department(s).

This allows for bulk assignment of shifts without handling individual schedules initially.



3. Individual Shift Modifications:

Once the common shift is assigned, managers can still modify shift timings for individual employees or specific days within the filtered department(s).

This ensures flexibility to accommodate specific employee requirements or special cases.



4. Save Shift Timings:

Managers can save the assigned or modified shift timings for the employees under the selected department(s).

If no further changes are needed, they can directly save the common shifts without additional modifications.



5. Efficient Workflow Management:

The department filter streamlines the process by allowing managers to focus on a subset of employees based on organizational hierarchy.

This approach improves the accuracy and efficiency of shift assignments and modifications.



6. Adaptable Process:

Whether assigning common shifts or customizing individual schedules, the system provides the flexibility to manage shifts in bulk or at a granular level.

This adaptability ensures that the shift management process aligns with the dynamic needs of the team or department.




This feature ensures a seamless workflow, giving managers complete control over shift scheduling, whether managing all employees or focusing on specific departments.






Objective

The Employee Shift Screen is a web application designed to simplify shift management for managers. It enables managers to assign, view, and modify shift timings for employees under their supervision, ensuring efficient workforce management for a week.

Features

1. View Active Employees:

Upon login, managers can see a list of all active employees working under them for the current week.



2. Common Shift Assignment:

A "Common Shift" option allows managers to assign a uniform start and end time for all employees for the entire week.

Managers can quickly update shift timings for all employees without handling individual schedules initially.



3. Individual Day and Employee Modifications:

Managers can later select any individual employee or specific day to modify the start and end times as needed.

This ensures flexibility to accommodate individual requirements.



4. Save Shift Timings:

Once the manager is satisfied with the schedule, they can save the shift timings for the selected week.



5. Department-Level Filters:

Managers can apply filters to manage shifts by department.

They can choose to filter by a single department or multiple departments to view and assign shifts efficiently.



6. Customizable and Adaptable:

The system supports individual adjustments even after assigning common shifts, ensuring accuracy and alignment with specific employee or department needs.




Data Management

All shift timings entered or modified are securely saved to an Azure database, ensuring data safety, integrity, and accessibility for future reference or audit. This setup facilitates centralized management and enhances overall efficiency.







Objective

The Employee Shift Screen is a web application that allows managers to assign, view, and update weekly shift timings for their employees. It provides a streamlined way to manage and adjust employee schedules effectively.

Key Features

1. View and Edit Shift Timings:

Managers can view and edit the entire week's shift timings for an employee or update timings for individual days as needed.

This flexibility ensures that shift schedules can be modified to accommodate changes.



2. Filter Options:

By default, the application displays all employees under the manager.

Managers can apply filters, such as department name, to narrow down the list of employees.

These filters make it easier to manage and edit shifts for specific groups of employees.



3. User-Friendly Interface:

The application provides a clear and organized interface for managers to update shift timings quickly and efficiently.



4. Data Storage:

All shift data entered or updated is securely stored in an Azure database, ensuring data safety and accessibility for future reference.



5. Improved Workforce Management:

By providing the ability to manage shifts by department or employee, the tool helps ensure proper planning and allocation of resources.















from flask import request, jsonify
from sqlalchemy import or_

@jwt_required()
def get(self):
    # Extract query parameters from the request
    mng_id = request.args.get("EMPID")
    department_names = request.args.get("department_name")  # Comma-separated string

    # Validate the presence of EMPID
    if not mng_id:
        return jsonify({"error": "Manager ID (EMPID) is required"}), 400
    
    if not department_names:
        return jsonify({"error": "Department name is required"}), 400

    # Split department_names into a list if it's a comma-separated string
    department_list = department_names.split(",") if "," in department_names else [department_names]

    with session_scope('DESIGNER') as session:
        try:
            # Query employees with matching manager ID and department name(s)
            employees = (
                session.query(EmployeeInfo)
                .filter(EmployeeInfo.func_mgr_id == mng_id)
                .filter(EmployeeInfo.dept_name.in_(department_list))
                .filter(EmployeeInfo.term_date.is_(None))  # Termination date should be NULL
                .all()
            )

            # If no employees are found, return an empty list
            if not employees:
                return jsonify({"employees": []}), 200

            # Prepare the response
            employee_list = [
                {"emp_id": emp.emplid, "emp_name": emp.name} for emp in employees
            ]

            return jsonify({"employees": employee_list}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500






from flask import request, jsonify
from sqlalchemy import or_

@jwt_required()
def get(self):
    # Parse payload from the request
    payload = request.get_json()
    
    if not payload:
        return jsonify({"error": "Payload is required"}), 400

    # Extract EMPID and Department Name from the payload
    mng_id = payload.get("EMPID")
    department_names = payload.get("department_name")  # This can be a single string or a list

    # Validate the presence of EMPID
    if not mng_id:
        return jsonify({"error": "Manager ID (EMPID) is required"}), 400
    
    if not department_names:
        return jsonify({"error": "Department name is required"}), 400

    # Ensure department_names is a list for consistency
    if isinstance(department_names, str):
        department_names = [department_names]

    with session_scope('DESIGNER') as session:
        try:
            # Query employees with matching manager ID and department name(s)
            employees = (
                session.query(EmployeeInfo)
                .filter(EmployeeInfo.func_mgr_id == mng_id)
                .filter(EmployeeInfo.dept_name.in_(department_names))
                .filter(EmployeeInfo.term_date.is_(None))  # Termination date should be NULL
                .all()
            )

            # If no employees are found, return an empty list
            if not employees:
                return jsonify({"employees": []}), 200

            # Prepare the response
            employee_list = [
                {"emp_id": emp.emplid, "emp_name": emp.name} for emp in employees
            ]

            return jsonify({"employees": employee_list}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500




import requests  # Assuming you're using the requests library

def send_request(client, method, endpoint, data=None):
    """
    Generic function to handle client requests.

    Args:
        client: The client instance (e.g., a session object or API client).
        method: HTTP method as a string ('GET', 'POST', 'PUT', 'DELETE').
        endpoint: The API endpoint.
        data: The data to send with the request (optional).

    Returns:
        Response object from the client.
    """
    method = method.upper()
    if method == 'GET':
        response = client.get(endpoint)
    elif method == 'POST':
        response = client.post(endpoint, json=data)
    elif method == 'PUT':
        response = client.put(endpoint, json=data)
    elif method == 'DELETE':
        response = client.delete(endpoint, json=data)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    return response








class EmployeeShiftDetails(Resource):
    def get(self):
        try:
            mng_id = request.args.get('emp_id')
            if not mng_id:
                return {
                    "error": "Manager ID (emp_id) is missing in the request parameters."
                }, 400

            # Start processing
            with session_scope('DESIGNER') as session:
                # Query for employees under this manager
                employees = session.query(EmployeeInfo).filter_by(func_mgr_id=mng_id).all()
                if not employees:
                    return {
                        "error": f"No employees found for the given Manager ID: {mng_id}",
                        "reporting_employee_shifts": [],
                        "reporting_employees": []
                    }, 404

                # Extract employee IDs
                employee_ids = [emp.emplid for emp in employees]
                if not employee_ids:
                    return {
                        "error": f"No employee IDs found for the given Manager ID: {mng_id}.",
                        "reporting_employee_shifts": [],
                        "reporting_employees": []
                    }, 404

                # Query for employee shifts
                employee_shifts = session.query(EmployeeShiftInfo).filter(
                    EmployeeShiftInfo.emp_id.in_(employee_ids)
                ).all()

                # Prepare shift data
                if employee_shifts:
                    reporting_employee_shifts = [
                        {
                            'emp_id': emp.emp_id,
                            'day_id': emp.day_id,
                            'is_week_off': emp.is_week_off,
                            'start_time': str(emp.start_time),
                            'end_time': str(emp.end_time),
                            'contracted_hours': emp.emp_contracted_hours,
                            'shift_date': str(emp.shift_date)
                        }
                        for emp in employee_shifts
                    ]
                else:
                    reporting_employee_shifts = []

                # Prepare employee data
                reporting_employees = [
                    {'emp_id': emp.emplid, 'emp_name': emp.name} for emp in employees
                ]

                # Prepare final response
                data = {
                    "reporting_employee_shifts": reporting_employee_shifts,
                    "reporting_employees": reporting_employees
                }
                return jsonify(data), 200

        except Exception as e:
            # Include error details in the response
            return {
                "error": "An unexpected error occurred.",
                "details": str(e)
            }, 500








# Subquery to fetch relevant employee IDs
subquery = session.query(EmployeeShiftInfo.emp_id).filter(
    EmployeeShiftInfo.emp_id.in_(emp_ids)
).subquery()

# Main query using the subquery
employees_shifts = (
    session.query(EmployeeShiftInfo)
    .filter(
        EmployeeShiftInfo.emp_id.in_(subquery),
        EmployeeShiftInfo.shift_date.between(from_date, to_date)
    )
    .all()
)




class EmployeeShiftTimeDetails(Resource):
    @jwt_required()
    def get(self):
        """
        This is a get method for fetching reporting employees' shift details
        within a specified date range.
        """
        mng_id = request.args.get('emp_id')
        from_date = request.args.get('from_date')  # Expected format: MM-DD-YYYY
        to_date = request.args.get('to_date')      # Expected format: MM-DD-YYYY

        if not mng_id or not from_date or not to_date:
            return {"error": "Missing emp_id, from_date, or to_date in request parameters."}, 400

        try:
            # Convert dates to a standard format
            from_date = datetime.strptime(from_date, "%m-%d-%Y")
            to_date = datetime.strptime(to_date, "%m-%d-%Y")
        except ValueError:
            return {"error": "Invalid date format. Use MM-DD-YYYY."}, 400

        with session_scope('DESIGNER') as session:
            # Get employee IDs for the given manager
            employees = session.query(EmployeeInfo).filter_by(func_mgr_id=mng_id).all()
            employee_ids = [emp.emp_id for emp in employees]

            # Fetch shifts for employees within the date range
            employees_shifts = (
                session.query(EmployeeShiftInfo)
                .filter(
                    EmployeeShiftInfo.emp_id.in_(employee_ids),
                    EmployeeShiftInfo.shift_date.between(from_date, to_date)
                )
                .all()
            )

            if employees_shifts:
                reporting_employee_shifts = [
                    {
                        "emp_id": emp.emp_id,
                        "day_id": emp.day_id,
                        "is_week_off": emp.is_week_off,
                        "start_time": str(emp.start_time),
                        "end_time": str(emp.end_time),
                        "contracted_hours": emp.emp_contracted_hours,
                        "shift_date": str(emp.shift_date),
                    }
                    for emp in employees_shifts
                ]
            else:
                reporting_employee_shifts = []

            # Fallback to reporting employee names if no shifts are found
            reporting_employees = [
                {"emp_id": emp.emp_id, "emp_name": emp.name} for emp in employees
            ]

            # Prepare response data
            data = {
                "reporting_employee_shifts": reporting_employee_shifts,
                "reporting_employees": reporting_employees,
            }

        return jsonify(data)






<template>
  <v-container>
    <!-- East Monday Date Picker -->
    <v-date-picker
      v-model="startDate"
      :allowed-dates="isMonday"
      label="Select East Monday"
    ></v-date-picker>
    <v-spacer />
    <!-- East Sunday Date Picker -->
    <v-date-picker
      v-model="endDate"
      :allowed-dates="isSundayAfterStart"
      label="Select East Sunday"
      :disabled="!startDate"
    ></v-date-picker>
  </v-container>
</template>

<script>
export default {
  data() {
    return {
      startDate: null, // Selected East Monday
      endDate: null, // Selected East Sunday
    };
  },
  methods: {
    // Allow only Mondays
    isMonday(date) {
      const day = new Date(date).getDay();
      return day === 1; // 1 = Monday
    },
    // Allow only Sundays after the selected start date
    isSundayAfterStart(date) {
      const day = new Date(date).getDay();
      if (day !== 0) return false; // 0 = Sunday
      if (!this.startDate) return true; // If no start date is selected, allow all Sundays
      const startDateObj = new Date(this.startDate);
      const currentDateObj = new Date(date);
      return currentDateObj > startDateObj; // Allow only Sundays after the start date
    },
  },
};
</script>





<template>
  <v-container>
    <v-date-picker 
      v-model="startDate" 
      :allowed-dates="isMonday" 
      label="Select Start Date (Mondays Only)"
    ></v-date-picker>
  </v-container>
</template>

<script>
export default {
  data() {
    return {
      startDate: null, // Bind your date value here
    };
  },
  methods: {
    isMonday(date) {
      const day = new Date(date).getDay();
      return day === 1; // 1 represents Monday
    },
  },
};
</script>







def check_existing_entries(existing_volume_entry, existing_config_entry):
    """
    Checks for existing entries and returns an appropriate response.

    Parameters:
    - existing_volume_entry: Result of the volume entry query
    - existing_config_entry: Result of the config entry query

    Returns:
    - Tuple (response, status_code) if an entry exists, otherwise None
    """
    if existing_volume_entry or existing_config_entry:
        message = (
            "Volume Entry Already Exists in Volume Store"
            if existing_volume_entry
            else f"Entry already exists with Request ID: {existing_config_entry.request_id}"
        )
        return {"message": message}, 400
    return None

# Usage
response = check_existing_entries(existing_volume_entry, existing_config_entry)
if response:
    return response


-----------------




import time

def start_window_monitoring(self):
    try:
        background_check_interval = 300  # 5 minutes in seconds
        last_background_check = time.time()

        while True:
            # Check the active window
            active_window = gw.getActiveWindowTitle()

            if active_window:
                for win in self.all_window_titles:
                    if win in active_window:
                        # Start monitoring for the matched active window
                        active_matched_title = win
                        workflow_details = f.whitelist.GET_WORKFLOW_DETAILS_FROM_TITLE(active_matched_title)
                        target_window_titles = workflow_details.get("window_titles", "").split(",")
                        target_workflow_name = workflow_details.get("workflow_name", "")
                        target_workflow_id = workflow_details.get("workflow_id", "")

                        if target_workflow_name.lower() == "ows":
                            self.start_ows_monitoring(target_window_titles, target_workflow_name, target_workflow_id)
                        break

            # Perform a background check every 5 minutes
            if time.time() - last_background_check >= background_check_interval:
                current_titles = set(gw.getAllTitles())
                for title in current_titles:
                    for win in self.all_window_titles:
                        if win in title:
                            # Start monitoring for the matched background window
                            active_matched_title = win
                            workflow_details = f.whitelist.GET_WORKFLOW_DETAILS_FROM_TITLE(active_matched_title)
                            target_window_titles = workflow_details.get("window_titles", "").split(",")
                            target_workflow_name = workflow_details.get("workflow_name", "")
                            target_workflow_id = workflow_details.get("workflow_id", "")

                            if target_workflow_name.lower() == "ows":
                                self.start_ows_monitoring(target_window_titles, target_workflow_name, target_workflow_id)
                            break
                last_background_check = time.time()  # Update the last background check time

            # Sleep to reduce CPU usage
            time.sleep(1)
    except Exception as e:
        # Handle exceptions as needed
        # log_print(f"An error occurred: {e}")
        pass




----------




import time
import gw  # Assuming gw is the library to get window titles

def start_window_monitoring(self):
    try:
        while True:
            # Get the active window title
            active_window = gw.getActiveWindowTitle()  # Replace with the correct method to get the active window title

            if active_window:
                # Check if the active window matches any of the monitored titles
                for win in self.all_window_titles:
                    if win in active_window:
                        # log_print(f"Active window {win} is running on User's system")
                        active_matched_title = win

                        # Get workflow details
                        workflow_details = f.whitelist.GET_WORKFLOW_DETAILS_FROM_TITLE(active_matched_title)
                        target_window_titles = workflow_details.get("window_titles", "").split(",")
                        target_workflow_name = workflow_details.get("workflow_name", "")
                        target_workflow_id = workflow_details.get("workflow_id", "")

                        if target_workflow_name.lower() == "ows":
                            # log_print("OWS Application Monitoring Started")
                            self.start_ows_monitoring(target_window_titles, target_workflow_name, target_workflow_id)

                        # Break after processing the matched window
                        break

            # Introduce a sleep interval to reduce CPU usage
            time.sleep(1)  # Adjust the interval as needed (e.g., 0.5 or 2 seconds)
    except Exception as e:
        # Handle exceptions
        # log_print(f"An error occurred: {e}")
        pass







import os
import json
import threading
from datetime import datetime, timedelta
import schedule
import time

# Function to process files with state__change__detected = True
def process_file(file_name):
    print(f"Processing file: {file_name}")

# Function to check for `.json` files and process them based on conditions
def check_json_files(directory):
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            
            # Check the content of the JSON file
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    if data.get("state__change__detected", False):  # If key is True
                        thread = threading.Thread(target=process_file, args=(file_name,))
                        thread.start()
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

# Function to delete files older than a certain number of days
def delete_old_files(directory, days=2):
    cutoff_time = datetime.now() - timedelta(days=days)
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            if modified_time < cutoff_time:
                try:
                    os.remove(file_path)
                    print(f"Deleted: {file_name}")
                except Exception as e:
                    print(f"Error deleting {file_name}: {e}")

# Combined function to execute tasks
def run_scheduled_tasks(directory):
    print(f"Running tasks at {datetime.now()}")
    check_json_files(directory)
    delete_old_files(directory, days=2)

# Function to run the scheduler in a separate thread
def start_scheduler(directory):
    def scheduler_thread():
        schedule.every(1).hours.do(run_scheduled_tasks, directory=directory)
        print("Scheduler thread started. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(1)

    thread = threading.Thread(target=scheduler_thread, daemon=True)
    thread.start()
    return thread

# Main execution
if __name__ == "__main__":
    directory_path = "path_to_your_directory"

    # Start the scheduler as a thread
    start_scheduler(directory_path)

    # Keep the main thread alive to prevent the script from exiting
    while True:
        time.sleep(1)





import os
import json
import threading
from datetime import datetime, timedelta

# Function to process files with state__change__detected = True
def process_file(file_name):
    print(f"Processing file: {file_name}")

# Function to check for `.json` files and process them based on conditions
def check_json_files(directory):
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            
            # Check the content of the JSON file
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    if data.get("state__change__detected", False):  # If key is True
                        thread = threading.Thread(target=process_file, args=(file_name,))
                        thread.start()
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

# Function to check if the file's modified date is older than 2 days
def check_file_age(directory, days=2):
    cutoff_time = datetime.now() - timedelta(days=days)
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            if modified_time < cutoff_time:
                print(f"{file_name} was modified more than {days} days ago.")

# Example usage
if __name__ == "__main__":
    directory_path = "path_to_your_directory"

    # Check JSON files for state__change__detected
    check_json_files(directory_path)

    # Check JSON files for modified date older than 2 days
    check_file_age(directory_path, days=2)

----++++++++--------



from pywinauto import Application
from pywinauto import mouse
from pywinauto.keyboard import send_keys

# Connect to the active application
app = Application(backend="uia").connect(title_re=".*")  # Connect to the active window
window = app.active_window()

# Locate the toolbar control
toolbar = window.child_window(control_type="ToolBar")  # Adjust control_type or title if necessary

# Locate the left button in the toolbar
left_button = toolbar.child_window(title="Left", control_type="Button")  # Use the actual title of the button

# Click the left button to expand the section
left_button.click_input()

# Optionally, drag or resize the section (if necessary)
# Assuming you need to drag to expand
mouse.drag_mouse((100, 200), (300, 400))  # Replace with actual coordinates for dragging

# Take a screenshot of the window after expanding
window.capture_as_image().save("screenshot.png")

print("Section expanded and screenshot saved!")






import json

def get_active_keys(json_file, expected_workflow_name):
    # Load the JSON data
    with open(json_file, 'r') as file:
        data = json.load(file)  # Parse the JSON file into a list of dictionaries
    
    # Filter dictionaries based on workflow name and isActive
    filtered_keys = [
        entry["activity_key_name"]
        for entry in data
        if entry.get("workflow_name") == expected_workflow_name and entry.get("isActive") is True
    ]
    
    # Return the list of matching keys
    return filtered_keys

# Example usage
json_file_path = 'path_to_your_json_file.json'
workflow_name = "Workflow A"
result = get_active_keys(json_file_path, workflow_name)
print(f"Active keys for workflow '{workflow_name}': {result}")




import pandas as pd
import sqlite3
from datetime import datetime

def insert_with_to_sql(df, db_path, table_name):
    # Connect to SQLite database
    con = sqlite3.connect(db_path)
    try:
        # Insert DataFrame into the SQLite table
        df.to_sql(table_name, con, if_exists='append', index=False)
        print("Data inserted successfully using to_sql.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        con.close()






import os
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def process_json_files(folder_path, emp_id, db_connection_string):
    # Prepare the output DataFrame
    dataframes = []
    
    # Get current date and time
    cal_date = datetime.now().strftime("%m/%d/%Y")
    created_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Iterate through each file in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):  # Process only JSON files
            file_path = os.path.join(folder_path, file_name)
            
            with open(file_path, 'r') as file:
                json_data = json.load(file)
            
            # Prepare the activity_info field
            activity_info = [{
                'w_id': 1089,
                'w_name': 'ows',
                'activity_pairs': json_data,
                'capture_method': 'copy'
            }]
            
            # Create a DataFrame for the current file
            df = pd.DataFrame([{
                'Cal_date': cal_date,
                'emp_id': emp_id,
                'file_name': file_name,
                'Activity_info': json.dumps(activity_info),  # Convert to JSON string
                'volume_info': json.dumps([]),  # Empty list as JSON string
                'created_date': created_date
            }])
            
            dataframes.append(df)
    
    # Combine all DataFrames
    final_df = pd.concat(dataframes, ignore_index=True)
    
    # Insert into the local database
    engine = create_engine(db_connection_string)
    final_df.to_sql('your_table_name', con=engine, if_exists='append', index=False)
    
    print("Data successfully inserted into the database.")

# Usage Example
folder_path = "path_to_your_folder"
emp_id = "EMP123"
db_connection_string = "sqlite:///local_database.db"  # Replace with your database connection string
process_json_files(folder_path, emp_id, db_connection_string)



-----------



import os
import json
import shutil

def process_json(file_name, source_folder, target_folder, key_list, padding_details, key_to_split):
    # Construct full file path
    file_path = os.path.join(source_folder, file_name)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File '{file_name}' does not exist in {source_folder}.")
        return
    
    # Read the JSON file
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {e}")
        return

    # Process only the keys in the key_list
    processed_data = {}
    for key in key_list:
        if key in data:
            # Check for the specific key to split
            if key == key_to_split:
                value = data[key]
                if isinstance(value, str) and ':' in value:
                    # Split by colon and extract the desired part
                    parts = value.split(':')
                    if len(parts) > 2:
                        country = parts[1].strip()  # Extract between the first and second colon
                        processed_data['country'] = country
            # Add the original key-value to processed data
            processed_data[key] = data[key]

    # Add the padding details
    processed_data.update(padding_details)
    
    # Write the updated JSON to the target folder
    target_path = os.path.join(target_folder, file_name)
    try:
        with open(target_path, 'w') as output_file:
            json.dump(processed_data, output_file, indent=4)
        print(f"Processed file moved to {target_folder}")
    except Exception as e:
        print(f"Error writing to file: {e}")
        return
    
    # Move the original file to the target folder
    try:
        shutil.move(file_path, target_path)
    except Exception as e:
        print(f"Error moving file: {e}")

# Example usage
file_name = "example.json"
source_folder = "source"
target_folder = "processed"
key_list = ["key1", "key2", "key3", "key_to_split"]
padding_details = {"added_detail1": "value1", "added_detail2": "value2"}
key_to_split = "key_to_split"  # The key whose value needs to be split

process_json(file_name, source_folder, target_folder, key_list, padding_details, key_to_split)







git for-each-ref --sort=-committerdate refs/remotes/ --format='%(committerdate:iso8601) %(refname:short)'



To decide whether to store your data in separate JSON files or in a single JSON file with keys representing file names, let’s analyze the pros and cons of each approach with respect to your specific requirements:


---

1. Storing Data in Separate JSON Files

Advantages

1. Simpler File Management:

Each file corresponds directly to a unique KSID (KSID_<number>.json), making it easy to locate, edit, and manage.



2. Efficient Data Replacement:

To replace data, you can directly find the file by its name and overwrite the content without parsing a large file.



3. Failure Isolation:

Corruption of one file does not affect others.



4. Scalable for Large Data:

If your data grows significantly, this method prevents a single JSON file from becoming too large to handle efficiently.



5. Parallel Processing:

Multiple files can be processed independently by threads or workers.




Disadvantages

1. Disk Space Overhead:

Storing metadata (e.g., file headers) for each file adds overhead.



2. File System Dependency:

If you have thousands of files, the file system’s performance may degrade due to excessive file reads/writes.



3. State Checking:

You have to iterate over multiple files every 15 seconds to check the state_change_detected field.





---

2. Storing Data in a Single JSON File

Advantages

1. Compact Structure:

All data is stored in one place, reducing the overhead of managing multiple files.



2. Simpler Checking:

You can load the entire JSON file into memory and iterate through the keys to check for state_change_detected, avoiding file I/O for each check.



3. Easier Backup:

One file is easier to copy or move to another location for backup or transfer.




Disadvantages

1. Data Replacement Overhead:

To update a single KSID entry, you need to load the entire JSON file, modify the required key, and save the whole file back. This can be inefficient if the file grows large.



2. Risk of Corruption:

If the file gets corrupted, all data is lost.



3. Memory Usage:

For large datasets, loading the entire JSON file into memory could be inefficient or impractical.



4. Concurrency Issues:

Concurrent updates to the file might lead to race conditions or conflicts unless managed carefully.





---

Recommendation Based on Your Use Case

End Goal:

Check for state_change_detected every 15 seconds and move relevant data to another database.

Replace data dynamically as needed when hovering on a KSID.


Optimal Approach:

Use separate JSON files. Here’s why:

1. State Checking: Since you're checking periodically for a specific value (state_change_detected), iterating over separate files is straightforward and avoids loading unnecessary data.


2. Dynamic Updates: When updating data for a specific KSID, directly locating and modifying a file by its name (KSID_<number>.json) is faster and more efficient than searching for a key in a large JSON file.


3. Scalability: As your data grows, managing thousands of smaller files will perform better than handling a single massive file.


4. Error Isolation: If one file is corrupted, it doesn’t impact others.





---

Implementation Strategy

1. Folder Structure

Create a process folder for storing KSID files:

process/
  KSID_123.json
  KSID_456.json
  KSID_789.json


2. State Checking Script

Every 15 seconds, iterate over the files to check state_change_detected:

import os
import json
from time import sleep

process_folder = "process"

def check_state_changes():
    for file_name in os.listdir(process_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(process_folder, file_name)
            with open(file_path, "r") as file:
                data = json.load(file)
                if data.get("state_change_detected"):
                    # Move to Event Hub or database
                    move_to_event_hub(file_path, data)

def move_to_event_hub(file_path, data):
    # Implement your Event Hub logic here
    print(f"Moving {file_path} to Event Hub: {data}")
    # Optionally delete the file after processing
    os.remove(file_path)

# Periodically check for state changes
while True:
    check_state_changes()
    sleep(15)


3. Dynamic Updates

To update data dynamically for a KSID:

def update_ksid_data(ksid, new_data):
    file_path = os.path.join(process_folder, f"KSID_{ksid}.json")
    with open(file_path, "w") as file:
        json.dump(new_data, file)



---

When to Use a Single JSON File?

If your dataset is relatively small and unlikely to grow beyond a few MB.

If you prioritize simplicity and don't expect high update frequency.

If you can tolerate the risks of corruption and loading overhead.


However, for your case (frequent updates, scalable solution, and periodic checks), separate JSON files are better.









-----------------

def monitor_ows_process(target_titles, monitor_details):
    """Monitor the OWS process for Change State or Case Management windows."""
    global json_created

    while True:
        # Track active windows
        active_windows = gw.getAllTitles()
        case_management_open = any(title for title in active_windows if "Case Management -" in title)
        change_state_open = any(title for title in active_windows if "Change State" in title)

        if not case_management_open and not change_state_open:
            # Neither Case Management nor Change State is open, restart monitoring
            print("Neither Case Management nor Change State window is open. Restarting monitoring...")
            json_created = False  # Reset JSON created flag
            break  # Exit monitoring loop to restart tracking

        if json_created:
            # If JSON is already created, monitor for Change State Window only
            if change_state_open:
                print("Monitoring Change State Window for state changes...")
                # Add logic to handle Change State monitoring here
                monitor_change_state(monitor_details)
                break  # Exit loop once Change State monitoring starts
            else:
                print("Change State Window is not open. Waiting...")
        else:
            # JSON not created, check for Case Management Window
            if case_management_open:
                print("Capturing data from Case Management Window...")
                case_window, monitor_index, window_info = check_window_title(
                    "Case Management -", monitor_details
                )
                location, start_time = track_location(
                    summary_location, "Summary Button", monitor_index, monitor_details, case_window
                )
                capture_data(location, start_time, monitor_details, monitor_index, case_window)
            else:
                print("Case Management Window is not open. Waiting...")

        time.sleep(1)  # Pause briefly before checking again

def monitor_change_state(monitor_details):
    """Monitor the Change State Window for state changes."""
    while True:
        active_windows = gw.getAllTitles()
        change_state_open = any(title for title in active_windows if "Change State" in title)

        if not change_state_open:
            # If Change State Window is closed, exit monitoring
            print("Change State Window closed. Stopping monitoring...")
            break

        # Add logic for monitoring state changes within the Change State window
        print("Monitoring Change State Window...")
        time.sleep(1)





import time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor with enhanced state tracking
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Enhanced monitoring state variables
        self.data_dict = {}
        self.monitoring_active = True
        
        # State tracking variables
        self.summary_captured = False
        self.change_state_triggered = False
        self.waiting_for_next_capture = False

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = self.get_monitor_index(window, monitor_details)

                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }

                return window, monitor_index, window_info
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 20)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        
        if not summary_text:
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
        
        pyautogui.moveTo(curr_x, curr_y)
        
        # Reset state change detection
        self.data_dict = {}
        self.data_dict["state_change_detected"] = False

        # Parse summary text
        summary_dict = {}
        case_id = "dummy"
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
                if key == "Id":
                    case_id = value

        # Update data dictionary
        self.data_dict.update(summary_dict)
        self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.save_to_json(self.data_dict, case_id, False)
        
        self.summary_captured = True
        self.waiting_for_next_capture = False
        print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            
            if window_result[0]:
                window, monitor_index, window_info = window_result

                # Check for Change State window to determine if state change occurred
                change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                
                # Capture summary logic
                if (window.isActive and 
                    (not self.summary_captured or 
                     (self.waiting_for_next_capture and not change_state_result[0]))):
                    
                    start_time = datetime.now()
                    self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    self.data_dict["window_info"] = window_info
                    self.data_dict["monitor_index"] = monitor_index + 1

                    # Capture summary data
                    location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                    self.capture_data(location, start_time, monitor_details, monitor_index, window_info)
                
                # Check for Change State window
                if change_state_result[0]:
                    # Change State window is open
                    self.waiting_for_next_capture = True
                    self.summary_captured = False
                    self.change_state_triggered = True
                    print("Change State window detected. Preparing for next capture.")

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Create monitor with configurable titles
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()-----





import time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor with enhanced state tracking
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Enhanced monitoring state variables
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True
        self.case_management_open = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = False

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = self.get_monitor_index(window, monitor_details)

                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }

                return window, monitor_index, window_info
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        # Only capture summary if not already captured and window is active
        if not self.summary_captured:
            x, y = location
            curr_x, curr_y = pyautogui.position()
            pyperclip.copy('')

            dynamic_offset = (20, 20)
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
            
            if not summary_text:
                pyautogui.rightClick(x, y)
                pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
                pyautogui.click()
                summary_text = pyperclip.paste()
            
            pyautogui.moveTo(curr_x, curr_y)
            
            # Reset state change detection
            self.data_dict["state_change_detected"] = False

            # Parse summary text
            summary_dict = {}
            case_id = "dummy"
            lines = summary_text.split('\n')
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    key = parts[0].strip() if len(parts) > 0 else ""
                    value = parts[1].strip() if len(parts) > 1 else ""
                    summary_dict[key] = value
                    if key == "Id":
                        case_id = value

            # Update data dictionary
            self.data_dict.update(summary_dict)
            self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            self.save_to_json(self.data_dict, case_id, False)
            
            self.json_created = True
            self.summary_captured = True
            print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        
        # Reset flags for next cycle
        self.json_created = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = True
        
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            
            if window_result[0]:
                window, monitor_index, window_info = window_result
                self.case_management_open = True

                # If waiting for reopen and Case Management is open again
                if self.waiting_for_reopen:
                    # Reset waiting flag and prepare for next cycle
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

                # Capture summary data only if not already captured
                if window.isActive and not self.summary_captured:
                    start_time = datetime.now()
                    self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    self.data_dict["window_info"] = window_info
                    self.data_dict["monitor_index"] = monitor_index + 1

                    # Capture summary data
                    location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                    self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If summary captured, monitor for Change State window
                if self.json_created:
                    change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                    
                    if change_state_result[0]:
                        self.change_state_opened = True
                        self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Check if Change State window is closed
                    if self.change_state_opened:
                        change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                        
                        if not change_state_result[0]:
                            # Capture entire data
                            location, _ = self.track_location(self.end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
            else:
                # Case Management window is not open
                self.case_management_open = False
                
                # If we were previously waiting for reopen, reset flags
                if self.waiting_for_reopen:
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Optional: Customize location coordinates if needed
    # summary_location = (0.07, 0.7)
    # end_location = (0.7, 0.825)
    
    # Create monitor with configurable titles
    # If you want to customize locations, use: 
    # monitor = OWSProcessMonitor(ows_titles, summary_location, end_location)
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()






import time
imimport time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor with enhanced state tracking
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Enhanced monitoring state variables
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True
        self.case_management_open = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = False

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = self.get_monitor_index(window, monitor_details)

                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }

                return window, monitor_index, window_info
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        # Only capture summary if not already captured and window is active
        if not self.summary_captured:
            x, y = location
            curr_x, curr_y = pyautogui.position()
            pyperclip.copy('')

            dynamic_offset = (20, 20)
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
            
            if not summary_text:
                pyautogui.rightClick(x, y)
                pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
                pyautogui.click()
                summary_text = pyperclip.paste()
            
            pyautogui.moveTo(curr_x, curr_y)
            
            # Reset state change detection
            self.data_dict["state_change_detected"] = False

            # Parse summary text
            summary_dict = {}
            case_id = "dummy"
            lines = summary_text.split('\n')
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    key = parts[0].strip() if len(parts) > 0 else ""
                    value = parts[1].strip() if len(parts) > 1 else ""
                    summary_dict[key] = value
                    if key == "Id":
                        case_id = value

            # Update data dictionary
            self.data_dict.update(summary_dict)
            self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            self.save_to_json(self.data_dict, case_id, False)
            
            self.json_created = True
            self.summary_captured = True
            print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        
        # Reset flags for next cycle
        self.json_created = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = True
        
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            
            if window_result[0]:
                window, monitor_index, window_info = window_result
                self.case_management_open = True

                # If waiting for reopen and Case Management is open again
                if self.waiting_for_reopen:
                    # Reset waiting flag and prepare for next cycle
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

                # Capture summary data only if not already captured
                if window.isActive and not self.summary_captured:
                    start_time = datetime.now()
                    self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    self.data_dict["window_info"] = window_info
                    self.data_dict["monitor_index"] = monitor_index + 1

                    # Capture summary data
                    location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                    self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If summary captured, monitor for Change State window
                if self.json_created:
                    change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                    
                    if change_state_result[0]:
                        self.change_state_opened = True
                        self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Check if Change State window is closed
                    if self.change_state_opened:
                        change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                        
                        if not change_state_result[0]:
                            # Capture entire data
                            location, _ = self.track_location(self.end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
            else:
                # Case Management window is not open
                self.case_management_open = False
                
                # If we were previously waiting for reopen, reset flags
                if self.waiting_for_reopen:
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Optional: Customize location coordinates if needed
    # summary_location = (0.07, 0.7)
    # end_location = (0.7, 0.825)
    
    # Create monitor with configurable titles
    # If you want to customize locations, use: 
    # monitor = OWSProcessMonitor(ows_titles, summary_location, end_location)
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()port re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Monitoring state variables
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        while self.monitoring_active:
            windows = gw.getAllTitles()
            for win in windows:
                if target_title in win:
                    window = gw.getWindowsWithTitle(win)[0]
                    monitor_index = self.get_monitor_index(window, monitor_details)

                    window_info = {
                        "left": window.left,
                        "top": window.top,
                        "width": window.width,
                        "height": window.height
                    }

                    return window, monitor_index, window_info
            time.sleep(1)
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 20)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        
        if not summary_text:
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
        
        pyautogui.moveTo(curr_x, curr_y)
        
        # Reset state change detection
        self.data_dict["state_change_detected"] = False

        # Parse summary text
        summary_dict = {}
        case_id = "dummy"
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
                if key == "Id":
                    case_id = value

        # Update data dictionary
        self.data_dict.update(summary_dict)
        self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.save_to_json(self.data_dict, case_id, False)
        
        self.json_created = True
        print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.json_created = False

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            if not window_result[0]:
                continue

            window, monitor_index, window_info = window_result
            
            if window.isActive:
                start_time = datetime.now()
                self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                self.data_dict["window_info"] = window_info
                self.data_dict["monitor_index"] = monitor_index + 1

                # Capture summary data
                location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If JSON created, monitor for Change State window
                while self.json_created:
                    change_state_found = False
                    change_state_window = None

                    # Wait for Change State window
                    while not change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        for win in windows:
                            if self.change_state_title in win:
                                change_state_window = gw.getWindowsWithTitle(win)[0]
                                change_state_found = True
                                self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                break
                        time.sleep(1)

                    # Wait for Change State window to close
                    while change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        if self.change_state_title not in windows:
                            change_state_found = False
                            
                            # Capture entire data
                            location, _ = self.track_location(self.end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
                            break
                        time.sleep(1)

                    # Check if original windows are still active
                    windows = gw.getAllTitles()
                    if self.case_management_title not in windows:
                        break

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Optional: Customize location coordinates if needed
    # summary_location = (0.07, 0.7)
    # end_location = (0.7, 0.825)
    
    # Create monitor with configurable titles
    # If you want to customize locations, use: 
    # monitor = OWSProcessMonitor(ows_titles, summary_location, end_location)
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()



-----





import time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

# App store configurations
app_store = {
    "OWS": "Case Management - ,Change State", 
    "BPM": "Work Object,Process Work,Loan Maintenance"
}

app_store_location = {
    "OWS": {
        "Case Management -": [(0.07, 0.7)],
        "Change State": [(0.5, 0.825)]
    },
    "BPM": {
        "Work Object": [(0.07, 0.7), (0.5, 0.3)], 
        "Process Work": [(0.5, 0.825)],
        "Loan Maintenance": [(0.24, 0.09)]
    }
}

# OWS related capture coordinates
summary_location = (0.07, 0.7)
end_location = (0.7, 0.825)

class OWSProcessMonitor:
    def __init__(self, target_window_title):
        self.target_window_title = target_window_title
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        while self.monitoring_active:
            windows = gw.getAllTitles()
            for win in windows:
                if target_title in win:
                    window = gw.getWindowsWithTitle(win)[0]
                    monitor_index = self.get_monitor_index(window, monitor_details)

                    window_info = {
                        "left": window.left,
                        "top": window.top,
                        "width": window.width,
                        "height": window.height
                    }

                    return window, monitor_index, window_info
            time.sleep(1)
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 20)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        
        if not summary_text:
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
        
        pyautogui.moveTo(curr_x, curr_y)
        
        # Reset state change detection
        self.data_dict["state_change_detected"] = False

        # Parse summary text
        summary_dict = {}
        case_id = "dummy"
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
                if key == "Id":
                    case_id = value

        # Update data dictionary
        self.data_dict.update(summary_dict)
        self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.save_to_json(self.data_dict, case_id, False)
        
        self.json_created = True
        print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.json_created = False

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.target_window_title, monitor_details)
            if not window_result[0]:
                continue

            window, monitor_index, window_info = window_result
            
            if window.isActive:
                start_time = datetime.now()
                self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                self.data_dict["window_info"] = window_info
                self.data_dict["monitor_index"] = monitor_index + 1

                # Capture summary data
                location, _ = self.track_location(summary_location, monitor_index, monitor_details, window_info)
                self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If JSON created, monitor for Change State window
                while self.json_created:
                    change_state_found = False
                    change_state_window = None

                    # Wait for Change State window
                    while not change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        for win in windows:
                            if "Change State" in win:
                                change_state_window = gw.getWindowsWithTitle(win)[0]
                                change_state_found = True
                                self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                break
                        time.sleep(1)

                    # Wait for Change State window to close
                    while change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        if "Change State" not in windows:
                            change_state_found = False
                            
                            # Capture entire data
                            location, _ = self.track_location(end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
                            break
                        time.sleep(1)

                    # Check if original windows are still active
                    windows = gw.getAllTitles()
                    if self.target_window_title not in windows:
                        break

            time.sleep(2)

def main():
    target_window_title = "Case Management -"
    monitor = OWSProcessMonitor(target_window_title)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()






--------------------
def save_to_json(data, file_name):
    """Save data to a JSON file."""
    file_path = os.path.join(process_folder, f"{file_name}.json")

    # Check if file exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            existing_data = json.load(file)
            # Check the state_change_detected key
            if existing_data.get("state_change_detected", False):
                print(f"File '{file_name}.json' already exists and has 'state_change_detected' as True. Overwriting...")
            else:
                print(f"File '{file_name}.json' exists but 'state_change_detected' is False. Skipping update.")
                return

    # Save updated or new data
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to '{file_name}.json'.")


def capture_entire_data(location, start_id_time, monitor_details, monitor_index, window_info):
    """Capture data and save it to JSON with dynamic offset."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyperclip.copy('')

    # Perform actions with the adjusted offset
    dynamic_offset = (20, 20)
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
    pyautogui.click()
    summary_text = pyperclip.paste()
    pyautogui.moveTo(curr_x, curr_y)
    print("Log Text copied:", summary_text)

    summary_dict = {}
    lines = summary_text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        if "Action by" in line:
            date_time = extract_date_time(line)
            summary_dict["Action By Date/Time"] = date_time

            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if "transition" in next_line:
                    transition_text = extract_transition(next_line)
                    summary_dict["Transition"] = transition_text
                    break

    summary_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Log Dict", summary_dict)

    # Compare "Action By Date/Time" with "state_change_time"
    if "Action By Date/Time" in summary_dict:
        action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
        state_change_time = datetime.strptime(data_dict.get("state_change_time", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")

        if action_by_time > state_change_time:
            data_dict["state_changed_true"] = True
            data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("State change detected. Marked as true.")

    data_dict.update(summary_dict)
    save_to_json(data_dict, summary_dict.get("Transition", "dummy"))
    print("JSON created with Log data.")






import re
from datetime import datetime

def extract_date_time(line):
    """
    Dynamically identifies and extracts date and time in the format 'MM/DD/YY HH:MM AM/PM' 
    from the given line, then converts it to 'YYYY-MM-DD HH:MM:SS'.
    
    Example input:
    - "Action by John Doe on 11/28/24 5:54 PM"
    - "Completed on 12/29/23 11:23 AM"

    Returns:
    - Converted datetime string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    # Regular expression to match the date in MM/DD/YY format
    date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
    match = re.search(date_time_pattern, line)

    if match:
        date_part = match.group(1)  # Extract date (MM/DD/YY)
        time_part = match.group(2)  # Extract time (HH:MM)
        am_pm_part = match.group(3)  # Extract AM/PM

        # Combine extracted components into a single datetime string
        date_time_str = f"{date_part} {time_part} {am_pm_part}"
        
        # Convert to datetime object and format as 'YYYY-MM-DD HH:MM:SS'
        try:
            date_time_obj = datetime.strptime(date_time_str, "%m/%d/%y %I:%M %p")
            return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(f"Error parsing date and time: {e}")
            return "Invalid Date/Time"
    
    # If no match is found, return a placeholder
    return "No valid date/time found"





------------+++-----+-+
import cv2
import pyautogui

def scale_image_with_opencv(cropped_image_path, original_resolution, current_resolution):
    """
    Scales the cropped image using OpenCV to match the current screen resolution.

    Parameters:
        cropped_image_path (str): Path to the cropped image.
        original_resolution (tuple): (width, height) of the monitor where the image was cropped.
        current_resolution (tuple): (width, height) of the current monitor.

    Returns:
        str: Path to the scaled image.
    """
    # Load the cropped image
    cropped_image = cv2.imread(cropped_image_path, cv2.IMREAD_UNCHANGED)

    # Calculate scale factors
    original_width, original_height = original_resolution
    current_width, current_height = current_resolution
    width_scale = current_width / original_width
    height_scale = current_height / original_height

    # Calculate new dimensions
    new_width = int(cropped_image.shape[1] * width_scale)
    new_height = int(cropped_image.shape[0] * height_scale)

    # Resize the cropped image
    scaled_image = cv2.resize(cropped_image, (new_width, new_height), interpolation=cv2.INTER_LINEAR)

    # Save the scaled image temporarily
    scaled_image_path = "scaled_image.png"
    cv2.imwrite(scaled_image_path, scaled_image)

    return scaled_image_path

def locate_cropped_image_on_screen(cropped_image_path, original_resolution, current_resolution):
    """
    Scales the cropped image and uses PyAutoGUI to locate it on the visible screen.

    Parameters:
        cropped_image_path (str): Path to the cropped image.
        original_resolution (tuple): (width, height) of the monitor where the image was cropped.
        current_resolution (tuple): (width, height) of the current monitor.
    """
    # Scale the cropped image using OpenCV
    scaled_image_path = scale_image_with_opencv(cropped_image_path, original_resolution, current_resolution)

    # Locate the scaled image on the visible screen using PyAutoGUI
    location = pyautogui.locateOnScreen(scaled_image_path, confidence=0.8)  # Adjust confidence as needed
    if location:
        print(f"Image found at: {location}")
    else:
        print("Image not found on the screen.")

# Example usage
original_resolution = (1920, 1080)  # Resolution of the monitor where the image was cropped
current_resolution = (1366, 768)   # Resolution of the current monitor
cropped_image_path = "Event_Trigger/cropped_image.png"  # Path to the cropped image

locate_cropped_image_on_screen(cropped_image_path, original_resolution, current_resolution)





----------------------
def calculate_dynamic_offset(monitor_details, window_info, base_offset):
    """Calculate dynamic offset based on monitor and window size."""
    monitor_width = monitor_details['width']
    monitor_height = monitor_details['height']
    window_width = window_info['width']
    window_height = window_info['height']

    # Calculate scaling factors
    width_ratio = window_width / monitor_width
    height_ratio = window_height / monitor_height

    # Adjust the base offset dynamically
    dynamic_x = int(base_offset[0] * width_ratio)
    dynamic_y = int(base_offset[1] * height_ratio)

    print(f"Dynamic offsets calculated: ({dynamic_x}, {dynamic_y})")
    return dynamic_x, dynamic_y


def capture_data(location, start_id_time, monitor_details, window_info):
    """Capture data and save it to JSON with dynamic offset."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyperclip.copy('')

    # Calculate dynamic offset
    dynamic_offset = calculate_dynamic_offset(monitor_details, window_info, (30, 30))

    # Perform actions with the adjusted offset
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    case_id = "dummy"
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value

    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")


def capture_entire_data(location, start_id_time, monitor_details, window_info):
    """Capture data and save it to JSON with dynamic offset."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyperclip.copy('')

    # Calculate dynamic offset
    dynamic_offset = calculate_dynamic_offset(monitor_details, window_info, (10, 20))

    # Perform actions with the adjusted offset
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
    time.sleep(5)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Log Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Check for "Action by" and extract date/time
        if "Action by" in line:
            date_time = extract_date_time(line)
            summary_dict["Action By Date/Time"] = date_time

            # Look for "Transition" in the next line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if "transition" in next_line:
                    transition_text = extract_transition(next_line)
                    summary_dict["Transition"] = transition_text
                    break

    # Save additional metadata
    summary_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Log Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    save_to_json(data_dict, summary_dict.get("Transition", "dummy"))
    print("JSON created with Log data.")


-------------------------+++++++++++--------------




To integrate the logic you've provided with the initial tracking system, I'll modify the original code so it works with the specific window titles and locations from the app_store and app_store_location dictionaries. This means we’ll combine tracking of window titles, their respective actions, and handling the screen capture at the specified locations.

Here's how we can adapt the approach:

Modified Approach

1. AppStore & Locations: The app_store dictionary contains application names and window titles. The app_store_location dictionary contains the specific locations (as ratios) for each action within each window.


2. Monitor Window Title and Location: As we detect the active window title, we will then identify the corresponding application. Once identified, we will use the specific locations for each action to perform the required operations.


3. Trigger Actions Dynamically: We'll dynamically track the first window title for each application, and when matched, locate the specified position based on the defined action.



Refactored Code

import time
import threading
from datetime import datetime
import pyautogui
import pygetwindow as gw
from pynput.mouse import Listener

# Dummy AppStore Details -> Will be replaced with Database Queries
app_store = {
    "OWS": "Case Management -,Change State",
    "BPM": "Work Object,Process Work,Loan Maintenance"
}
app_store_location = {
    "OWS": {
        "Case Management -": [(0.07, 0.7)],
        "Change State": [(0.5, 0.825)]
    },
    "BPM": {
        "Work Object": [(0.07, 0.7), (0.5, 0.3)],
        "Process Work": [(0.5, 0.825)],
        "Loan Maintenance": [(0.24, 0.09)]
    }
}

# Global state to track running threads
running_threads = {}

def get_active_window_title():
    """Fetch the current active window title."""
    return gw.getActiveWindow().title

def get_app_from_window_title(window_title):
    """Identify the application name based on the active window title."""
    for app_name, titles in app_store.items():
        first_title = titles.split(",")[0].strip()  # Extract the first window title
        if window_title == first_title:
            return app_name
    return None

def start_tracker(app_name, action_name):
    """Start the tracker thread dynamically for the given app."""
    if app_name in running_threads:
        return  # Avoid starting duplicate threads

    # Fetch and start the tracker class
    tracker_class = globals().get(f"{app_name}Tracker")
    if not tracker_class:
        print(f"No tracker class found for {app_name}")
        return

    tracker_instance = tracker_class(action_name)  # Initialize the tracker class with action name
    thread = threading.Thread(target=tracker_instance.run)
    thread.daemon = True
    thread.start()
    running_threads[app_name] = thread
    print(f"Started tracker for {app_name} - {action_name}")

def monitor_windows():
    """Continuously monitor active windows and start trackers as needed."""
    while True:
        active_window = get_active_window_title()
        app_name = get_app_from_window_title(active_window)
        if app_name:
            # Get the corresponding action for the app (first action in the list for now)
            actions = app_store_location.get(app_name)
            for action_name, locations in actions.items():
                start_tracker(app_name, action_name)
                for location in locations:
                    monitor_location(location, action_name)
        time.sleep(1)

def monitor_location(location, action_name):
    """Monitor the location where action needs to be taken (e.g., click or capture data)."""
    x_ratio, y_ratio = location
    # Locate the window based on its coordinates and window info (would be dynamic in actual implementation)
    window = gw.getWindowsWithTitle(action_name)[0]  # Get the window by action name
    window_info = {"left": window.left, "top": window.top, "width": window.width, "height": window.height}
    
    # Calculate absolute coordinates relative to the window
    x = window_info["left"] + int(window_info["width"] * x_ratio)
    y = window_info["top"] + int(window_info["height"] * y_ratio)

    print(f"{action_name} located at ({x}, {y})")
    capture_data((x, y), datetime.now())

def capture_data(location, start_id_time):
    """Capture data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.copy('')
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + 10, y + 10)
    pyautogui.click()
    
    # For example, let's just log the capture action
    print(f"Captured data at {location} at {start_id_time}")

# Example tracker classes for specific actions
class OWSTracker:
    def __init__(self, action_name):
        self.action_name = action_name

    def run(self):
        while True:
            print(f"Tracking OWS for {self.action_name}...")
            time.sleep(5)

class BPMTracker:
    def __init__(self, action_name):
        self.action_name = action_name

    def run(self):
        while True:
            print(f"Tracking BPM for {self.action_name}...")
            time.sleep(5)

if __name__ == "__main__":
    monitor_windows()

Key Changes:

1. Dynamic Action Handling:

Based on the app_store_location dictionary, the code now tracks specific actions (e.g., Case Management -, Work Object) for each application.

The start_tracker method now passes the action name to the tracker class for each application.



2. Tracker Initialization:

The tracker classes (OWSTracker, BPMTracker) are initialized with an action name, and they will execute based on which action is being tracked.



3. Location Calculation:

For each action within an application, the location is calculated based on window ratios (x_ratio, y_ratio).

The coordinates are calculated dynamically based on the monitor and window size.



4. Window Interaction:

The capture_data method demonstrates how interaction with specific locations (like right-click and capture) can be implemented.




Next Steps:

Extend with More Actions: You can easily extend the system by adding more window titles and corresponding locations in app_store and app_store_location.

Customize Tracker Logic: Each tracker class (like OWSTracker or BPMTracker) can have customized logic to perform actions specific to the application, such as capturing data, clicking buttons, or navigating the application.


Let me know if you need further customization or enhancements!






# Your imports and definitions remain unchanged...

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles", windows)
        
        for win in windows:
            if target_title in win:
                print("Checking with", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window found:", window)
                monitor_index = get_monitor_index(window, monitor_details)

                # Capture window size and position
                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }
                print(f"Window size and position: {window_info}")

                return window, monitor_index, window_info  # Return active window, monitor index, and window info
        time.sleep(1)

def track_location(location_tuple, action_name, monitor_index, monitor_details, window_info):
    """Locate a specific location based on the active monitor and window size."""
    x_ratio, y_ratio = location_tuple
    monitor = monitor_details[monitor_index]

    # Calculate absolute coordinates relative to the window
    x = window_info["left"] + int(window_info["width"] * x_ratio)
    y = window_info["top"] + int(window_info["height"] * y_ratio)

    print(f"{action_name} located at ({x}, {y}) on monitor {monitor_index + 1}")
    return (x, y), datetime.now()

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index, window_info = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        print("Tracking active window...")
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index + 1}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

            # Add window and monitor info to the data
            data_dict["window_info"] = window_info
            data_dict["monitor_index"] = monitor_index + 1

            # Perform right-click and copy
            location, start_id_time = track_location(summary_location, "summary_button", monitor_index, monitor_details, window_info)
            capture_data(location, start_time)

            if json_created:
                print("JSON created. Monitoring for 'ChangeState' window...")
                
                # Monitor for 'ChangeState' window
                change_state_window = None
                while not change_state_window:
                    windows = gw.getAllTitles()
                    for win in windows:
                        if "Change State" in win:
                            print(f"'Change State' window detected: {win}")
                            change_state_window = gw.getWindowsWithTitle(win)[0]
                            break

                print("Bypassing by adding sleep time of 10s")
                time.sleep(10)

                # Wait for 'ChangeState' window to close
                print("'Change State' window closed. Proceeding to capture 'Copy All'...")
                end_time = datetime.now()
                time_difference = (end_time - start_time).total_seconds()

                # Check if time difference is within the threshold
                if time_difference <= 30:
                    print(f"Time difference is {time_difference} seconds, updating JSON...")
                    location, start_id_time = track_location(end_location, "end_button", monitor_index, monitor_details, window_info)
                    capture_entire_data(location, start_time)

                    # Update JSON with state change data
                    data_dict["state_change_detected"] = True
                    data_dict["state_change_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, "state_change")
                    return
                else:
                    print(f"Time difference of {time_difference} seconds exceeds threshold. Skipping update.")
            else:
                print("JSON not yet created. Continuing monitoring...")
            time.sleep(5)  # Adjust sleep time as needed





import win32gui
import ctypes
from ctypes import wintypes

user32 = ctypes.WinDLL('user32', use_last_error=True)

class RECT(ctypes.Structure):
    _fields_ = [
        ("left", wintypes.LONG),
        ("top", wintypes.LONG),
        ("right", wintypes.LONG),
        ("bottom", wintypes.LONG)
    ]

def find_window_by_partial_title(partial_title):
    def callback(hwnd, titles):
        if win32gui.IsWindowVisible(hwnd) and partial_title in win32gui.GetWindowText(hwnd):
            titles.append(hwnd)
    titles = []
    win32gui.EnumWindows(callback, titles)
    return titles[0] if titles else None

def get_window_rect(hwnd):
    rect = RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return rect

partial_title = "Notepad"  # Replace with part of your application's title
hwnd = find_window_by_partial_title(partial_title)
if hwnd:
    rect = get_window_rect(hwnd)
    print(f"Window Size: {rect.right - rect.left}x{rect.bottom - rect.top}")
    print(f"Position: ({rect.left}, {rect.top})")
else:
    print("Window not found!")



To determine the size and position of your application window (e.g., when it’s resized to half the screen width), you can use libraries like pygetwindow, pywinauto, or the Windows API (ctypes) to track the window's dimensions and coordinates, regardless of whether it is fullscreen or resized.

Approach Using pygetwindow

pygetwindow provides an easy way to retrieve the window's size and position.

Installation:

pip install PyGetWindow

Example:

import pygetwindow as gw

# Get a list of all open windows
windows = gw.getAllWindows()

# Find your specific application by title
for window in windows:
    if 'YourAppTitle' in window.title:  # Replace 'YourAppTitle' with the actual window title
        print(f"Title: {window.title}")
        print(f"Size: {window.width}x{window.height}")
        print(f"Position: ({window.left}, {window.top})")
        if window.width < 1920 or window.height < 1080:
            print("The window is not occupying the full screen.")
        break

This will give you the window's current size (width, height) and its position on the screen (left, top). You can use this information to check if the window is occupying the full screen.


---

Approach Using pywinauto

If you need more advanced control, pywinauto can be used to retrieve window size and detect if it’s not fullscreen.

Installation:

pip install pywinauto

Example:

from pywinauto import Application

# Connect to your application (replace 'YourAppTitle' with your window title)
app = Application().connect(title_re=".*YourAppTitle.*")
window = app.window(title_re=".*YourAppTitle.*")

# Get the window's rectangle (left, top, right, bottom)
rect = window.rectangle()
width = rect.width()
height = rect.height()

print(f"Window Size: {width}x{height}")
print(f"Position: ({rect.left}, {rect.top})")

if width < 1920 or height < 1080:
    print("The window is not occupying the full screen.")

This method can handle minimized and hidden windows as well.


---

Approach Using Windows API (ctypes)

For more low-level access, you can use ctypes to retrieve window dimensions using the Windows API.

Example:

import ctypes
from ctypes import wintypes

# Define Windows API functions and structures
user32 = ctypes.WinDLL('user32', use_last_error=True)

class RECT(ctypes.Structure):
    _fields_ = [
        ("left", wintypes.LONG),
        ("top", wintypes.LONG),
        ("right", wintypes.LONG),
        ("bottom", wintypes.LONG)
    ]

def get_window_rect(title):
    hwnd = user32.FindWindowW(None, title)  # Replace None with the title of your application window
    if not hwnd:
        raise Exception("Window not found!")
    rect = RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return rect

# Example usage
title = "YourAppTitle"  # Replace with the exact title of your window
rect = get_window_rect(title)
width = rect.right - rect.left
height = rect.bottom - rect.top

print(f"Window Size: {width}x{height}")
print(f"Position: ({rect.left}, {rect.top})")

if width < 1920 or height < 1080:
    print("The window is not occupying the full screen.")


---

Key Points

1. Full-Screen Check: Compare the window's width and height to the full resolution (e.g., 1920x1080) to determine if it’s fullscreen.

if window.width < 1920 or window.height < 1080:
    print("The window is not fullscreen.")


2. Coordinates: The left and top values of the window indicate its position on the screen.


3. Cross-Monitor Considerations: If using multiple monitors, ensure you account for their combined resolutions or the specific monitor coordinates.



Choose the method that best fits your requirements based on ease of use (pygetwindow) or control and extensibility (pywinauto or ctypes).






def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        print("Tracking active window...")
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

            # Perform right-click and copy
            location, _ = track_location(summary_location, "summary_button", monitor_index, monitor_details)
            capture_data(location, start_time)

            if json_created:
                print("JSON created. Monitoring for 'ChangeState' window...")
                
                # Monitor for 'ChangeState' window
                change_state_window = None
                while not change_state_window:
                    windows = gw.getAllTitles()
                    for win in windows:
                        if "ChangeState" in win:
                            print(f"'ChangeState' window detected: {win}")
                            change_state_window = gw.getWindowsWithTitle(win)[0]
                            break
                    time.sleep(1)

                # Wait for 'ChangeState' window to close
                while change_state_window.isVisible:
                    print("'ChangeState' window is still visible, waiting...")
                    time.sleep(1)

                print("'ChangeState' window closed. Proceeding to capture 'Copy All'...")
                end_time = datetime.now()
                time_difference = (end_time - start_time).total_seconds()

                # Check if time difference is within the threshold
                if time_difference <= 30:
                    print(f"Time difference is {time_difference} seconds, updating JSON...")
                    location, _ = track_location(end_location, "end_button", monitor_index, monitor_details)
                    capture_entire_data(location, start_time)

                    # Update JSON with state change data
                    data_dict["state_change_detected"] = True
                    data_dict["state_change_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, "state_change")
                else:
                    print(f"Time difference of {time_difference} seconds exceeds threshold. Skipping update.")
            else:
                print("JSON not yet created. Continuing monitoring...")
            time.sleep(5)  # Adjust sleep time as needed





import time
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

summary_location = (0.07, 0.7)
end_location = (0.5, 0.825)

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info:", monitor_details)
    return monitor_details


def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles:", windows)

        for win in windows:
            if target_title in win:
                print("Checking with:", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window is:", window)
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)


def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height,
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor["x"],
            "top": monitor["y"],
            "right": monitor["x"] + monitor["width"],
            "bottom": monitor["y"] + monitor["height"],
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect["left"], monitor_rect["left"])
        overlap_top = max(window_rect["top"], monitor_rect["top"])
        overlap_right = min(window_rect["right"], monitor_rect["right"])
        overlap_bottom = min(window_rect["bottom"], monitor_rect["bottom"])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")

        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None


def track_location(location_tuple, action_name, monitor_index, monitor_details):
    """Locate a specific location based on the active monitor."""
    x_ratio, y_ratio = location_tuple
    monitor = monitor_details[monitor_index]

    # Calculate absolute coordinates with padding
    x = monitor["x"] + int(monitor["width"] * x_ratio)
    y = monitor["y"] + int(monitor["height"] * y_ratio)

    print(f"{action_name} located at ({x}, {y}) on monitor {monitor_index + 1}")
    return (x, y), datetime.now()


def save_to_json(data, case_id):
    """Save tracked data to JSON with a formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        print("Tracking active window")
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(1)

            if not json_created:
                # Track summary button location
                location, start_id_time = track_location(summary_location, "Summary Button", monitor_index, monitor_details)
                if location:
                    save_to_json(data_dict, "summary_button")
                break  # Exit tracking after handling JSON creation
        time.sleep(1)


if __name__ == "__main__":
    target_title = "Your Window Title"  # Replace with your target window title
    monitor_process(target_title)




import json
import os
from datetime import datetime
import pyautogui
import pyperclip

data_dict = {}
process_folder = "./output"  # Update to your desired folder path


def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Check for "Action By" and extract date/time
        if "Action By" in line:
            date_time = extract_date_time(line)
            summary_dict["Action By Date/Time"] = date_time

            # Look for "Transition" in the next line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if "Transition" in next_line:
                    transition_text = extract_transition(next_line)
                    summary_dict["Transition"] = transition_text

    # Save additional metadata
    summary_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    save_to_json(data_dict, summary_dict.get("Id", "dummy"))
    print("JSON created with Summary data.")


def extract_date_time(line):
    """Extract date and time from a line."""
    # Assuming date/time is in the format "YYYY-MM-DD HH:MM:SS"
    parts = line.split()
    for part in parts:
        try:
            date_time = datetime.strptime(part, "%Y-%m-%d %H:%M:%S")
            return date_time.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return "Unknown Date/Time"


def extract_transition(line):
    """Extract transition text from a line."""
    if "Transition" in line:
        parts = line.split("Transition")
        if len(parts) > 1:
            return parts[1].strip()
    return "Unknown Transition"


def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    if not os.path.exists(process_folder):
        os.makedirs(process_folder)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


# Example usage (replace with your actual parameters)
capture_summary_data((100, 200), datetime.now())




import time
from pywinauto import Application, timings

# Set logging level to help debug and understand what's happening internally
timings.Timings.LOG_LEVEL = 3

def print_controls(window):
    """
    Recursively print all controls within the given window.
    """
    for control in window.children():
        try:
            control_type = control.friendly_class_name()
            control_text = control.window_text()
            print(f"Control Type: {control_type}, Text: '{control_text}'")

            # Recursively check child elements of complex controls like panels, groupboxes, etc.
            if control_type in ['TabControl', 'GroupBox', 'Pane', 'Panel', 'TabItem', 'Frame']:
                print_controls(control)  # Dig deeper if it's a container or group

        except Exception as e:
            print(f"Error inspecting control {control}: {e}")

def explore_tabs_and_controls(window):
    """
    Explore tabs and nested elements (if applicable), ensuring all controls are printed.
    """
    # First, ensure the main window is visible
    window.wait('visible', timeout=30)  # Wait for the main window to be visible

    # Print all controls at the top level
    print_controls(window)

    # If there are tabs, explore them as well
    if window.child_window(control_type="TabControl").exists():
        tab_control = window.child_window(control_type="TabControl")
        tabs = tab_control.children()

        # Iterate through each tab and explore its contents
        for tab in tabs:
            print(f"Exploring tab: {tab.window_text()}")
            tab.click_input(double=True)  # Double-click to select the tab
            time.sleep(1)  # Wait for tab to load content
            print_controls(window)  # Print all controls in the current tab

def main():
    # Connect to the application
    app = Application(backend="uia").connect(title_re=".*YourApplicationTitle.*")  # Adjust with your app's title
    window = app.window(title_re=".*YourApplicationTitle.*")  # Adjust window title regex to match

    # Ensure window is visible and ready for interaction
    window.wait('visible', timeout=30)
    print(f"Window {window.window_text()} is now visible.")

    # Call function to explore and print all controls
    explore_tabs_and_controls(window)

if __name__ == "__main__":
    main()




from pywinauto import Application
from pywinauto.findwindows import ElementNotFoundError

def list_controls_with_text(window):
    """
    Lists all controls with their text and control type in the given window.
    :param window: The Pywinauto WindowSpecification object.
    """
    print("All Controls and Their Text:")
    print("=" * 80)

    try:
        # Find all descendants (all elements in the window hierarchy)
        elements = window.descendants()
        for elem in elements:
            try:
                # Get the control type and text
                control_type = elem.friendly_class_name()
                control_text = elem.window_text()
                print(f"Control Type: {control_type}, Text: '{control_text}', Rect: {elem.rectangle()}")
            except Exception as e:
                print(f"Error processing element: {e}")
    except Exception as e:
        print(f"Error while retrieving controls: {e}")


# Main Execution
try:
    # Connect to the application window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print(f"Inspecting the 'Case Management' window for all controls and text:\n")
    list_controls_with_text(window)

except ElementNotFoundError:
    print("Error: The specified window 'Case Management' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")



from pywinauto import Application
from pywinauto.findwindows import ElementNotFoundError
from pywinauto.uia_defines import IUIA

def list_supported_controls(window):
    """
    Lists and prints all supported controls in the given window.
    :param window: The Pywinauto WindowSpecification object.
    """
    control_types = [
        "Button", "CheckBox", "RadioButton", "GroupBox", "ComboBox", "Edit",
        "Header", "ListBox", "ListView", "PopupMenu", "Static", "StatusBar",
        "TabControl", "Toolbar", "ToolTips", "TreeView", "UpDown"
    ]

    print("Supported Controls Found:")
    print("=" * 50)

    # Find all descendants and filter by control type
    try:
        elements = window.descendants()
        for elem in elements:
            try:
                control_type = elem.friendly_class_name()
                if control_type in control_types:
                    print(f"Control Type: {control_type}, Text: '{elem.window_text()}', "
                          f"Rect: {elem.rectangle()}")
            except Exception as e:
                print(f"Error processing element: {e}")
    except Exception as e:
        print(f"Error while listing controls: {e}")


# Main Execution
try:
    # Connect to the application window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print(f"Inspecting the 'Case Management' window for supported control types:\n")
    list_supported_controls(window)

except ElementNotFoundError:
    print("Error: The specified window 'Case Management' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")






from pywinauto import Application
from pywinauto.controls.uiawrapper import UIAWrapper
from pywinauto.findwindows import ElementNotFoundError

def print_element_hierarchy(element, level=0):
    """
    Recursively prints the hierarchy of all elements in the given element's subtree.
    :param element: The root element to inspect.
    :param level: The depth of the current element (for indentation).
    """
    try:
        indent = "  " * level
        element_type = element.control_type()
        element_text = element.window_text()
        element_rect = element.rectangle()

        print(f"{indent}- Control Type: {element_type}, Text: '{element_text}', "
              f"Rect: {element_rect}")

        # Recursively inspect all child elements
        for child in element.children():
            print_element_hierarchy(child, level + 1)
    except Exception as e:
        print(f"{indent}- Error accessing element: {e}")

# Main Execution
try:
    # Connect to the application window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print("Inspecting the entire element hierarchy of the 'Case Management' window:")
    print_element_hierarchy(window)
except ElementNotFoundError:
    print("Error: The specified window 'Case Management' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")





from pywinauto import Application

def print_and_find_element(window, level=0, search_text="summary"):
    """
    Recursively prints all elements in the window with their text and control type.
    If an element matches the search_text, performs right-click and copy operation.
    :param window: The window or container element.
    :param level: The depth level in the hierarchy (used for indentation).
    :param search_text: The text to search for in the elements.
    """
    try:
        elements = window.descendants()
        for element in elements:
            indent = "  " * level
            try:
                # Print the element's details
                text = element.window_text()
                control_type = element.control_type()
                print(f"{indent}- Text: '{text}', Control Type: '{control_type}'")

                # Check if the text matches the search target
                if search_text.lower() in text.lower():
                    print(f"{indent}>>> Found element with text '{search_text}'!")
                    # Right-click and copy
                    element.right_click_input()
                    element.type_keys("^c")  # Simulate Ctrl+C to copy
                    print(f"{indent}>>> Copied text from the element.")

                # Recursively print child elements if available
                print_and_find_element(element, level + 1, search_text)
            except Exception as e:
                print(f"{indent}- Error accessing element: {e}")
    except Exception as e:
        print(f"Error fetching descendants: {e}")

# Main program
try:
    # Connect to the "Case Management" window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print("Inspecting all elements in the 'Case Management' window...")
    print_and_find_element(window, search_text="summary")
except Exception as e:
    print(f"Error: {e}")




from pywinauto import Desktop, Application
from pywinauto.keyboard import send_keys
from datetime import datetime
import json
import os
import time

# Define paths for JSON output and other settings
process_folder = r"C:\pulse_event_trigger\process"
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def find_window_by_title(partial_title):
    """Find a window by its partial title."""
    while True:
        try:
            windows = Desktop(backend="uia").windows()
            for win in windows:
                if partial_title in win.window_text():
                    print(f"Found window: {win.window_text()}")
                    return win
        except Exception as e:
            print(f"Error finding window: {e}")
        time.sleep(1)


def find_element_by_text(window, search_text):
    """Find an element in the window that matches the search text."""
    try:
        window.set_focus()
        elements = window.descendants(control_type="Text")  # Get all text elements
        for element in elements:
            if search_text.lower() in element.window_text().lower():
                print(f"Found element with text: {element.window_text()}")
                return element
        print(f"No element found with text: {search_text}")
    except Exception as e:
        print(f"Error finding element: {e}")
    return None


def scroll_and_interact(window, element, search_text):
    """Scroll to an element and interact with it."""
    try:
        # Ensure the window is focused
        window.set_focus()

        # Simulate scrolling until the element is visible
        for _ in range(10):  # Adjust scroll attempts as needed
            element = find_element_by_text(window, search_text)
            if element:
                # Right-click on the found element
                element.right_click_input()
                send_keys("^c")  # Simulate Ctrl+C for copying
                return True
            else:
                # Scroll down if the element is not found
                send_keys("{PGDN}")  # Page Down
                time.sleep(1)
        print("Failed to locate the element after scrolling.")
    except Exception as e:
        print(f"Error interacting with element: {e}")
    return False


def capture_summary_data(window, search_text="Summary"):
    """Capture summary data by searching for a specific element."""
    try:
        # Find the desired element by text and scroll to it
        element = find_element_by_text(window, search_text)
        if not element:
            print(f"Scrolling to find the element with text '{search_text}'")
            if not scroll_and_interact(window, element, search_text):
                return

        # Fetch the copied content
        summary_text = os.popen("powershell -command Get-Clipboard").read()
        print("Summary Text copied:", summary_text)

        # Parse the pasted content into key-value pairs
        summary_dict = {}
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
        print("Summary Dict:", summary_dict)

        # Update data_dict and save to JSON
        start_id_time = datetime.now()
        data_dict.update(summary_dict)
        data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
        save_to_json(data_dict)
        global json_created
        json_created = True
        print("JSON created with Summary data.")
    except Exception as e:
        print(f"Error capturing summary data: {e}")


def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    """Monitor a specific window and capture data."""
    global json_created
    window = find_window_by_title(target_title)
    print(f"Tracking started for '{target_title}'")
    
    start_time = datetime.now()
    data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    # Capture data from the window
    capture_summary_data(window)


# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")



from pywinauto import Desktop, Application
from pywinauto.keyboard import send_keys
from datetime import datetime
import json
import os
import time

# Define paths for JSON output and other settings
process_folder = r"C:\pulse_event_trigger\process"
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def find_window_by_title(partial_title):
    """Find a window by its partial title."""
    while True:
        try:
            windows = Desktop(backend="uia").windows()
            for win in windows:
                if partial_title in win.window_text():
                    print(f"Found window: {win.window_text()}")
                    return win
        except Exception as e:
            print(f"Error finding window: {e}")
        time.sleep(1)


def get_window_coordinates(window):
    """Retrieve the coordinates of the given window."""
    rect = window.rectangle()
    coords = {
        "left": rect.left,
        "top": rect.top,
        "right": rect.right,
        "bottom": rect.bottom,
        "width": rect.width(),
        "height": rect.height(),
    }
    print(f"Window coordinates: {coords}")
    return coords


def capture_summary_data(window):
    """Capture summary data by simulating interactions."""
    try:
        # Right-click on the window and copy data
        window.set_focus()
        window.right_click_input()
        send_keys("^c")  # Simulate Ctrl+C for copying
        
        # Fetch the copied content
        summary_text = os.popen("powershell -command Get-Clipboard").read()
        print("Summary Text copied:", summary_text)

        # Parse the pasted content into key-value pairs
        summary_dict = {}
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
        print("Summary Dict:", summary_dict)

        # Update data_dict and save to JSON
        start_id_time = datetime.now()
        data_dict.update(summary_dict)
        data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
        save_to_json(data_dict)
        global json_created
        json_created = True
        print("JSON created with Summary data.")
    except Exception as e:
        print(f"Error capturing summary data: {e}")


def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    """Monitor a specific window and capture data."""
    global json_created
    window = find_window_by_title(target_title)
    print(f"Tracking started for '{target_title}'")
    
    start_time = datetime.now()
    data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    # Get window coordinates (if needed)
    coords = get_window_coordinates(window)

    # Capture data from the window
    capture_summary_data(window)


# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")





Overview of PyAutoGUI

PyAutoGUI is a Python library that automates interactions with the GUI. It allows developers to simulate mouse movements, keyboard presses, and screen interactions, including locating images or regions on the screen. It works by capturing screenshots of the screen and searching for visual matches.


---

Pros of PyAutoGUI in Your Situation

1. Ease of Use

PyAutoGUI has simple and intuitive functions (locateOnScreen, click, drag, etc.) for screen automation and locating images.



2. Cross-Platform Support

Works on Windows, macOS, and Linux, making it versatile for different environments.



3. Built-in Screenshot and Image Matching

PyAutoGUI directly integrates image recognition with locateOnScreen, making it ideal for scenarios where you need to identify elements visually.



4. Confidence Matching

Supports a confidence parameter for image matching (e.g., confidence=0.8), making it flexible for images with slight variations.



5. Good for On-Screen Matching

PyAutoGUI performs well when matching smaller images (e.g., partial image) on the screen directly, which is suitable for GUI-based tasks.



6. No Manual Coordinates

You don't need to manually define coordinates; it dynamically finds the image's location, which simplifies automation tasks.





---

Limitations of PyAutoGUI

1. Dependency on Screen Capture

PyAutoGUI relies on screen resolution and the pixel-perfect matching of images. If the captured image (e.g., via MSS) differs in scale, DPI, or compression, it may fail to match.



2. Performance

Matching large images or searching within high-resolution screens can be slow, as it scans pixel by pixel.



3. Limited to Static Images

It cannot handle dynamic or real-time visual changes effectively. For example, partial transparency or animations may cause matching to fail.



4. Accuracy Issues

Matching depends on exact pixel values. Even small variations in color, resolution, or scale can result in failure, especially when the source images are captured using different tools (e.g., MSS vs. Snipping Tool).



5. Lack of Advanced Image Processing

Unlike OpenCV, PyAutoGUI doesn’t support advanced image preprocessing (e.g., scaling, rotation, or feature-based matching), limiting its flexibility in handling mismatches.



6. Screen-Specific

PyAutoGUI is designed to work on live screens. Matching images within saved files or screenshots is less efficient and not its primary purpose.



7. Platform-Specific Constraints

On certain platforms or with multi-monitor setups, PyAutoGUI may face challenges capturing or processing images correctly.





---

When to Use PyAutoGUI in Your Scenario

Best Use Case:
If you're matching partial images directly on the live screen, PyAutoGUI is an excellent choice because it’s built for GUI-based automation tasks.

Not Recommended For:
If your process requires matching within static screenshots or handling images captured via different tools (e.g., MSS vs. Snipping Tool). In such cases, OpenCV or feature-based methods are more reliable.



---

How to Mitigate PyAutoGUI’s Limitations

1. Preprocess Images:
Save screenshots using the same tool (e.g., MSS) and ensure consistent resolution and DPI.


2. Adjust Confidence Threshold:
Use a lower confidence threshold (e.g., confidence=0.7) to account for minor differences in images.


3. Scale Matching:
Ensure the partial image’s scale matches the full image exactly by resizing if needed.


4. Switch to OpenCV or MSS for Advanced Matching:
If PyAutoGUI consistently fails, use OpenCV for precise and advanced matching, especially for static screenshots or secondary monitor scenarios.




---

By understanding PyAutoGUI's strengths and limitations, you can decide whether to stick with it for live screen-based tasks or opt for alternatives like OpenCV for more complex image processing requirements.

















def process_key_store(self, key_store_data, workflow_dict, keyname_store_set, keyname_mapping_set,
                      business_function_dict, delivery_function_dict, process_function_dict,
                      session, user_id, user_name, user_email):
    key_store_entries = key_store_data.to_dict(orient='records')
    key_store_request_id = None
    seen_keynames = set()
    serial_number = 1

    # Group entries by ProcessName, DeliveryService, BusinessLevel, and WorkflowName
    grouped_entries = {}
    for entry in key_store_entries:
        group_key = (
            entry['ProcessName'],
            entry['DeliveryService'],
            entry['BusinessLevel'],
            entry['WorkflowName']
        )
        grouped_entries.setdefault(group_key, []).append(entry)

    # Validate each group
    for group_key, entries in grouped_entries.items():
        is_unique_values = [entry['UniqueKey'] for entry in entries]
        yes_count = is_unique_values.count("Yes")
        no_count = is_unique_values.count("No")

        # Check if the group has exactly one "Yes" and not all "No"
        if yes_count != 1:
            return jsonify({
                'message': f'Group {group_key} must have exactly one "Yes" in UniqueKey field.'
            }), 400
        if no_count == len(is_unique_values):
            return jsonify({
                'message': f'Group {group_key} cannot have all entries as "No" in UniqueKey field.'
            }), 400

    # Process each entry after validation
    for entry in key_store_entries:
        workflow_name = entry['WorkflowName']
        workflow_id = workflow_dict.get(workflow_name)

        if workflow_id is None:
            return jsonify({'message': f'Workflow "{workflow_name}" does not exist in KEY_STORE sheet'}), 400

        key_name = entry['KeyName']
        if key_name in seen_keynames:
            return jsonify({'message': f'Duplicate KeyName "{key_name}" in KEY_STORE sheet'}), 400

        seen_keynames.add(key_name)

        if (workflow_id, key_name) in keyname_store_set or (workflow_id, key_name) in keyname_mapping_set:
            return jsonify({'message': f'Duplicate KeyName "{key_name}" for Workflow "{workflow_name}" in KEY_STORE'}), 400

        if not key_store_request_id:
            new_request = KeynameStoreRequests(
                count=len(key_store_entries),
                req_created_date=datetime.utcnow(),
                modified_date=datetime.utcnow(),
                created_by=user_id,
                creator_name=user_name,
                creator_email=user_email,
                is_active=True,
                status="open",
            )
            session.add(new_request)
            session.flush()
            key_store_request_id = new_request.request_id

        new_keyname_config = KeynameStoreConfigRequests(
            request_id=key_store_request_id,
            workflow_id=workflow_id,
            serial_number=serial_number,
            business_level_id=business_function_dict.get(entry['BusinessLevel']),
            delivery_service_id=delivery_function_dict.get(entry['DeliveryService']),
            process_name_id=process_function_dict.get(entry['ProcessName']),
            activity_key_name=key_name,
            activity_key_layout=entry['Layout'],
            is_unique=entry['UniqueKey'] == 'Yes',
            remarks=str(entry['Remarks']),
            is_active=True,
            status_ar='open'
        )
        session.add(new_keyname_config)
        serial_number += 1

    return None




import cv2
import pyautogui
from screeninfo import get_monitors

def get_screen_resolution():
    """
    Get the resolution of the primary screen using ScreenInfo.
    """
    for monitor in get_monitors():
        # Assuming the primary monitor
        print(f"Monitor: {monitor.name}, Resolution: {monitor.width}x{monitor.height}")
        return monitor.width, monitor.height

def perform_image_matching(fixed_image_path, fixed_resolution, threshold=0.8):
    """
    Perform image matching by scaling the fixed image to match the current screen resolution.
    
    Args:
        fixed_image_path: Path to the fixed image.
        fixed_resolution: Tuple of (width, height) for the fixed image's original resolution.
        threshold: Confidence threshold for template matching.

    Returns:
        The coordinates of the top-left corner of the matched area if found, else None.
    """
    # Get the current screen resolution
    current_width, current_height = get_screen_resolution()
    print(f"Current Screen Resolution: {current_width}x{current_height}")

    # Scaling factors
    original_width, original_height = fixed_resolution
    scale_width = current_width / original_width
    scale_height = current_height / original_height

    # Capture the current screen
    screenshot = pyautogui.screenshot()
    screenshot.save("screenshot.png")

    # Load images
    fixed_image = cv2.imread(fixed_image_path)
    screenshot_image = cv2.imread("screenshot.png")

    # Resize the fixed image to match the current resolution
    resized_fixed_image = cv2.resize(fixed_image, (0, 0), fx=scale_width, fy=scale_height)

    # Perform template matching
    result = cv2.matchTemplate(screenshot_image, resized_fixed_image, cv2.TM_CCOEFF_NORMED)

    # Get the best match position
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)

    print(f"Match confidence: {max_val}")
    if max_val >= threshold:
        print(f"Match found at coordinates: {max_loc}")
        # Highlight the matched area on the screenshot (optional)
        top_left = max_loc
        h, w = resized_fixed_image.shape[:2]
        bottom_right = (top_left[0] + w, top_left[1] + h)
        cv2.rectangle(screenshot_image, top_left, bottom_right, (0, 255, 0), 2)
        cv2.imwrite("output_match.png", screenshot_image)
        return top_left
    else:
        print("No match found.")
        return None

if __name__ == "__main__":
    # Path to the fixed image (1920x1080 resolution)
    fixed_image_path = "fixed_image.png"

    # Fixed resolution of the reference image
    fixed_resolution = (1920, 1080)

    # Perform the image matching
    match_coordinates = perform_image_matching(fixed_image_path, fixed_resolution)

    if match_coordinates:
        print(f"Element located at: {match_coordinates}")
    else:
        print("Element not found on the screen.")




import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os
from pynput.mouse import Listener

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info:", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles:", windows)
        
        for win in windows:
            if target_title in win:
                print("Checking with", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window is:", window)
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor['x'],
            "top": monitor['y'],
            "right": monitor['x'] + monitor['width'],
            "bottom": monitor['y'] + monitor['height']
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect['left'], monitor_rect['left'])
        overlap_top = max(window_rect['top'], monitor_rect['top'])
        overlap_right = min(window_rect['right'], monitor_rect['right'])
        overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")
        
        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None

def capture_screenshot(monitor_index, monitor_details):
    """Capture a screenshot of a specific monitor."""
    monitor = monitor_details[monitor_index]
    with mss.mss() as sct:
        screenshot = sct.grab({
            "top": monitor["y"],
            "left": monitor["x"],
            "width": monitor["width"],
            "height": monitor["height"]
        })
    return screenshot

def track_image(image_path, monitor_index, monitor_details):
    """Locate an image on a specific monitor."""
    screenshot = capture_screenshot(monitor_index, monitor_details)
    screenshot_path = os.path.join(process_folder, f"temp_monitor_{monitor_index}.png")
    mss.tools.to_png(screenshot.rgb, screenshot.size, output=screenshot_path)

    # Locate the image using PyAutoGUI
    location = pyautogui.locateOnScreen(image_path, confidence=0.9, region=(
        monitor_details[monitor_index]["x"],
        monitor_details[monitor_index]["y"],
        monitor_details[monitor_index]["width"],
        monitor_details[monitor_index]["height"]
    ))

    if location:
        x, y = pyautogui.center(location)
        print(f"Image {image_path} found at {x}, {y} on monitor {monitor_index}")
        return (x, y)
    else:
        print(f"Image {image_path} not found on monitor {monitor_index}")
        return None

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
    print("Summary Dict:", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict)
    global json_created
    json_created = True
    print("JSON created with Summary data.")

def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            location = track_image(summary_button_path, monitor_index, monitor_details)
            if location:
                capture_summary_data(location, start_time)
            time.sleep(1)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")





def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor['x'],
            "top": monitor['y'],
            "right": monitor['x'] + monitor['width'],
            "bottom": monitor['y'] + monitor['height']
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect['left'], monitor_rect['left'])
        overlap_top = max(window_rect['top'], monitor_rect['top'])
        overlap_right = min(window_rect['right'], monitor_rect['right'])
        overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")
        
        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None


----






import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os
from pynput.mouse import Listener

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info ", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles",windows)
        
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the window is open."""    
    # Loop through each monitor and find where the window is located
    for index, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
                monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index

    # If no monitor contains the window, default to the first monitor
    print("Window position did not match any monitor, defaulting to primary monitor.")
    return 0 if monitors else None

def track_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    else:
        return None, None

def monitor_clicks_and_track_end_button(end_button_path):
    """Monitor clicks and track the 'end button' when clicked."""
    print("Monitoring clicks to find the 'end button'.")
    end_button_location = None
    while True:
        # Start listening for a mouse click event
        with Listener(on_click=on_click) as listener:
            listener.join()  # Wait for click event to trigger

            # Once clicked, check for the end button location
            location_end, end_time = track_final_image(end_button_path, "end_button")
            if location_end:
                # Store end_button location details
                end_button_region = {
                    "left": location_end[0] - 10,
                    "top": location_end[1] - 10,
                    "right": location_end[2] + 10,
                    "bottom": location_end[3] + 10
                }
                print("End button detected, monitoring for clicks in region.")
                
                # Monitor clicks in the end button region
                monitor_clicks_in_region(end_button_region)
                break  # Exit after tracking the 'end button' event

        time.sleep(0.1)  # Allow for quick reaction to clicks

def track_final_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    print("Is location ?", location )
    if location:
        w, x, y, z = location.left, location.top, location.width, location.height 
        print(f"{action_name} located at {w}, {x}, {y}, {z}")
        return (w, x, y, z), datetime.now()
    else:
        return None, None

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value
            else:
                case_id = "dummy"
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")


def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            time.sleep(5)
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            # Track summary_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    # JSON creation and copying data once
                    capture_summary_data(location, start_id_time)


            # Start monitoring for end button after JSON creation
            if json_created:
                monitor_clicks_and_track_end_button(end_button_path)
                return  # Exit after tracking the 'end button' event
            
            time.sleep(1)  # Check every second


def monitor_clicks_in_region(region):
    """Monitor for clicks in a specific region and take a screenshot."""
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                
                # Capture full screen and save to process folder
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break  # Exit after capturing the screenshot
            time.sleep(0.1)  # Check clicks frequently

def on_click(x, y, button, pressed):
    """Callback function for mouse click events."""
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        return False  # Stop listener after a click (you can adjust behavior here)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"

    # Run the monitor process in a separate thread
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()



def monitor_clicks_and_track_end_button(end_button_path):
    """Monitor clicks and track the 'end button' when clicked."""
    print("Monitoring clicks to find the 'end button'.")
    while True:
        x, y = pyautogui.position()
        pyautogui.waitForClick()  # Wait for a click event
        
        # After a click, try to locate the 'end button'
        location_end, end_time = track_final_image(end_button_path, "end_button")
        if location_end:
            # Store end_button location details
            end_button_region = {
                "left": location_end[0] - 10,
                "top": location_end[1] - 10,
                "right": location_end[2] + 10,
                "bottom": location_end[3] + 10
            }
            print("End button detected, monitoring for clicks in region.")
            
            # Monitor clicks in the end button region
            monitor_clicks_in_region(end_button_region)
            break  # Exit after tracking the 'end button' event
        
        time.sleep(0.1)  # Allow for quick reaction to clicks

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
            # Track summary_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    # Capture and process summary data
                    capture_summary_data(location, start_id_time)
            
            # Start monitoring for end button after JSON creation
            if json_created:
                monitor_clicks_and_track_end_button(end_button_path)
                return  # Exit after tracking the 'end button' event
            
            time.sleep(1)  # Check every second

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value
            else:
                case_id = "dummy"
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")





import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    while True:
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    for index, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
                monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index
    return 0 if monitors else None

def track_image(image_path, action_name):
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    return None, None

def track_final_image(image_path, action_name):
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        w, x, y, z = location.left, location.top, location.width, location.height
        print(f"{action_name} located at {w}, {x}, {y}, {z}")
        return (w, x, y, z), datetime.now()
    return None, None

def save_to_json(data, case_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    x, y = location
                    curr_x, curr_y = pyautogui.position()
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.moveTo(x + 30, y + 30)
                    pyautogui.click()
                    summary_text = pyperclip.paste()
                    pyautogui.moveTo(curr_x, curr_y)
                    summary_dict = {}
                    lines = summary_text.split('\n')
                    for line in lines:
                        if line.strip():
                            parts = line.split('\t')
                            key = parts[0].strip() if len(parts) > 0 else ""
                            value = parts[1].strip() if len(parts) > 1 else ""
                            summary_dict[key] = value
                            if key == "Id":
                                case_id = value
                            else:
                                case_id = "dummy"
                    print("Summary Dict", summary_dict)
                    data_dict.update(summary_dict)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, case_id)
                    json_created = True
                    print("JSON created with Summary data.")

            if json_created:
                while True:
                    location_end, end_time = track_final_image(end_button_path, "end_button")
                    if location_end:
                        end_button_region = {
                            "left": location_end[0] - 10,
                            "top": location_end[1] - 10,
                            "right": location_end[2] + 10,
                            "bottom": location_end[3] + 10
                        }
                        print("End button detected, monitoring for clicks in region.")
                        monitor_clicks_in_region(end_button_region)
                        return
                    time.sleep(1)

            time.sleep(1)

def monitor_clicks_in_region(region):
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break
            time.sleep(0.1)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()




import screeninfo
import pygetwindow as gw

def get_monitor_index(window_title):
    # Get a list of all monitors
    monitors = screeninfo.get_monitors()
    # Try to find the specified window by title
    try:
        window = gw.getWindowsWithTitle(window_title)[0]
    except IndexError:
        print(f"Window with title '{window_title}' not found.")
        return None

    # Get the window's x, y position
    win_x, win_y = window.left, window.top
    
    # Loop through each monitor and find where the window is located
    for index, monitor in enumerate(monitors):
        if (monitor.x <= win_x < monitor.x + monitor.width and
                monitor.y <= win_y < monitor.y + monitor.height):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index

    # If no monitor contains the window, default to the first monitor
    print("Window position did not match any monitor, defaulting to primary monitor.")
    return 0 if monitors else None

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # Get X, Y from located position of the okay_button
                    x, y = location
                    
                    # Move to detected location and adjust to the appropriate offset for right-click
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()  # Right-click on the okay_button location
                    time.sleep(0.5)

                    # Move to "Copy" option in the context menu and click to copy text
                    pyautogui.moveTo(x + 40, y + 60)  # Adjust coordinates to hover on "Copy"
                    pyautogui.click()  # Click on "Copy" option
                    time.sleep(0.5)

                    # Paste the copied content
                    summary_text = pyperclip.paste()

                    # Parse the pasted content into key-value pairs
                    summary_dict = {}
                    lines = summary_text.split('\n')
                    for line in lines:
                        if line.strip():  # Ignore empty lines
                            parts = line.split('\t')  # Split by tab
                            key = parts[0].strip() if len(parts) > 0 else ""
                            value = parts[1].strip() if len(parts) > 1 else ""
                            summary_dict[key] = value
                    
                    # Update data_dict with parsed summary_dict
                    data_dict.update(summary_dict)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second




Given the updated requirements, here’s an approach to handle each step:

1. Detect the OK Button: When the okay_button image is first found, create a JSON file named with the specified format, and include details such as timestamp and extracted text. Skip creating the JSON file again if it already exists.


2. Monitor for the End Button: Once the okay_button is detected and JSON is created, start monitoring for end_button.


3. Handle Clicks in the End Button Region: If end_button is detected, track its coordinates (left, top, bottom, width). If any clicks happen within this region, immediately take a full screenshot and store it in the process folder.


4. Threading and Click Monitoring: Use threads for continuous monitoring and to ensure click events in the end_button region are detected for capturing screenshots.



Here’s how the updated code would look:

import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os

# Define paths for images and folders
okay_button_path = "path/to/okay_button.png"
end_button_path = "path/to/end_button.png"
process_folder = "path/to/process_folder"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the window is open."""
    for i, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
            monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            return i
    return None

def track_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    else:
        return None, None

def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # JSON creation and copying data once
                    x, y = location
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.hotkey('ctrl', 'c')
                    copied_text = pyperclip.paste()  # Copy extracted data

                    # Store data in dictionary
                    data_dict['copied_data'] = copied_text
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second

def monitor_clicks_in_region(region):
    """Monitor for clicks in a specific region and take a screenshot."""
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                
                # Capture full screen and save to process folder
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break  # Exit after capturing the screenshot
            time.sleep(0.1)  # Check clicks frequently

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"

    # Run the monitor process in a separate thread
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()

Explanation of Updates:

1. Flag for JSON Creation (json_created):

A flag json_created is set to False initially.

Once the okay_button is detected and JSON is created, json_created is set to True, so subsequent detections of okay_button will be ignored.



2. monitor_clicks_in_region Function:

Tracks clicks within the end_button region.

On detecting a click in the region, a full screenshot is taken and saved in the process_folder with a timestamped name.

After taking a screenshot, the function breaks out, ending the monitoring.



3. Modifications to monitor_process:

JSON creation is now triggered only on the first detection of okay_button.

Once the JSON is created, end_button tracking begins.

When end_button is detected, the system monitors clicks within its region and captures a screenshot on a detected click.




This code structure meets your requirements for efficient, click-based tracking of the end_button and conditions around JSON creation. Adjust file paths, image paths, and case_id as necessary for your specific setup.


____((((()))))___&&

Thank you for the clarification! Here’s how we can update the code to parse the copied text line-by-line, split it into key-value pairs, and store it as separate entries in the dictionary. This way, each line will be processed individually, with the first part as the key and the remaining part as the value.

Here's the updated monitor_process function with the necessary changes:

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # JSON creation and copying data once
                    x, y = location
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.hotkey('ctrl', 'c')
                    copied_text = pyperclip.paste()  # Copy extracted data

                    # Parse the copied text line by line into key-value pairs
                    parsed_data = {}
                    for line in copied_text.splitlines():
                        if line.strip():  # Ignore empty lines
                            key_value = line.split(maxsplit=1)  # Split into key and value based on first whitespace
                            if len(key_value) == 2:  # Ensure there is both a key and a value
                                key, value = key_value
                                parsed_data[key.strip()] = value.strip()

                    # Store parsed key-value pairs in data_dict
                    data_dict.update(parsed_data)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second

Explanation of Updates:

1. Parsing copied_text into Key-Value Pairs:

Each line of copied_text is processed individually by splitting it into key and value using line.split(maxsplit=1). This splits only at the first space, ensuring the key and value are separated correctly.

Only lines that contain both a key and a value are included in parsed_data.

Each key and value is stripped of extra whitespace for cleaner formatting.



2. Updating data_dict with Parsed Data:

After parsing, data_dict.update(parsed_data) is used to add the key-value pairs into data_dict.

This ensures that each key-value pair from the copied text is stored individually in the JSON.



3. Creating JSON with Parsed Data:

save_to_json(data_dict, case_id) is called to save the formatted data in JSON format, as previously.




This will result in a JSON file where each extracted line is stored as a separate key-value pair, as specified.




-----------------
import pyautogui
import time

# Move the mouse to the top-left corner of the first monitor (laptop screen)
pyautogui.moveTo(100, 100)
print("Moved mouse to first monitor")

# Wait for 2 seconds to observe the mouse
time.sleep(2)

# Move the mouse to the top-left corner of the second monitor (TV)
# Adjust the coordinates based on your setup (e.g., if your TV is to the right, x=1920 for 1080p laptop)
pyautogui.moveTo(1920 + 100, 100)
print("Moved mouse to second monitor (TV)")

# Wait for 2 seconds to observe the mouse
time.sleep(2)

# Perform a click action on the second monitor
pyautogui.click(1920 + 100, 100)  # Adjust based on the screen resolution and location
print("Clicked on second monitor")






To handle exiting the mouse.Listener loop based on a flag, you can use a global variable that gets updated when the flag condition is met. This flag can be checked inside the on_click function, and if the condition is true, it will stop the listener.

To implement this, modify the on_click function to check the flag after each click. If the flag indicates that the program should stop, you can call listener.stop() to exit the loop.

Here's how you can modify the code:

import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "test_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click and the button region
awaiting_final_ok_click = False
ok_button_region = None  # Store button's bounding box
exit_flag = False  # Flag to indicate when to stop the listener

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    global ok_button_region
    try:
        print("Checking for button visibility...")
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.9)
        if location:
            print(f"Button found at {location.left}, {location.top}, {location.width}, {location.height}")
            # Store the bounding box as (left, top, width, height)
            ok_button_region = location
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click, ok_button_region, exit_flag

    # Only proceed on button release
    if not pressed:
        print(f"Mouse clicked at ({x}, {y})")
        if awaiting_final_ok_click:
            # Check if the OK button is visible and a region is defined
            if ok_button_region:
                # Check if click is within the button region
                left, top, width, height = ok_button_region.left, ok_button_region.top, ok_button_region.width, ok_button_region.height
                if left <= x <= left + width and top <= y <= top + height:
                    print("Click is within OK button region. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
                    exit_flag = True  # Set exit flag to stop the listener
        else:
            # Update the OK button location and enable the awaiting click state
            if check_button_visible("ok"):
                awaiting_final_ok_click = True

        # Stop the listener if the exit flag is set
        if exit_flag:
            return False

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")

Explanation of Changes:

1. exit_flag Variable: This variable is set to True when the condition to stop the listener is met.


2. Condition to Stop Listener: In the on_click function, if exit_flag is True, the listener stops by returning False, which breaks out of the listener.join() loop.


3. Flag Update: After capturing a screenshot, exit_flag is set to True, stopping the listener after the next click.



Now, the code will automatically stop the listener when the exit_flag is set to True, allowing you to exit the loop when the desired condition is met.






Here is a modified version of your code that incorporates the functionality you've described:

Locate Image: When a user clicks, the program will locate the image on the screen.

Move the Mouse Below the Found Button: It will move slightly below the image (button) found.

Right-Click: The program will perform a right-click.

Copy: After right-clicking, the program will copy the content.

Store the Summary in JSON: It will store the summary in a JSON file with the timestamp when the image was found.

Return to Previous Position: After completing the actions, it will return the cursor to its original position.


Modified Code:

import time
import os
import pyautogui
import pyperclip
import json
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"
SUMMARY_JSON_PATH = r"C:\pulse_event_trigger\summary_data.json"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "test_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

# Function to capture screenshot
def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

# Function to check if a button is visible and take an action
def check_button_visible(button_name, curr_x, curr_y):
    try:
        print("Searching for button...")
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.9)
        
        if location:
            print(f"Button found at location: {location.left}, {location.top}, {location.width}, {location.height}")
            
            # Capture screenshot of the area
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_check_{timestamp}.png")
            pyautogui.screenshot(screenshot_filename, region=(location.left, location.top, location.width, location.height))
            print(f"Button screenshot captured and saved as {screenshot_filename}")
            
            # Move the cursor below the button and perform right click
            pyautogui.moveTo(location.left + location.width // 2, location.top + location.height + 5)
            pyautogui.rightClick()
            print("Right-click done")

            # Wait for a moment and then perform the copy action
            time.sleep(0.5)
            pyautogui.hotkey('ctrl', 'c')
            print("Content copied")

            # Get the copied text from clipboard
            summary_text = pyperclip.paste()
            print("Summary Text:", summary_text)

            # Create JSON data for storing summary
            summary_dict = {}
            lines = summary_text.split('\n')
            for line in lines:
                parts = line.split('\t')
                if len(parts) == 2:
                    key = parts[0]
                    value = parts[1].replace('\r', '')  # Clean up the value if needed
                    summary_dict[key] = value

            # Add timestamp to the summary data
            summary_dict["timestamp"] = timestamp

            # Save to a JSON file
            if os.path.exists(SUMMARY_JSON_PATH):
                with open(SUMMARY_JSON_PATH, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = {}

            # Append new summary data
            existing_data[timestamp] = summary_dict

            with open(SUMMARY_JSON_PATH, 'w') as f:
                json.dump(existing_data, f, indent=4)

            print(f"Summary saved to {SUMMARY_JSON_PATH}")
            
            return True

        else:
            print(f"Button '{button_name}' not found on the screen.")
            return False

    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return False

# Function to track mouse clicks
def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    print(f"Mouse clicked at ({x}, {y}) with {button}")

    # Only proceed on button release
    if not pressed:
        # If we're in the final "OK" step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok", x, y)
            if ok_button_location:
                # After the button is clicked and actions are completed, reset the state
                awaiting_final_ok_click = False
        else:
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring mouse clicks
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")

Key Changes:

1. check_button_visible Function:

This function now:

Locates the image on the screen.

Takes a screenshot of the image location.

Moves the mouse slightly below the found image (button) and performs a right-click.

Copies the content after the right-click and stores it in the clipboard.

Parses the copied content into a dictionary and saves it into a JSON file with a timestamp.




2. JSON Handling:

If the JSON file already exists, it loads the existing data and appends the new summary with the timestamp.

If it doesn't exist, a new file is created.



3. Timestamp in JSON:

The timestamp is added to each summary dictionary to track when the summary was captured.



4. Return to Previous Position:

You could modify this behavior if needed to return the cursor to the original position by saving the current position before the action and restoring it afterward.




Functionality Flow:

1. Locate Image: When the user clicks on the screen, the image is located and processed.


2. Perform Right-Click: If the image is found, the mouse is moved below the image and a right-click is performed.


3. Copy and Parse Content: The content copied to the clipboard is parsed and saved in a JSON file along with the timestamp.


4. Return to Position: The cursor can be returned to the original position if needed, and the summary data is saved for later use.



This approach ensures that the user can continue working without any noticeable delay, as the operations happen in milliseconds. The timestamp will also help you track when the summary was captured.




--------------------

import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "ok_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    try:
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    # Only proceed on button release
    if not pressed:
        # If we're in the final OK step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location:
                if ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width and \
                   ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                    print("OK button clicked. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
        else:
            # Placeholder for additional logic to handle other buttons
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")






import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "ok_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    try:
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    # Only proceed on button release
    if not pressed:
        # If we're in the final OK step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location:
                if ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width and \
                   ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                    print("OK button clicked. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
        else:
            # Placeholder for additional logic to handle other buttons
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
with mouse.Listener(on_click=on_click) as listener:
    print("Monitoring started...")
    listener.join()




Your code is well-structured, but there are a few areas that could be refined to ensure smooth functioning, especially around detecting the "OK" button and handling errors. I’ve adjusted a few parts to enhance reliability and address the GetIsInvokedPattern() method, which isn’t directly available in uiautomation. I also refined the find_ok_button method to streamline the button search process and logging.

Here’s the modified version of your code:

import uiautomation as auto
import win32gui
import win32con
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "c:\\Pulserimage"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window title matches Claude's window"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}\\claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find the OK button by name within the Claude window"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Search for button by exact match on button name
            button = window.ButtonControl(Name=self.button_name)
            if button.Exists(0, 0):
                return button
            
            # Alternative search if exact match not found
            for control in window.GetChildren():
                if isinstance(control, auto.ButtonControl) and \
                   self.button_name.lower() in control.Name.lower():
                    return control
            
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_click_time = 0
        click_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists(0, 0):
                            try:
                                # Check if button was clicked
                                current_time = time.time()
                                
                                if current_time - last_click_time > click_cooldown:
                                    logging.info("OK button detected - Capturing screenshot")
                                    self.capture_screenshot(active_window)
                                    last_click_time = current_time
                            
                            except Exception as e:
                                logging.error(f"Button click detection error: {e}")
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")

Explanation of Modifications

1. Simplified find_ok_button:

Now it first tries a direct search for a button with the name self.button_name. If it doesn’t find an exact match, it searches through children controls.



2. Removed GetIsInvokedPattern:

As this is not directly available or necessary here, we rely on Exists() to check if the button is present, and we capture the screenshot if it’s detected within the Claude window.



3. Added Error Handling and Logging:

Improved logging for button detection and screenshot functions to help diagnose issues during execution.




Required Packages

Pillow: for ImageGrab, installed with pip install Pillow.

pywin32: for win32gui and win32con, installed with pip install pywin32.

uiautomation: for UI control handling, installed with pip install uiautomation.


This code will continuously monitor for the "OK" button in the specified window, and if it appears and is active, it will take a screenshot and save it with a timestamp. This way, it should meet the requirements without unnecessary complexity.





import uiautomation as auto
import win32gui
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "claude_screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        # Store last button state
        self.last_button_state = None
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window is Claude"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find OK button using multiple methods"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Method 1: Direct search
            try:
                button = window.ButtonControl(Name=self.button_name)
                if button.Exists():
                    return button
            except:
                pass
            
            # Method 2: Search all descendants
            try:
                buttons = window.GetChildren()
                for control in buttons:
                    try:
                        if isinstance(control, auto.ButtonControl) and \
                           self.button_name.lower() in control.Name.lower():
                            return control
                    except:
                        continue
            except:
                pass
            
            # Method 3: Deep search
            try:
                buttons = window.FindAll(auto.ButtonControl)
                for button in buttons:
                    try:
                        if self.button_name.lower() in button.Name.lower():
                            return button
                    except:
                        continue
            except:
                pass
                
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def is_button_clicked(self, button):
        """Check if button was clicked using multiple methods"""
        try:
            # Method 1: Check invoke pattern
            try:
                invoke_pattern = button.GetInvokePattern()
                if invoke_pattern:
                    current_state = button.CurrentIsInvoked
                    if current_state != self.last_button_state:
                        self.last_button_state = current_state
                        return current_state
            except:
                pass

            # Method 2: Check selection pattern
            try:
                selection_pattern = button.GetSelectionPattern()
                if selection_pattern:
                    return selection_pattern.IsSelected
            except:
                pass

            # Method 3: Check toggle pattern
            try:
                toggle_pattern = button.GetTogglePattern()
                if toggle_pattern:
                    return toggle_pattern.ToggleState == auto.ToggleState.On
            except:
                pass

            # Method 4: Check button state
            try:
                current_state = button.GetLegacyIAccessiblePattern().CurrentState
                was_clicked = bool(current_state & auto.State.Pressed)
                return was_clicked
            except:
                pass

            return False

        except Exception as e:
            logging.error(f"Button click check error: {e}")
            return False

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_capture_time = 0
        capture_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists():
                            # Check if button was clicked
                            current_time = time.time()
                            
                            if self.is_button_clicked(ok_button):
                                # Prevent duplicate captures
                                if current_time - last_capture_time > capture_cooldown:
                                    print("OK button clicked - Capturing screenshot")
                                    logging.info("OK button clicked - Capturing screenshot")
                                    self.capture_screenshot(active_window)
                                    last_capture_time = current_time
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")





import uiautomation as auto
import win32gui
import win32con
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "claude_screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window is Claude"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find OK button using multiple methods"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Method 1: Direct search for OK button
            try:
                button = window.ButtonControl(Name=self.button_name)
                if button.Exists():
                    return button
            except:
                pass
            
            # Method 2: Search all buttons
            try:
                all_buttons = window.GetChildren()
                for control in all_buttons:
                    try:
                        if isinstance(control, auto.ButtonControl) and \
                           self.button_name.lower() in control.Name.lower():
                            return control
                    except:
                        continue
            except:
                pass
            
            # Method 3: Deep search
            try:
                buttons = window.FindAllControl(auto.ButtonControl)
                for button in buttons:
                    try:
                        if self.button_name.lower() in button.Name.lower():
                            return button
                    except:
                        continue
            except:
                pass
                
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_click_time = 0
        click_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists():
                            try:
                                # Check if button was clicked
                                current_time = time.time()
                                
                                if ok_button.GetIsInvokedPattern():
                                    # Prevent duplicate captures
                                    if current_time - last_click_time > click_cooldown:
                                        logging.info("OK button clicked - Capturing screenshot")
                                        self.capture_screenshot(active_window)
                                        last_click_time = current_time
                            except:
                                pass
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")




-------------------

import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}

    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def find_buttons_in_window(self, window_control):
        """
        Recursively find all buttons in a window
        """
        buttons = []
        try:
            # Check if current control is a button
            if isinstance(window_control, auto.ButtonControl):
                buttons.append(window_control)
            
            # Get all children of the current control
            children = window_control.GetChildren()
            for child in children:
                # Recursively search for buttons in children
                buttons.extend(self.find_buttons_in_window(child))
                
        except Exception as e:
            logging.error(f"Error finding buttons: {e}")
        
        return buttons

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            # Get the window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                try:
                    button = window.ButtonControl(AutomationId=button_config['automation_id'])
                    if button.Exists():
                        return button
                except:
                    pass
            
            # 2. Try by Name and Class
            if button_config['class_name']:
                try:
                    button = window.ButtonControl(
                        Name=button_config['button_name'],
                        ClassName=button_config['class_name']
                    )
                    if button.Exists():
                        return button
                except:
                    pass
            
            # 3. Search all buttons and match by name
            all_buttons = self.find_buttons_in_window(window)
            for btn in all_buttons:
                try:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        return btn
                except:
                    continue
            
            return None
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button:
                            try:
                                # Check if button is clickable and was clicked
                                if button.IsEnabled:
                                    # You might need to adjust this condition based on your specific applications
                                    # Some applications might require different methods to detect if a button was clicked
                                    button_state = button.GetTogglePattern() if hasattr(button, 'GetTogglePattern') else None
                                    if button_state and button_state.ToggleState == auto.ToggleState.On:
                                        logging.info(f"Endpoint triggered for {app_name}")
                                        screenshot_path = self.capture_screenshot(app_name, active_window)
                                        if screenshot_path:
                                            logging.info(f"Screenshot saved: {screenshot_path}")
                                            # Add your trigger code here
                                            pass
                            except Exception as e:
                                logging.error(f"Error checking button state: {e}")
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()







-----2--2--2-2--2-2


import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()







 Let me break down the solution and explain its limitations and reliability.

The current approach has several potential issues that could affect reliability. Let me explain the core components and suggest a more robust approach.



<antArtifact identifier="robust-application-monitor" type="application/vnd.ant.code" language="python" title="Robust Application Monitor with UI Automation">
import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()
</antArtifact>

Let me explain why the previous approach wasn't 100% reliable and how this new version is more robust:

1. Key Improvements:

- Uses UI Automation instead of mouse position tracking
- Multiple button detection methods
- Window-specific screenshot capture
- Improved error handling and logging
- Caching for better performance
- More reliable event detection

2. Reliability Factors:

The previous version had these issues:
- Relied on mouse position (unreliable)
- Couldn't detect buttons accurately
- No fallback methods
- Vulnerable to window changes

The new version is more reliable because:

a) Button Detection:
   - Uses Windows UI Automation framework
   - Multiple search methods (Automation ID, Class Name, Name)
   - Doesn't rely on screen coordinates
   - Can detect actual UI elements

b) Window Handling:
   - Proper window detection
   - Caches window handles
   - Handles window state changes
   - Window-specific screenshots

c) Error Prevention:
   - Multiple fallback methods
   - Extensive error handling
   - Detailed logging
   - Resource cleanup

3. Limitations (Still Not 100%):

Even this improved version has some limitations:

- Some applications might block UI Automation
- Custom UI frameworks might not be fully compatible
- Window titles might change
- System performance could affect timing
- Some applications might use non-standard controls

4. To Make It More Reliable:

You should:

1. Add application-specific details:
```python
"OWS": {
    "window_pattern": "OWS",
    "endpoint": {
        "button_name": "OK",
        "automation_id": "okButton",  # Add actual ID
        "class_name": "Button"        # Add actual class
    }
}
```

2. Test with your specific applications and add:
   - Exact window titles
   - Button automation IDs
   - Class names
   - Any specific patterns

3. Add additional error handling for your environment

To use this version:

1. Install requirements:
```python
pip install uiautomation pillow pywin32
```

2. Run the script:
```python
python robust_monitor.py
```

Would you like me to:
1. Add support for specific applications you're using?
2. Implement additional detection methods?
3. Add specific error handling for your environment?
4. Add custom triggers for when endpoints are detected?




import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()






merhod-1 :

In this case, since the endpoint (like the "OK" button) doesn't exist in the window title, we'll need a way to detect specific buttons on the screen by using image recognition to look for UI elements. Here’s how we can achieve this with pyautogui to recognize on-screen buttons and trigger the screenshot capture when the final button (like "OK") is clicked.

Approach Outline

1. Image Recognition for Button Detection: Use pyautogui.locateOnScreen() to search for specific buttons on the screen by using screenshots of each button type as reference images. For example, take screenshots of the "Next," "Clear," and "OK" buttons and save them as images like next_button.png, clear_button.png, and ok_button.png.


2. Click Monitoring: Set up a listener with pynput to monitor each mouse click and use pyautogui.locateOnScreen() to check if the "OK" button is visible. When it's clicked, trigger the screenshot capture.


3. Trigger Screenshot on "OK" Click: Only take a screenshot when the "OK" button is clicked. We can determine this by using image matching right after a mouse click to check if the click position aligns with the "OK" button.



Here's an example of how this could be implemented in Python:

import time
import pyautogui
from pynput import mouse
from datetime import datetime

# Define button images for reference (these images should be screenshots of your buttons)
BUTTON_IMAGES = {
    "next": "next_button.png",   # Image of the "Next" button
    "clear": "clear_button.png", # Image of the "Clear" button
    "ok": "ok_button.png"        # Image of the "OK" button
}

# This flag helps track if we are in the final step (where "OK" appears)
awaiting_final_ok_click = False

def capture_screenshot():
    # Capture and save screenshot with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = f"screenshot_{timestamp}.png"
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    # Use pyautogui to search for the button on screen
    location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
    return location

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click
    
    # Only process on button release
    if not pressed:
        # Check for "OK" button visibility and click position
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location and ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width \
                    and ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                print("OK button clicked. Capturing screenshot.")
                capture_screenshot()
                awaiting_final_ok_click = False  # Reset the state after capturing screenshot
        else:
            # Check if "Next" button or other buttons are clicked, progressing to the final step
            next_button_location = check_button_visible("next")
            if next_button_location:
                print("Next button detected and clicked. Progressing workflow.")
                # Move forward in workflow, expecting OK button soon
                awaiting_final_ok_click = True

# Set up the mouse listener
with mouse.Listener(on_click=on_click) as listener:
    print("Monitoring started...")
    listener.join()

Explanation

1. Button Image Matching:

check_button_visible() checks if a specific button is on the screen by using an image of the button (e.g., next_button.png, clear_button.png, ok_button.png).

If locateOnScreen() finds the button with sufficient confidence, it returns the position of that button on the screen.



2. Mouse Click Monitoring:

In on_click, we first check if the "Next" or other intermediate buttons are visible and clicked.

If they are, we set awaiting_final_ok_click = True, indicating we’re ready for the "OK" button in the workflow.

When awaiting_final_ok_click is True, we listen for the "OK" button to be clicked at the specified location.



3. Screenshot Trigger on Final Click:

When awaiting_final_ok_click is True, the script will monitor for an "OK" button click.

Once detected, capture_screenshot() is called to save the current screen image.




Additional Notes

Button Images: Ensure the images of each button are clear and cropped tightly to the button's dimensions for accurate detection.

Confidence Parameter: The confidence parameter in locateOnScreen() allows for a match tolerance, helping with slight visual variations in buttons. Experiment with values like 0.8 or 0.9.

Error Handling: You might need to add error handling for cases where the buttons aren’t detected due to changes in UI.


This setup should help you track the click sequence, detect the final click on the "OK" button, and take a screenshot accordingly.




method-2: 


Given the need to monitor all types of applications, including both desktop and web, and to detect when a button click matches specific conditions (like the button being labeled "OK"), here’s a refined approach that combines accessibility APIs and click monitoring to address your requirements:

Approach Overview

1. Monitor Global Click Events: Use a global hook to detect mouse clicks.


2. Identify UI Elements at Click Locations: For each click, detect if it’s on a button or other UI control with specific attributes (such as “OK” or “Submit”) using accessibility APIs.


3. Trigger Screenshot on Matching Clicks: If the click matches the target endpoint (e.g., button labeled "OK"), trigger the screenshot capture.



Implementation Steps

1. Monitor Global Mouse Clicks

To globally monitor mouse clicks across applications, you can use libraries like pynput to set up a global mouse listener in Python. This listener will intercept all mouse events, regardless of the active application.

from pynput import mouse
import time

# Callback function for mouse clicks
def on_click(x, y, button, pressed):
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        # Call function to check if this click is on the desired button
        check_ui_element(x, y)

# Set up a global listener
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

This code captures each click's location (x, y coordinates). For every click, it calls check_ui_element(x, y) to inspect the UI element under the cursor.

2. Identify the Clicked UI Element

Using libraries like pywinauto or uiautomation, you can retrieve information about the UI element at the clicked position. Since pywinauto can inspect accessibility information for most applications, it’s helpful in identifying UI elements on both Windows and some web applications running in a browser.

Here’s an example using uiautomation to check if the click location has a button with the desired label:

from uiautomation import Control

def check_ui_element(x, y):
    # Find the control at the given screen coordinates
    control = Control(x=x, y=y)
    
    # Check if the control is a button and matches the desired title
    if control.ControlTypeName == 'Button' and control.Name in ['OK', 'Submit', 'Done']:
        print(f"Button '{control.Name}' clicked at ({x}, {y})")
        capture_screenshot()
    else:
        print("Click did not match the target button.")

This function:

Finds the control at the coordinates of the mouse click.

Checks the control type and name to see if it’s a button and if its name matches the desired endpoint (OK, Submit, Done).

Triggers a screenshot if the control meets these conditions.


3. Capture a Screenshot

To capture a screenshot, you can use libraries like Pillow. You’d call this function within check_ui_element() if the click matches your endpoint.

from PIL import ImageGrab
import datetime

def capture_screenshot():
    # Generate a timestamped filename
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

This code captures the entire screen and saves it with a timestamped filename when called.

Putting It All Together

Here’s a consolidated example combining all the pieces:

from pynput import mouse
from uiautomation import Control
from PIL import ImageGrab
import datetime

# Define a list of target button names
TARGET_BUTTON_NAMES = ['OK', 'Submit', 'Done']

# Function to capture a screenshot
def capture_screenshot():
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

# Function to check if the clicked element matches the target endpoint
def check_ui_element(x, y):
    control = Control(x=x, y=y)
    if control.ControlTypeName == 'Button' and control.Name in TARGET_BUTTON_NAMES:
        print(f"Button '{control.Name}' clicked at ({x}, {y})")
        capture_screenshot()
    else:
        print("Click did not match the target button.")

# Mouse click event handler
def on_click(x, y, button, pressed):
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        check_ui_element(x, y)

# Set up global mouse listener
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

Explanation of How It Works

1. Global Mouse Listener: The mouse.Listener detects all mouse clicks globally.


2. Element Inspection: For each click, check_ui_element(x, y) checks if the clicked UI element matches the target button criteria.


3. Screenshot Trigger: If a target button is clicked, capture_screenshot() is called to capture and save the screen.



Advantages of This Method

Versatile Application: Works across different types of applications since it relies on coordinates and UI Automation APIs.

Efficient Detection: Only triggers a screenshot for relevant button clicks, without constantly searching for button images.

Scalability: Easily adaptable if you need to monitor other UI elements or applications with similar button endpoints.


This approach avoids relying on visual matching and instead leverages UI properties, making it more robust for different types of applications.



method-3:

To streamline this process and avoid frequent image captures, you can use a more direct approach that combines system hooks and lightweight UI element checks. By using pyautogui to track specific clicks and uiautomation to verify elements only when a click occurs, you can avoid excessive image processing and reduce processing time. Here’s an optimized solution that focuses on capturing a screenshot only when the user clicks on the desired endpoint button (e.g., “OK”).

Optimized Solution Outline

1. Initial and Final Image Capture Only: Capture an initial image when the target window opens and a final image when the workflow endpoint is clicked (e.g., the “OK” button).


2. Direct Click Monitoring with pyautogui: Track clicks and verify the active window title to ensure you only check for clicks within relevant applications.


3. UI Automation for Click Validation: Instead of image processing for each click, use uiautomation to directly validate whether a click occurred on a specific button (like “OK”) within the target window.



Implementation Steps

This example captures a screenshot only when the “OK” button is clicked, marking the end of the workflow.

from pynput import mouse
import pyautogui
import time
import datetime
from PIL import ImageGrab
import uiautomation as automation
from ctypes import windll
import ctypes

# Target application and button configurations
TARGET_APPS = ["OWS", "AWS", "JWS"]
TARGET_BUTTON_NAMES = ["OK", "Submit", "Done"]

# Track start and end times
start_time = None

# Helper to get the current active window title
def get_active_window_title():
    hwnd = windll.user32.GetForegroundWindow()
    length = windll.user32.GetWindowTextLengthW(hwnd)
    buffer = ctypes.create_unicode_buffer(length + 1)
    windll.user32.GetWindowTextW(hwnd, buffer, length + 1)
    return buffer.value

# Capture screenshot function
def capture_screenshot():
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

# Function to verify button click
def check_if_button_clicked(x, y):
    control = automation.Control(x=x, y=y)
    if control.ControlTypeName == 'Button' and control.Name in TARGET_BUTTON_NAMES:
        return control.Name
    return None

# Event handler for mouse clicks
def on_click(x, y, button, pressed):
    global start_time
    if pressed:
        active_window = get_active_window_title()

        # Check if active window is one of the target applications
        if any(app in active_window for app in TARGET_APPS):
            # Track start time on first detected click in target app
            if start_time is None:
                start_time = datetime.datetime.now()
                print("Start time recorded:", start_time)

            # Check if the clicked element is a target button
            clicked_button = check_if_button_clicked(x, y)
            if clicked_button:
                print(f"'{clicked_button}' button clicked at ({x}, {y})")

                # Capture end time and screenshot if the endpoint is clicked
                if clicked_button in TARGET_BUTTON_NAMES:
                    end_time = datetime.datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"End of workflow detected. Duration: {duration} seconds.")
                    capture_screenshot()  # Capture final screenshot
                    start_time = None  # Reset for next workflow

# Start listening for mouse events
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

Explanation of the Optimized Solution

1. Window Title Check: Each click event first checks the active window title to ensure clicks are within the target application. This avoids unnecessary checks outside the relevant applications.


2. Direct Click Validation: When a click is detected, the code uses uiautomation to verify if it’s on a button matching the endpoint criteria (like "OK"). This avoids image comparisons, reducing processing time to milliseconds.


3. Minimal Screenshot Capture: Screenshots are only taken at the start (optional) and end of the workflow, ensuring no redundant image captures during the workflow.


4. Timing Control: The start_time and end_time are recorded only when the relevant window is active, and the workflow endpoint button is clicked, allowing for precise tracking of workflow duration.



Performance Considerations

Efficiency: By reducing image captures and relying on UI element checks only when needed, this method avoids the delays associated with frequent image processing.

Accuracy: Using uiautomation ensures that you’re validating clicks on actual UI elements (like buttons) rather than relying on pixel-based image matching.

Responsiveness: This approach minimizes overhead, so the response time should be close to milliseconds for each click check.


This setup should meet the requirement for fast, efficient tracking without the heavy processing of frequent image analysis.




Yes, you can print the UI tree using the uiautomation library, which allows you to visualize the hierarchy and properties of accessible UI elements. This can be very useful for understanding the structure of the application's UI and identifying the elements available for interaction.

Here’s how you can do it:

1. Install uiautomation (if you haven’t already):

pip install uiautomation


2. Use the automation.GetRootControl() Method to get the root of the UI tree, then recursively print the tree. This will print the properties of each accessible control, including control type and name.



Here’s an example code snippet that captures the UI tree and prints it:

import uiautomation as automation

def print_ui_tree(control, depth=0):
    """
    Recursively print the UI tree starting from the given control.
    """
    # Print the current control's information with indentation based on depth
    print("  " * depth + f"ControlType: {control.ControlTypeName}, Name: {control.Name}, Class: {control.ClassName}")

    # Recursively print child controls
    for child in control.GetChildren():
        print_ui_tree(child, depth + 1)

# Capture the root control (entire screen)
root_control = automation.GetRootControl()

# Print the UI tree from the root control
print_ui_tree(root_control)

Explanation

Root Control: automation.GetRootControl() gets the root control of the entire desktop UI. You can also use automation.GetFocusedControl() if you want to start from the currently active window.

Recursive Printing: The print_ui_tree function goes through each control's children and prints details, adjusting indentation (depth) for easier visualization.


Adjustments and Usage Tips

1. Limiting Depth: If the UI tree is very large, you might want to limit the depth to avoid printing too many levels.


2. Filtering Controls: You can add conditions within print_ui_tree to filter specific control types, names, or classes.


3. Focus on a Specific Window: If you only want to print the tree for a specific window, use automation.WindowControl(Name="Window Title") in place of GetRootControl().



Example Output

The output will look something like this:

ControlType: Window, Name: Untitled - Notepad, Class: Notepad
  ControlType: MenuBar, Name: , Class: 
    ControlType: MenuItem, Name: File, Class: 
    ControlType: MenuItem, Name: Edit, Class: 
  ControlType: Edit, Name: Text Editor, Class: 
  ControlType: StatusBar, Name: , Class:

This output helps you understand the hierarchy and properties of each control, which you can then use to identify and interact with specific elements based on ControlType, Name, or ClassName.




method-5:

Sure, here’s an example code snippet that follows this approach using pyautogui to capture a small area around the mouse click and pytesseract for OCR to detect if a particular button (e.g., an “OK” button) is in that region.

This code assumes you’re checking for a specific word, like “OK,” on the button to determine if it’s your endpoint. If the word is detected, it will then trigger a full-screen capture.

Code Example

import pyautogui
import pytesseract
from PIL import Image
import time

# Configure pytesseract path if needed
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def capture_and_check_button(click_x, click_y, region_size=(100, 50)):
    """
    Captures a small region around the click coordinates and checks for the target text.
    :param click_x: X-coordinate of the click.
    :param click_y: Y-coordinate of the click.
    :param region_size: Size of the region to capture around the click (width, height).
    :return: True if the target text is found, False otherwise.
    """
    # Define the region to capture around the click (x, y, width, height)
    region = (click_x - region_size[0] // 2, click_y - region_size[1] // 2, region_size[0], region_size[1])
    
    # Capture the region
    small_capture = pyautogui.screenshot(region=region)
    
    # Use OCR to read text from the captured region
    text = pytesseract.image_to_string(small_capture)
    print(f"OCR Text Detected: {text.strip()}")
    
    # Check if the target text (e.g., "OK") is present in the text
    return "OK" in text.strip()

def monitor_clicks(region_size=(100, 50)):
    """
    Monitors mouse clicks and captures the full screen if the target button is clicked.
    :param region_size: Size of the region to capture around each click.
    """
    print("Monitoring clicks... Press Ctrl+C to stop.")
    
    try:
        while True:
            # Wait for a mouse click
            if pyautogui.mouseDown(button="left"):
                # Get mouse position
                click_x, click_y = pyautogui.position()
                
                # Check if the small capture contains the endpoint button (e.g., "OK")
                if capture_and_check_button(click_x, click_y, region_size):
                    print("Endpoint button clicked! Capturing the full screen...")
                    
                    # Capture full screen if the endpoint button was clicked
                    full_screenshot = pyautogui.screenshot()
                    full_screenshot.save("full_screenshot.png")
                    print("Full screen captured.")
                
                # Allow a small delay to avoid multiple triggers from a single click
                time.sleep(0.5)
                
    except KeyboardInterrupt:
        print("Click monitoring stopped.")

# Start monitoring clicks
monitor_clicks()

Explanation

1. capture_and_check_button():

This function captures a small region around the click location (specified by click_x, click_y).

It uses OCR to read any text in the captured image.

If the OCR detects the word “OK” in the text, it returns True to indicate that the endpoint button is clicked.



2. monitor_clicks():

Continuously monitors for left mouse clicks.

For each click, it retrieves the mouse position and passes it to capture_and_check_button() to check for the endpoint button.

If capture_and_check_button() returns True, it captures the full screen and saves it as “full_screenshot.png”.




How to Adjust for Performance

Adjust region_size: Try different values to find the smallest region that reliably detects your button.

OCR Optimization: Since pytesseract can be slow, reduce OCR workload by:

Setting a specific OCR language or whitelist.

Considering alternatives like OpenCV template matching if button appearance is consistent.



This setup should process quickly for small regions, minimizing time compared to full-screen processing at each click.

