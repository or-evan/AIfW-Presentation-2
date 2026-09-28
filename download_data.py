import csv
import datetime
import os
import shutil
import sys
import eumdac

# 1. Load Credentials
KEYS_FILE = "Keys.csv"

if not os.path.exists(KEYS_FILE):
  print(f"Error: File '{KEYS_FILE}' not found in current directory.")
  sys.exit(1)

with open(KEYS_FILE, mode="r") as f:
  reader = csv.reader(f)
  row = next(reader, None)
  if not row or len(row) < 2:
    print(f"Error: '{KEYS_FILE}' must contain consumer_key,consumer_secret.")
    sys.exit(1)
  consumer_key, consumer_secret = row[0].strip(), row[1].strip()

# 2. Authenticate
try:
  credentials = (consumer_key, consumer_secret)
  token = eumdac.AccessToken(credentials)
  datastore = eumdac.DataStore(token)
  print(f"Authentication successful. Token expires: {token.expiration}")
except Exception as e:
  print(f"Authentication failed: {e}")
  sys.exit(1)

# 3. Collection & Batch Configuration
COLLECTION_ID = "EO:EUM:DAT:MSG:HRSEVIRI"
OUTPUT_DIR = "./msg_seviri_data"

start_time = datetime.datetime(2026, 8, 27, 7, 0)
end_time = datetime.datetime(2026, 8, 27, 12, 0)

# Set to number of products to process (set to float('inf') to process all 192)
MAX_PRODUCTS_TO_PROCESS = 5

# 4. Search and Stream Data
try:
  collection = datastore.get_collection(COLLECTION_ID)
  print(f"Connected to collection: '{collection.title}'")

  products = list(collection.search(dtstart=start_time, dtend=end_time))
  print(f"Total products found matching query: {len(products)}")

  if not products:
    print("No products match the search window.")
    sys.exit(0)

  os.makedirs(OUTPUT_DIR, exist_ok=True)

  # Limit target slice for fast iteration
  products_to_run = (
      products[:MAX_PRODUCTS_TO_PROCESS]
      if isinstance(MAX_PRODUCTS_TO_PROCESS, int)
      else products
  )

  downloaded_files_count = 0

  for idx, target_product in enumerate(products_to_run, 1):
    print(
        f"\n[{idx}/{len(products_to_run)}] Processing product:"
        f" {target_product}"
    )

    entries = target_product.entries
    if not entries:
      entries = [None]

    for entry in entries:
      filename = str(entry) if entry else f"{target_product}.nat"
      out_path = os.path.join(OUTPUT_DIR, filename)

      # Skip existing non-empty files
      if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        print(f"  Skipping {filename} (already downloaded)")
        continue

      print(f"  Downloading: {filename}...")
      open_kwargs = {"entry": entry} if entry else {}

      try:
        with (
            target_product.open(**open_kwargs) as f_in,
            open(out_path, "wb") as f_out,
        ):
          shutil.copyfileobj(f_in, f_out)

        file_mb = os.path.getsize(out_path) / (1024 * 1024)
        print(f"  Successfully saved: {out_path} ({file_mb:.2f} MB)")
        downloaded_files_count += 1

      except eumdac.errors.EumdacError as pe:
        print(f"  EUMDAC Error downloading {filename}: {pe}")
        if os.path.exists(out_path):
          os.remove(out_path)  # Clean partial file
      except Exception as e:
        print(f"  Failed to download {filename}: {e}")
        if os.path.exists(out_path):
          os.remove(out_path)  # Clean partial file

  print(f"\nSession completed. New files saved: {downloaded_files_count}")

except Exception as e:
  print(f"Execution error: {e}")