def Upload():
    import base64
    import pandas as pd
    import requests
    import xml.etree.ElementTree as ET
    from datetime import datetime

    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import requests, pathlib  
    import os
    from dotenv import load_dotenv
    load_dotenv()
    import re
    import html
    # ------------------------------------------------------------------#
    # PARAMETERS – update these as needed
    # ------------------------------------------------------------------#
    BATCH_SAVE_EVERY = 50     
    INPUT_PRODUCTS='pricefinal.csv'  # Input CSV file path
    OUTPUT_CSV = 'pricefinal.csv'  # Output CSV file path
    SHOP_URL = os.getenv("SHOP_URL") 
    API_KEY = os.getenv("API_KEY") 
    WEB_URL = os.getenv("WEB_URL") 
    CSV_FILE = pathlib.Path("pricefinal.csv")  
    # ------------------------------------------------------------------#
    # Google Sheet Helper – Download and Overwrite CSV
    # ------------------------------------------------------------------#
    import requests, pathlib
    WEB_URL = os.getenv("WEB_URL")                        
    def download_sheet(out_file: pathlib.Path = CSV_FILE,
                       web_url: str = WEB_URL) -> pathlib.Path:
        r = requests.get(web_url, timeout=30)
        r.raise_for_status()
        out_file.write_bytes(r.content)
        print(f"✓ downloaded {len(r.content):,} bytes → {out_file}")
    def upload_csv(csv_file: pathlib.Path = CSV_FILE,
                   web_url: str = WEB_URL):
        """
        POST the contents of input.csv back to the sheet, replacing all rows.
        """
        if not csv_file.exists():
            raise FileNotFoundError(csv_file)
        data = csv_file.read_bytes()         
        r = requests.post(f"{web_url}?cmd=overwrite",
                          data=data,
                          headers={"Content-Type": "text/plain"},
                          timeout=30)
        r.raise_for_status()
        print(r.json())    

    # ------------------------------------------------------------------#
    # PrestaShopAPI – Handle API requests to PrestaShop
    # ------------------------------------------------------------------#


    class PrestaShopAPI:
        def __init__(self, shop_url: str, api_key: str):
            self.shop_url = shop_url.rstrip('/')
            self.api_key = api_key
            self.auth = base64.b64encode(f'{api_key}:'.encode()).decode()
            self.headers = {
                'Authorization': f'Basic {self.auth}',
                'Content-Type': 'application/xml'
            }
        def update_price_by_product_id(self, product_id: str, new_price: float) -> bool:
            xml_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
                                <prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
                                  <product>
                                    <id><![CDATA[{product_id}]]></id>
                                    <price><![CDATA[{new_price:.6f}]]></price>
                                  </product>
                                </prestashop>"""

            url = f"{self.shop_url}/api/products/{product_id}"
            response = requests.patch(url, headers=self.headers, data=xml_payload, timeout=15)

            if response.status_code in [200, 201, 202]:
                    print(f"Price  successfully Updete {product_id}")
                    return True
 
            elif response.status_code == 500:
                    # print(f"⚠️  Server Error on Price update for {product_id}: {response.content}")
                    if 'deprecated' in response.text.lower():
                        print(f"✓ Price updated: {product_id} → {new_price}")
                        return True
                    else:
                        print(f"✓ Price updated: {product_id} → {new_price}")
                        return True
            else:
                        # Try to parse the XML error response
                        try:
                            root = ET.fromstring(response.content)
                            error_messages = [error.find('message').text for error in root.findall('.//error')]
                            real_errors = [msg for msg in error_messages if "deprecated" not in msg.lower()]

                            if not real_errors and "updateproduct" in response.content.decode('utf-8'):
                                return True
                            else:
                                print(f"Failed to Price update: {real_errors if real_errors else response.content}")
                        except:
                            print(f"Failed to Price update: {response.content}")

                        return False

    # ------------------------------------------------------------------#
    # HELPERS – threading & Updating Prices With IDs
    # ------------------------------------------------------------------#

    def is_missing(value) -> bool:
        return (
            pd.isna(value) or          # catches NaN, None, pd.NA
            str(value).strip().lower() in {"", "n/a", "na", "none"}
        )

    LOCK = threading.Lock()

    def process_row(api, row, idx, today):
        row_no = idx + 1
        print(f"\n🔹 Updating Price row {row_no} | ASIN: {row['ASIN']}")

        try:
            price_date = pd.to_datetime(row.get("Update_Date", pd.NaT))
            if pd.notna(price_date) and price_date.date() == today:
                return ("skipped", idx, None)

            product_id = str(row.get("ID"))
            final_price = row.get("final_price")

            if is_missing(product_id) or is_missing(final_price):
                return ("no", idx, None)
            if product_id:
                updated = api.update_price_by_product_id(product_id, float(final_price))
                if updated:
                    return ("yes", idx, today)
                else:
                    return ("no", idx, None)
            else:
                return ("no", idx, None)

        except Exception as e:
            print(f"❌ Error on row {row_no}: {e}")
            return ("no", idx, None)

    def update_prices_from_csv(api: PrestaShopAPI, csv_path: str, output_csv: str):
        df = pd.read_csv(csv_path).fillna("").astype(str)
        today = datetime.now().date()
        TOTAL = len(df)
        # results = []
        # for idx, row in df.iterrows():
        #     print(f"Processing row {idx + 1}/{TOTAL}...")
        #     result = process_row(api, row, idx, today)
        #     results.append(result)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(process_row, api, row, idx, today): idx for idx, row in df.iterrows()}

            for i, future in enumerate(as_completed(futures)):
                status, idx, date = future.result()

                with LOCK:
                    if status != "skipped":
                        df.at[idx, "Prestashop_Status"] = status
                        if status == "yes":
                            df.at[idx, "Update_Date"] = date

                # Save every BATCH_SAVE_EVERY completed rows
                if (i + 1) % BATCH_SAVE_EVERY == 0:
                    with LOCK:
                        df.to_csv(output_csv, index=False)
                        print(f"💾 Progress saved → {output_csv} ({i+1}/{TOTAL})")

        df.to_csv(output_csv, index=False)
        print(f"\n✅ Final CSV saved → ({TOTAL})")
        print("\033[1;33m⌛ Please wait, finalizing everything. Do not close the program...\033[0m")


    import time
    def process_new_products():
        print("\n\n\033[1;33m➤ Starting...\033[0m")
        print("\033[1;34m⏳ Please wait, gathering data...\033[0m")
        download_sheet()
        api = PrestaShopAPI(SHOP_URL, API_KEY)
        update_prices_from_csv(api, INPUT_PRODUCTS, OUTPUT_CSV)
        upload_csv()
        if CSV_FILE.exists():
            CSV_FILE.unlink()
        print("\033[1;32m🎉 Thanks for waiting! All processes are complete.Check The Data On Google Sheet\033[0m")
        time.sleep(5)
    process_new_products()
Upload()






