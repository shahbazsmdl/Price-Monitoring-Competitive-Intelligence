
import base64
import requests
import os
import xml.etree.ElementTree as ET
import sqlite3
from pathlib import Path
import requests
from dotenv import load_dotenv
load_dotenv()




class PrestaShopAPI:
    def __init__(self, shop_url: str, api_key: str):
        self.shop_url = shop_url.rstrip('/')
        self.api_key = api_key
        self.auth = base64.b64encode(f'{api_key}:'.encode()).decode()
        self.headers = {
            'Authorization': f'Basic {self.auth}',
            'Content-Type': 'application/xml'
        }

    def fetch_active_products_with_reference(self):
        
        """
        Return a list of (product_id, reference) tuples for products that are:
        • active = 1
        • have a non-empty reference code
        """
        url = (
            f"{self.shop_url}/api/products"
            "?display=[id,reference]"     # only these two fields
            "&filter[active]=[1]"         # keep only active products
        )

        try:
            resp = requests.get(url, headers=self.headers, timeout=120)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)

            results = []
            for product in root.findall(".//product"):
                ref_el = product.find("reference")
                ref = ref_el.text.strip() if ref_el is not None and ref_el.text else ""
                if ref:                      # skip products without a reference
                    prod_id = product.find("id").text
                    results.append((prod_id, ref))

            return results

        except (requests.HTTPError, ET.ParseError) as err:
            raise RuntimeError(f"Failed to fetch products: {err}") from err


def process_new_products(db_path: str = "slave.db") -> None:
    SHOP_URL = os.getenv("SHOP_URL") 
    API_KEY = os.getenv("API_KEY")          
    api = PrestaShopAPI(SHOP_URL, API_KEY)
    # ------------------------------------------------------------------
    # 0. Prep the meta table (once).
    # ------------------------------------------------------------------
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                id               INTEGER PRIMARY KEY CHECK(id = 1),
                last_processed_id INTEGER NOT NULL DEFAULT 0
            );
        """)
        # make sure there is exactly one row (id=1)
        conn.execute("INSERT OR IGNORE INTO meta(id) VALUES (1);")

    # ------------------------------------------------------------------
    # 1. Read the current high-water mark.
    # ------------------------------------------------------------------
    # last_processed_id = 5783
    last_processed_id = conn.execute(
        "SELECT last_processed_id FROM meta WHERE id = 1"
    ).fetchone()[0]
    # print(f"[meta] last_processed_id = {last_processed_id}")

    # ------------------------------------------------------------------
    # 2. Grab live (ID, ASIN) tuples.
    # ------------------------------------------------------------------
    records = api.fetch_active_products_with_reference()
    # make sure IDs are ints for comparison
    records = [(int(pid), ref) for pid, ref in records]


    # ------------------------------------------------------------------
    # 3. Keep only those strictly greater than the stored ID.
    # ------------------------------------------------------------------
    new_rows = [row for row in records if row[0] > last_processed_id]
    if not new_rows:
        print("\033[1;90m🙁 Sorry, no new products to record.\033[0m")
        return
    WEB_URL = os.getenv("WEB_URL") 
    payload = {"rows": new_rows}
    r = requests.post(f"{WEB_URL}?cmd=append",
                      json=payload,
                      timeout=30)
    print(r.json())



    # ------------------------------------------------------------------
    # 5. Update the meta table with the greatest ID just saved.
    # ------------------------------------------------------------------
    new_max_id = max(row[0] for row in new_rows)
    with conn:
        conn.execute(
            "UPDATE meta SET last_processed_id = ? WHERE id = 1",
            (new_max_id,),
        )
    print(f"[meta] last_processed_id updated → {new_max_id}")
    conn.close()
    print("New products added to the sheet.")
    print("\033[1;90m🙁Please add EAN numbers in Google Sheets.\033[0m")
    
import time
print("\n\033[1;35m🛒 Checking for newly added products...\033[0m")
process_new_products()
time.sleep(5)