from __future__ import annotations

# ────────────────────────────────────────────────
# STANDARD LIB IMPORTS
# ────────────────────────────────────────────────
import os, re, time, socket, threading, queue, csv, pathlib
from datetime import date
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import math
# ────────────────────────────────────────────────
# THIRD‑PARTY IMPORTS
# ────────────────────────────────────────────────
import numpy as np
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, InvalidSessionIdException
)
import sys
from dateutil import parser
from dotenv import load_dotenv
load_dotenv()
# ────────────────────────────────────────────────
# CONSTANTS & PARAMETERS (env vars overridable)
# ────────────────────────────────────────────────
PRICE_COLS      = ["ebay", "fr", "sp", "it", "uk", "gr", "us"]
INPUT_PRODUCTS  = "input.csv"
INPUT_AMAZON    = "amazon_list.csv"
INPUT_CURRENCY  = "currency.csv"
MARGINS_CSV     = "margins.csv"
OUTPUT_CSV      = "input.csv"

PROFILE_DIR     = "my_profile"
HEADLESS        = os.getenv("HEADLESS", "False")
ROW_UPDATE      = int(os.getenv("ROW_UPDATE", 50))
BATCH_SAVE_EVERY= 10
NUM_THREADS     = int(os.getenv("NUM_THREADS", 5))
NUMBER_OF_DAYS  = int(os.getenv("numberofdays", 2))

PAGE_LOAD_TIMEOUT   =25     # seconds page may take to deliver base HTML
MAX_RENDERER_TIMEOUT= 40     # seconds JS may run before we abort
PER_TASK_TIMEOUT    = 120    # seconds a single product scrape may take
RECYCLE_EVERY       = 30     # recycle driver every N products

WEB_URL = os.getenv("WEB_URL")
CSV_FILE = Path("input.csv")

# ────────────────────────────────────────────────
# NETWORK HELPER
# ────────────────────────────────────────────────
def wait_for_connection(host="8.8.8.8", port=53, timeout=3):
    """Block until outbound internet is reachable."""
    while True:
        try:
            socket.setdefaulttimeout(timeout)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)\
                  .connect((host, port))
            print("✅ Internet connected.")
            return
        except socket.error:
            print("🔄 Internet disconnected. Retrying in 5 s…")
            time.sleep(5)

# ────────────────────────────────────────────────
# GOOGLE‑SHEET HELPERS
# ────────────────────────────────────────────────
def download_sheet(out_file: Path = CSV_FILE, web_url: str = WEB_URL) -> Path:
    r = requests.get(web_url, timeout=30)
    r.raise_for_status()
    out_file.write_bytes(r.content)
    print(f"✓ downloaded {len(r.content):,} bytes → {out_file}")
    return out_file

def upload_csv(csv_file: Path = CSV_FILE, web_url: str = WEB_URL):
    if not csv_file.exists():
        raise FileNotFoundError(csv_file)
    for i in range(4):
      if i >3:
            print("Failed to upload after 3 attempts. Try Again After Few hours.")
            return
      try:
          r = requests.post(f"{web_url}?cmd=overwrite",
                            data=csv_file.read_bytes(),
                            headers={"Content-Type": "text/plain"},
                            timeout=30)
          r.raise_for_status()
      except requests.RequestException as e:
          print("Failed to upload Please Wait Trying Again")
          time.sleep(5*i)
          continue
      
      
      
        
    
    print(r.json())

# ────────────────────────────────────────────────
# MARGIN HELPERS (unchanged from your original)
# ────────────────────────────────────────────────
def _parse_range(label: str) -> tuple[float, float]:
    label = label.replace("–", "-").strip()
    if m := re.match(r"(\d+)\s*-\s*(\d+)", label):
        return float(m[1]), float(m[2])
    if m := re.match(r"Greater\s+than\s+(\d+)", label, re.I):
        return float(m[1]), float("inf")
    raise ValueError(f"Unrecognised range label: {label}")

def _load_margins(path: Path):
    mdf = pd.read_csv(path, header=None)
    labels, vals = mdf.iloc[0].tolist(), mdf.iloc[1].astype(float).tolist()
    price_range_margins = [(_parse_range(labels[i]), vals[i]) for i in range(5)]
    us_margin, other_margin = float(vals[5]), float(vals[6])
    return price_range_margins, us_margin, other_margin

def _margin_for_price(price: float, ranges):
    for (low, high), margin in ranges:
        if low <= price <= high:
            return margin
    return 0.0
def min_item(d):
    return min(d.items(), key=lambda x: x[1]) if d else (None, float('inf'))
def lowest_price_among(row: pd.Series,us_margin: float) -> tuple[float, str]:
    valid_prices = {
        col: row[col] for col in PRICE_COLS
        if pd.notna(row.get(col))
    }
    if not valid_prices:
        return np.nan, ""

    group1, group2 = {}, {}

    for source, price in valid_prices.items():
        if "us" ==source.lower():
            group1[source] = price
        elif "ebay"  == source.lower():
            group1[source] = price
        else:
            group2[source] = price
    # print(f"Group 1: {group1} | Group 2: {group2}")
    min1_source, min1_price = min_item(group1)
    min2_source, min2_price = min_item(group2)
    
    if min1_price+us_margin < min2_price:
        lowest_source, lowest_price = min1_source, min1_price
        
        lowest_group  = "group1"
        # print(f"Lowest price from group 1: {lowest_price} | Source: {lowest_source}")
        return lowest_price, lowest_source
    else:
        lowest_source, lowest_price = min2_source, min2_price
        lowest_group  = "group2"
        # print(f"Lowest price from group 2: {lowest_price} | Source: {lowest_source}")
        return lowest_price, lowest_source

# ────────────────────────────────────────────────
# SELENIUM DRIVER FACTORY & SAFE_GET
# ────────────────────────────────────────────────
def create_driver(profile_dir: str = PROFILE_DIR) -> webdriver.Chrome:
    if getattr(sys, 'frozen', False):
       script_dir = os.path.dirname(sys.executable)
    else:
      script_dir = os.path.dirname(os.path.abspath(__file__))
    internal_dir= os.path.join(script_dir, "_internal")
    # profile_path= os.path.join(internal_dir, f"{PROFILE_DIR}_{profile_suffix}")

    profile_path = os.path.join(internal_dir, profile_dir)  # Path to your chrome profile
    print(f"Profile path: {profile_path}")
    if not os.path.exists(profile_path):
        print("Run 0-UpdateProfile then Try Again")
    # # Always create _internal first
    # os.makedirs(internal_dir, exist_ok=True)
    # os.makedirs(profile_path, exist_ok=True)

    opts = Options()
    opts.add_argument(f"--user-data-dir={profile_path}")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.media_stream_camera": 2,
        "profile.default_content_setting_values.media_stream_mic": 2,
    })
    opts.add_argument("disable-infobars")
    opts.add_argument("--disable-extensions")
    opts.add_argument("Permissions-Policy=interest-cohort=()")
    if HEADLESS=="True":
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    # return after base HTML – speeds up and avoids many hangs
    opts.page_load_strategy = "none"

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.set_script_timeout(MAX_RENDERER_TIMEOUT)
    return driver

def safe_get(driver: webdriver.Chrome, url: str) -> bool:
    """
    Loads only the base HTML; aborts resource loads on timeout.
    Returns True if HTML arrived, else raises to let caller recycle driver.
    """
    try:
        driver.get(url)
        return True
    except TimeoutException:
        # HTML arrived but resources still loading – stop them
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass
        return True
    except (WebDriverException, InvalidSessionIdException):
        # Chromedriver socket froze → propagate so caller can rebuild driver
        raise

# ────────────────────────────────────────────────
# SCRAPERS
# ────────────────────────────────────────────────
def get_ebay_price(driver: webdriver.Chrome, product_ean: str):
    url = ("https://www.ebay.com/sch/i.html?_nkw=" +
           product_ean.replace(".0", "") +
           "&_sacat=0&_from=R40&rt=nc&LH_ItemCondition=3")

    for attempt in range(4):
        try:
            safe_get(driver, url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "mainContent"))
            )

            lowest_price = float("inf")
            lowest_price_url = None
            # for li in driver.find_elements(By.CSS_SELECTOR, "ul.srp-results li.s-item")[:4]:
            #     try:
            #         price_el = li.find_element(By.CLASS_NAME, "s-item__price")
            #         price_text = price_el.text.strip().replace("$", "").replace(",", "")
            #         price = float(price_text.split("to")[0].strip()) if "to" in price_text else float(price_text)
            #         item_url = li.find_element(By.CSS_SELECTOR, "a.s-item__link").get_attribute("href")
            #         if price < lowest_price:
            #             lowest_price = price
            #             lowest_price_url = item_url
            #             return lowest_price, lowest_price_url
            #     except Exception:
            #         continue
            
            # for li in driver.find_elements(By.CSS_SELECTOR, "li.s-card")[:4]:
            #     try:
            #         # Extract price
            #         price_el = li.find_element(By.CSS_SELECTOR, "span.s-card__price")
            #         price_text = price_el.text.strip().replace("$", "").replace(",", "")
            #         price = float(price_text.split("to")[0].strip()) if "to" in price_text else float(price_text)
            #         print("Found eBay price:", price)
            
            #         # Extract item link
            #         item_url = li.find_element(By.CSS_SELECTOR, "a.su-link").get_attribute("href")
            #         print("Found eBay item URL:", item_url)
            
            #         # Update lowest price
            #         if price < lowest_price:
            #             lowest_price = price
            #             lowest_price_url = item_url
            
            #     except Exception as e:
            #         continue
                
            # print("Lowest price:", lowest_price, "URL:", lowest_price_url)
            for li in driver.find_elements(By.CSS_SELECTOR, "li.s-card")[:4]:
                try:
                    # Extract price (current + fallback)
                    try:
                        price_el = li.find_element(By.CSS_SELECTOR, "span.s-card__price")
                    except Exception:
                        price_el = li.find_element(By.CSS_SELECTOR, ".s-item__price")  # fallback
            
                    price_text = price_el.text.strip().replace("$", "").replace(",", "")
                    price = float(price_text.split("to")[0].strip()) if "to" in price_text.lower() else float(price_text)
                    # print("Found eBay price:", price)
            
                    # Extract item link (current + fallbacks)
                    item_url = None
                    for sel in ("a.s-card__link", "a.su-link", "a.s-item__link"):
                        try:
                            item_url = li.find_element(By.CSS_SELECTOR, sel).get_attribute("href")
                            if item_url:
                                break
                        except Exception:
                            continue
                    # print("Found eBay item URL:", item_url)
            
                    # Update lowest price
                    if item_url and price < lowest_price:
                        lowest_price = price
                        lowest_price_url = item_url
            
                except Exception as e:
                    continue
                
            # print("Lowest price:", lowest_price, "URL:", lowest_price_url)
            
            if lowest_price != float("inf"):
                return lowest_price, lowest_price_url
            return None, None
        except Exception:
            if attempt < 3:
                time.sleep(1)
                wait_for_connection()
            else:
                return None, None

def parse_price(price_text: str) -> float:
    for sym in ["$", "USD", "€", "EUR", "£", "GBP"]:
        price_text = price_text.replace(sym, "")
    price_text = price_text.replace(",", "").strip()
    clean = re.sub(r"[^\d.]", "", price_text)
    return float(clean)

def get_amazon_price(driver: webdriver.Chrome, url: str):
    for attempt in range(3):
        try:
            safe_get(driver, url)
            time.sleep(2)

            # quick CAPTCHA check
            try:
                    captcha_header = driver.find_element(
                        "xpath",
                        "//div[contains(@class, 'a-box') and contains(@class, 'a-alert')]//div[contains(@class, 'a-box-inner')]//h4"
                    )

                    if captcha_header:
                        captcha_img = driver.find_element("xpath", "//div[@class='a-row a-text-center']/img")
                        print("Captcha detected! Please Run the update_profile.py script to solve it.")
                        exit(0)    
            except Exception as e:
                pass 

            if not driver.find_elements(By.ID, "add-to-cart-button") and \
               not driver.find_elements(By.ID, "buy-now-button"):
                return None  # out of stock

            try:
                whole = driver.find_element(By.CLASS_NAME, "a-price-whole").text
                frac  = driver.find_element(By.CLASS_NAME, "a-price-fraction").text
                price_text = f"{whole}.{frac}"
                if price_text == ".":
                    price_text = driver.find_element(
                        By.XPATH,
                        "//div[@class='a-section a-spacing-micro']//span[@class='a-offscreen']"
                    ).get_attribute("textContent").strip()
                return parse_price(price_text)
            except Exception:
                return None
        except RuntimeError:
            return None          # CAPTCHA → treat as unavailable
        except Exception:
            if attempt < 2:
                time.sleep(1)
                wait_for_connection()
            else:
                return None

# ────────────────────────────────────────────────
# HARD‑TIMEOUT HELPER
# ────────────────────────────────────────────────
def run_with_timeout(fn, timeout, *args, **kwargs):
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *args, **kwargs)
        return fut.result(timeout=timeout)

# ────────────────────────────────────────────────
# MAIN PROCESSING PIPELINE
# ────────────────────────────────────────────────
def main():
    print("\033[1;34m⏳ Gathering data…\033[0m")
    download_sheet()

    today = date.today()

    products      = pd.read_csv(INPUT_PRODUCTS, dtype={"ean": str}).fillna("").astype(str)
    amazon_sites  = pd.read_csv(INPUT_AMAZON)
    currency_df   = pd.read_csv(INPUT_CURRENCY, index_col="currency")
    ranges, us_margin, other_margin = _load_margins(Path(MARGINS_CSV))
    exchange_rates = currency_df["rate"].to_dict()

    # ensure all price/link columns exist
    for col in PRICE_COLS:
        products[col] = pd.to_numeric(products.get(col, np.nan), errors="coerce")
    for c in ["eb","fr","sp","it","uk","gr","us"]:
        products[f"{c}_link"] = products.get(f"{c}_link", "")

    TOTAL = len(products)
    task_q = queue.Queue()

    for idx, row in products.iterrows():
    # for idx, row in products.iloc[500:].iterrows():
    


        price_date_str = row.get("Price_Date", None)

        if price_date_str:
            try:
                pd_date = parser.parse(price_date_str)
            except Exception:
                # Fallback to pandas (handles ISO, standard formats)
                pd_date = pd.to_datetime(price_date_str, errors="coerce")
        else:
            pd_date = pd.NaT

        if pd.notna(pd_date) and (today - pd_date.date()).days <= NUMBER_OF_DAYS:
            continue
        task_q.put((idx, row))


    lock = threading.Lock()
    processed_count = 0

    # ────────────────────────────────
    # WORKER FUNCTION
    # ────────────────────────────────
    def worker(tid: int):
        nonlocal processed_count
        driver = create_driver(f"{PROFILE_DIR}_{tid}")
        pages_handled = 0

        while True:
            try:
                idx, row = task_q.get_nowait()
            except queue.Empty:
                break

            row_no = idx + 1
            try:
                start = time.perf_counter()
                out = run_with_timeout(process_row, PER_TASK_TIMEOUT,
                                       driver, idx, row, amazon_sites,
                                       exchange_rates, today, ranges,
                                       us_margin, other_margin)
                elapsed = time.perf_counter() - start

                with lock:
                    for k, v in out.items():
                        products.at[idx, k] = v
                    print(f"✅ T{tid} Row {row_no}/{TOTAL} in {elapsed:.1f}s")
                    processed_count += 1
                    if processed_count % BATCH_SAVE_EVERY == 0:
                        products.to_csv(OUTPUT_CSV, index=False,
                                        quoting=csv.QUOTE_NONNUMERIC,
                                        float_format="%.2f")
                        print(f"💾 Progress → {OUTPUT_CSV}")
                    if processed_count % ROW_UPDATE == 0:
                        upload_csv()
                        print("📤 Sheet updated")

            except FutureTimeout:
                print(f"⏰ T{tid} Row {row_no} >{PER_TASK_TIMEOUT}s – restart Chrome")
                task_q.put((idx, row))
                try: driver.quit()
                except Exception: pass
                driver = create_driver(f"{PROFILE_DIR}_{tid}")

            except (WebDriverException, InvalidSessionIdException) as e:
                print(f"♻️  T{tid} Selenium error: {e} – restart Chrome")
                task_q.put((idx, row))
                try: driver.quit()
                except Exception: pass
                driver = create_driver(f"{PROFILE_DIR}_{tid}")

            finally:
                task_q.task_done()

            pages_handled += 1
            if pages_handled % RECYCLE_EVERY == 0:
                try: driver.quit()
                except Exception: pass
                driver = create_driver(f"{PROFILE_DIR}_{tid}")

        try: driver.quit()
        except Exception: pass

    # ────────────────────────────────
    # THREAD SPAWN
    # ────────────────────────────────
    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        t.start()
        time.sleep(1)
        threads.append(t)
    for t in threads:
        t.join()

    # ────────────────────────────────
    # FINAL SAVE & CLEAN
    # ────────────────────────────────
    products.to_csv(OUTPUT_CSV, index=False,
                    quoting=csv.QUOTE_NONNUMERIC,
                    float_format="%.2f")
    print("\n✅ All done! Results →", Path(OUTPUT_CSV).resolve())
    upload_csv()
    if CSV_FILE.exists():
        CSV_FILE.unlink()
    print("\033[1;32m🎉 Finished. You can now run UpdatePrice.\033[0m")

# ────────────────────────────────────────────────
# ROW PROCESSOR (needs amazon_sites etc. passed in)
# ────────────────────────────────────────────────
def process_row(driver, idx, row, amazon_sites, exchange_rates,
                today, ranges, us_margin, other_margin):
    res = {c: np.nan for c in PRICE_COLS}
    for c in ["eb","fr","sp","it","uk","gr","us"]:
        res[f"{c}_link"] = ""

    # eBay
    ean = str(row["ean"]).strip().lower().replace("/", "\\")
    if ean not in {"n\\a", "", "na"}:
        p, link = get_ebay_price(driver, row["ean"])
        if p is not None:
            p *= exchange_rates.get("usd", 1)
            res["ebay"], res["eb_link"] = p, link or ""

    # Amazon sites
    for _, arow in amazon_sites.iterrows():
        base = arow["url"]
        cc   = base.split("//www.amazon.")[1].split("/")[0]
        url  = f"{base}dp/{row['ASIN']}"
        price = get_amazon_price(driver, url)
        if price is not None:
            if cc == "com":   price *= exchange_rates.get("usd", 1)
            if cc == "co.uk": price *= exchange_rates.get("pound", 1)
        dest, link_col = {
            "fr": ("fr", "fr_link"), "es": ("sp", "sp_link"),
            "it": ("it", "it_link"), "co.uk": ("uk", "uk_link"),
            "de": ("gr", "gr_link"), "com": ("us", "us_link")
        }.get(cc, (None, None))
        if dest:
            res[dest] = round(price, 2) if price is not None else np.nan
            res[link_col] = url

    # best & final
    bp, src =lowest_price_among(pd.Series(res), us_margin)
    res["best_price"], res["shop_status"] = bp, src
    if pd.notna(bp):
        m  = _margin_for_price(bp, ranges)
        sm = us_margin if src.lower() in {"us", "ebay"} else other_margin
        
        # res["final_price"] = round(bp + m + sm, 2)
        res["final_price"] = (math.floor(bp + m + sm) - 1 + 0.99 
                      if (bp + m + sm) % 1 < 0.5 
                      else math.floor(bp + m + sm) + 0.99)

    else:
        res["final_price"] = np.nan
    res["Price_Date"] = today.strftime("%Y-%m-%d")
    return res

# # ────────────────────────────────────────────────
# # ORPHAN‑CHROME CLEANER (optional but handy)
# # ────────────────────────────────────────────────
# def kill_orphan_chrome():
#     me = os.getpid()
#     for p in psutil.process_iter(["pid", "name", "ppid"]):
#         if p.info["name"] and "chrome" in p.info["name"].lower() and p.info["ppid"] == me:
#             try: os.kill(p.info["pid"], signal.SIGTERM)
#             except Exception: pass

# ────────────────────────────────────────────────
# ENTRY POINT
# ────────────────────────────────────────────────
if __name__ == "__main__":
    print("\033[1;33m➤ Starting…\033[0m")
    wait_for_connection()
    main()
    print("\033[1;32m🎉 Thanks for waiting! All processes are complete. You can now update the prices By Runing UpdatePrice\033[0m")
    time.sleep(2)
    from UpdatePrice import Upload
    Upload()
    # kill_orphan_chrome()
    time.sleep(5)
