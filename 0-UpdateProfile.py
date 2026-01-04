
import os
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

openai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=openai_api_key)
model="gpt-4.1-mini"

# Example usage
source_profile = "my_profile_0"
num_profiles  = 5


print("\n\n\033[1;33m➤ Starting...\033[0m")
import os
import sys
import shutil

if getattr(sys, 'frozen', False):
    script_dir = os.path.dirname(sys.executable)
else:
    script_dir = os.path.dirname(os.path.abspath(__file__))
internal_dir = os.path.join(script_dir, "_internal")

# Always create _internal first
os.makedirs(internal_dir, exist_ok=True)

def remove_copies(base_name, num_profiles):
    for i in range(num_profiles):
        path = os.path.join(internal_dir, f"{base_name.rsplit('_', 1)[0]}_{i}")
        if os.path.isdir(path):
            shutil.rmtree(path)

def create_copies(base_name, num_profiles):
    source = os.path.join(internal_dir, base_name)
    if not os.path.isdir(source):
        print("Source profile missing!")
        return
    for i in range(1, num_profiles):
        dest = os.path.join(internal_dir, f"{base_name.rsplit('_', 1)[0]}_{i}")
        shutil.copytree(source, dest)
        
remove_copies(source_profile, num_profiles)


def ocr_captcha_image(image_url):
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract the text from this captcha image. you just need to type the text you see in the image. don't write extra text.there will be 6 characters in the image. "},
                    {
                        "type": "image_url",
                        "image_url": {"url": image_url},
                    },
                ],
            }
        ],
        max_tokens=7,  # Limit output length
    )

    return response.choices[0].message.content.strip()


driver_path = os.path.join(script_dir, "chromedriver.exe") 
internal_dir = os.path.join(script_dir, "_internal")
os.makedirs(internal_dir, exist_ok=True)  
profile0_path = os.path.join(internal_dir, source_profile)
os.makedirs(profile0_path, exist_ok=True)

chrome_options = Options()
chrome_options.add_argument(f"--user-data-dir={profile0_path}")
chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
chrome_options.add_experimental_option("prefs", {
    "profile.default_content_setting_values.notifications": 2,
    "profile.default_content_setting_values.media_stream_camera": 2,
    "profile.default_content_setting_values.media_stream_mic": 2,
})
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("Permissions-Policy=interest-cohort=()")
# chrome_options.add_argument("--headless")
# chrome_options.add_argument("--window-size=1920,1080")  # Set window size to avoid rendering issues inheadless mode
chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
chrome_options.add_argument('--log-level=3')  # Add this for Chrome's logging
driver = webdriver.Chrome(options=chrome_options)
driver.minimize_window()
driver.get("https://www.amazon.com/")


print("\033[1;33m⌛ Please wait, finalizing everything. Do not close the program...\033[0m")



asin_list = ["B00266J9OE","B0CFQNMHVL"]
amazon_sites = [
    ("https://www.amazon.fr/", "75017"),
    ("https://www.amazon.es/", "28001"),
    ("https://www.amazon.de/", "10115"),
    ("https://www.amazon.it/", "00127"),
    ("https://www.amazon.co.uk/", "SW1W 0NY"),
    ("https://www.amazon.com/", "10013"),
]

driver.get("https://www.ebay.com/sch/i.html?_nkw=810104130707&_sacat=0&_fcid=1&_from=R40&_trksid=m570.l1313")
time.sleep(1)
for asin in asin_list:
    for base_url, postal_code in amazon_sites:
        country_code = base_url.split("//www.amazon.")[1].split("/")[0]
        product_url = f"{base_url}dp/{asin}"
        

        try:
            driver.get(product_url)
            time.sleep(2)

            # Handle captcha if present
            for attempt in range(3):
                try:
                    captcha_header = driver.find_element(
                        By.XPATH,
                        "//div[contains(@class, 'a-box') and contains(@class, 'a-alert')]//div[contains(@class, 'a-box-inner')]//h4"
                    )
                    if captcha_header:
                        captcha_img = driver.find_element(By.XPATH, "//div[@class='a-row a-text-center']/img")
                        captcha_src = captcha_img.get_attribute("src")

                        # Handle OCR logic here
                        captcha_text = ocr_captcha_image(captcha_src)
                        captcha_input = driver.find_element(By.ID, "captchacharacters")
                        captcha_input.send_keys(captcha_text)
                        captcha_input.send_keys(Keys.RETURN)
                        time.sleep(2)
                        continue
                except:
                    break

            # Accept cookies if prompt exists
            try:
                accept_cookies = driver.find_element(By.ID, "sp-cc-accept")
                accept_cookies.click()
                time.sleep(2)
            except:
                pass

            # Set delivery location
            try:
                delivery_btn = driver.find_element(By.ID, "contextualIngressPtLink")
                delivery_btn.click()
                time.sleep(2)

                try:
                    zip_input = driver.find_element(By.ID, "GLUXZipUpdateInput")
                    zip_input.clear()
                    zip_input.send_keys(postal_code)
                    zip_input.send_keys(Keys.RETURN)
                    time.sleep(2)
                except:
                    pass

                try:
                    done_btn = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.NAME, "glowDoneButton"))
                    )
                    done_btn.click()
                    time.sleep(1)
                except:
                    try:
                        confirm_close = driver.find_element(By.ID, "GLUXConfirmClose")
                        confirm_close.click()
                        time.sleep(1)
                    except:
                        pass
                print(f"✅ Done: {product_url}")
            except:
                print("⚠️ Delivery location setup failed.")

        except Exception as e:
            print(f"❌ Error opening {product_url}: {e}")
            continue

driver.quit()
print("\033[1;34m⏳ Please wait\033[0m")
time.sleep(2)
create_copies(source_profile, num_profiles)
print("\033[1;32m🎉 Thanks for waiting!. Now You Can Run Next App..\033[0m")











