from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
import time

class Zillow_Estate:
    def __init__(self):
        self.download_directory = "/home/oem/PycharmProjects/RealEstate_Data_Pipeline/realestate_raw_data_storage"
        if not os.path.exists(self.download_directory):
            os.makedirs(self.download_directory)
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--start-maximized')
        self.options.add_experimental_option("prefs", {
            "download.default_directory": os.path.abspath(self.download_directory),
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True
        })
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)

    def main(self):
        self.driver.get("https://www.zillow.com/research/data/")
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "custom-select")))

        dropdowns = self.driver.find_elements(By.CLASS_NAME, "custom-select")  # Update selector if needed

        for dropdown in dropdowns:
            select = Select(dropdown)
            for option in select.options:
                select.select_by_visible_text(option.text)
                time.sleep(2)  # Wait for the data to load
                try:
                    download_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Download"))
                    )
                    download_button.click()
                    print(f"Downloading data for: {option.text}")
                    time.sleep(5)  # Adjust as needed to wait for download
                except Exception as e:
                    print(f"Error clicking download for {option.text}: {e}")

        self.driver.quit()

if __name__ == "__main__":
    zillow_scraper = Zillow_Estate()
    zillow_scraper.main()