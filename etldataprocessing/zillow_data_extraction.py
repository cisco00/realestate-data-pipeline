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
        # Set up the directory where files will be downloaded
        self.download_directory = "/home/idoko/PycharmProjects/realEstateHousePrices_AcrossUSAState/realestate-data-pipeline/raw-unprocessed-data"
        os.makedirs(self.download_directory, exist_ok=True)

        # Chrome options for Selenium Grid
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": self.download_directory,
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True,
        })
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        # Remote WebDriver URL (Selenium Docker container)
        self.selenium_grid_url = "http://localhost:4444/wd/hub"

        # Connect to Selenium Grid
        self.driver = webdriver.Remote(
            command_executor=self.selenium_grid_url,
            options=chrome_options
        )


    def main(self):
        self.driver.get("https://www.zillow.com/research/data/")

        self.iframe = WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.ID, "median-home-value-zillow-home-value-index-zhvi-dropdown-2")))

        self.driver.switch_to.frame(self.iframe)

        # WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "custom-select")))

        dropdowns = self.driver.find_elements(By.ID, "median-home-value-zillow-home-value-index-zhvi-dropdown-2")  # Update selector if needed

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