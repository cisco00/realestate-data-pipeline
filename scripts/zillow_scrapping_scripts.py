import hashlib
import shutil
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

        #Evaluating and updating existing files
    # @staticmethod
    # def updating_files(file):
    #     hash_func = hashlib.md5()
    #     with open(file, "rb") as f:
    #         while chunk := f.read(8192):
    #             hash_func.update(chunk)
    #     return hash_func.hexdigest()
    #
    #
    # def update_file(self, downloaded_file, title):
    #     new_path_file = os.path.join(self.updating_files(downloaded_file), f"title")
    #     if os.path.exists(new_path_file):
    #         old_file = self.updating_files(new_path_file)
    #         new_file = self.updating_files(downloaded_file)
    #
    #         if old_file == new_file:
    #             print("The file already exists can't update current file")
    #             os.remove(downloaded_file)
    #         else:
    #             os.replace(old_file, new_path_file)
    #             print("The file has been updated")
    #     else:
    #         shutil.move(downloaded_file, new_path_file)
    #         print("The file has been updated")


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
                    #
                    # downloaded_files = sorted(
                    #     [os.path.join(self.download_directory, f) for f in os.listdir(self.download_directory)],
                    #     key=os.path.getmtime,
                    #     reverse=True
                    # )
                    # if downloaded_files:
                    #     latest_file = downloaded_files[0]
                    #     self.update_file(latest_file, option.text)

                except Exception as e:
                    print(f"Error clicking download for {option.text}: {e}")

        #updating existing files

        self.driver.quit()

if __name__ == "__main__":
    zillow_scraper = Zillow_Estate()
    zillow_scraper.main()