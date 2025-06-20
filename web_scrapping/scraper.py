from math import log
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class WebScraper:
    def __init__(self, url:str, download_folder:str) -> None:
        self.url = url
        self.download_folder = download_folder
        self.options = webdriver.ChromeOptions()
        self.options.add_experimental_option("prefs", {
            "download.default_directory": download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })
        self.options.add_argument("--headless")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=self.service, options=self.options)
        self.wait = WebDriverWait(self.driver, 10)

    def get_page(self):
        try:
            self.driver.get(self.url)
        except Exception as e:
            logging.error(f"Error in get_page: {str(e)}")
            raise

    def get_files(self):
        try:
            resources_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Recursos']]"))
            )
            resources_button.click()

            containers = self.driver.find_elements(By.XPATH, 
                "//div[h4[contains(text(), 'STFC') or contains(text(), 'SCM') or contains(text(), 'SMP')] and .//button[@id='btnDownloadUrl']]")
            logging.info(f"Found {len(containers)} containers")
            
            for i, container in enumerate(containers, 1):
                try:
                    title = container.find_element(By.TAG_NAME, "h4").text
                    button = container.find_element(By.XPATH, ".//button[@id='btnDownloadUrl']")
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                    time.sleep(0.5)
                    
                    button.click()
                    logging.info(f"Downloaded {title}")
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"Error clicking button {i}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Error in get_files: {str(e)}")
            raise

    def remove_files(self):
        logging.info(self.download_folder)
        for file in os.listdir(self.download_folder):
            os.remove(os.path.join(self.download_folder, file))
        logging.info("Files removed")

    def run(self):
        self.remove_files()
        self.get_page()
        self.get_files()
    
    def __del__(self):
        self.driver.quit()
