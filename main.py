import re
from math import trunc

from selenium.webdriver.common.by import By
import zipcode
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException

def get_zipcode_list(items):
    if  isinstance(items, str):
        zipcode_obj = zipcode.islike(items)
        output = [str(i).split("", 1)[1].split(">")[0] for i in zipcode_obj]

    elif isinstance(items, list):
        zipcode_obj = [n for i in items for n in zipcode.islike(str(i))]
        output = [str(i).split("", 1)[1].split(">")[0] for i in zipcode_obj]

    else:
        raise ValueError("Items must be of type string or list")
    return output


def init_driver(file_path):
    drivwr = webdriver.Chrome(executable_path=file_path)
    drivwr.wait = WebDriverWait(drivwr, 5)
    return (drivwr)


def navigate_to_website(drivwr, url):
    drivwr.get(url)


def click_button(driver):
    try:
        button = driver.wait.until(EC.element_to_be_clickable(By.CLASS_NAME, 'nav_header'))
        button.click()
        time.sleep(10)
    except (TimeoutException, NoSuchElementException):
        return ValueError("Clicking buy button failed")


def search_term(driver, search_term):
    try:
        search_bar = driver.wait.until(EC.presence_of_element_located(By.ID, "citystatezip"))
        button = driver.wait.until(EC.element_to_be_clickable(By.CLASS_NAME, "zsg-icon-searchglass"))

        search_bar.clear()
        time.sleep(3)
        search_bar.send_keys(search_term)
        button.click()
        time.sleep(3)
        return (True)

    except (TimeoutException, NoSuchElementException):
        return (False)


def test_result(driver):
    try:
        no_result = driver.find_element_by_css_selector('.zoom-out-message').is_displayed()
    except (NoSuchElementException, TimeoutException):
        try:
            no_result = driver.find_element_by_class_name('zsg-icon-x-thick').is_displayed()
        except (NoSuchElementException, TimeoutException):
            no_result = False
    return (no_result)


def get_html(driver):
    output = []
    keep_going = True

    while keep_going:
        # pull te html page
        try:
            output.append(driver.page_source)
        except TimeoutException:
            pass

        #checking if next page exist
        try:
            keep_going = driver.find_element_by_class_name("zsg-pagination-next").is_displayed()
        except NoSuchElementException:
            keep_going = False

        #ensure the updated image doesn't display
        # Will try up to 5 times before giving up, with a 5 second wait
        if keep_going:
            tries = 5
            try:
                cover = driver.find_element_by_class_name('list-loading-message-cover').is_displayed()
            except NoSuchElementException:
                cover = False

            while cover and tries > 0:
                time.sleep(5)
                tries -= 1

                try:
                    cover = driver.find_element_by_class_name('list-loading-message-cover').is_displayed()
                except (TimeoutException, NoSuchElementException):
                    cover = False

                # If the "updating results" image is confirmed to be gone
                # (cover == False), click next page. Otherwise, give up on trying
                # to click thru to the next page of house results, and return the
                # results that have been scraped up to the current page.
                if cover == False:
                    try:
                        driver.wait.until(EC.element_to_be_clickable(By.CLASS_NAME, 'zsg-pagination-next')).click()
                        time.sleep(3)
                    except TimeoutException:
                        keep_going = False
                else:
                    keep_going = False
    return output


def get_list(list_obj):
    output = []
    for i in list_obj:
            htmmSplit = i.split("", id="zipid_")[1:]
            output += htmmSplit
    print(str(len(output)) + " Home listing scrapped. ")
    return output


def str_address(soup_obj):
    try:
        street = soup_obj.find("span", {"itemprop": "streetAddress"}).get_text().strip()
    except (ValueError, AttributeError):
        street = "NA"
    if len(street) == 0 or street == "null":
        street = "NA"
    return (street)


def get_city(soup_obj):
    try:
        city = soup_obj.find("span", {"itemprop": "addressLocality"}).get_text().strip()
    except (ValueError, AttributeError):
        city = "NA"
    if len(city) == 0 or city == "null":
        city = "NA"
    return (city)


def get_neighbourhood(soup_obj):
    try:
        neighbor = soup_obj.find("span", {"itemprop": "addressNeighbour"}).get_text().strip()
    except (ValueError, AttributeError):
        neighbor = "NA"
    if len(neighbor) == 0 or neighbor == "null":
        neighbor = "null"
    return (neighbor)


def region(soup_obj):
    try:
        state = soup_obj.find("span",{"itemprop": "addressRegion"}).get_text().strip()
    except (ValueError, AttributeError):
        state = "NA"
    if len(state) == 0 or state == "null":
        state = "NA"
    return (state)


def zipcode(soup_obj):
    try:
        zip = soup_obj.find("span", {"itemprop": "postalCode"}).get_text().strip()
    except (ValueError, AttributeError):
        zip = "NA"
    if len(zip) == 0 or zip == "null":
        zip = "NA"
    return (zip)


def house_prices(soup_obj, list_obj):
    try:
        price = soup_obj.find("span", {"class": "zsg-photo-card-price"}).get_text().strip()
    except (ValueError, AttributeError):

        try:
            price = [n for n in list_obj
                     if any(["$" in n, "K" in n, "k" in n])]
            if len(price) > 0:
                price = price[0].split(" ")
                price = [n for n in price if re.search("[0-9]", n) is not None]
                if len(price) > 0:
                    price = price[0]
                else:
                    price = "NA"
            else:
                price = "NA"
        except (ValueError, AttributeError):
            price = "NA"
    if len(price) == 0 or price == "null":
        price = "NA"
    if price is not "NA":
        price = price.replace(",", " ").replace("+", " ").replace("$", "")
        if any(["K" in price, "K" in price]):
            price = price.lower().split("K")[0].strip()
            if "." not in price:
                price = price + "000000"
            else:
                priceLen = len(price.split(".")[0]) +6
                price = price.replace(".", "")
                diff = priceLen - len(price)

                price = price + (diff * "0")
        if len(price) == 0:
            price = "NA"
    return (price)


def card_details(soup_obj):
    try:
        card = soup_obj.find({"class": "zsg-photo-card-info"}).get_text().strip()
    except (ValueError, AttributeError):
        card = "NA"
    if len(card) == 0 or card == "null":
        card = "NA"
    return (card)