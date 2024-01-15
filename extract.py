import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import random

def get_urls_from_page(url, driver):
    driver.get(url)
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    links = soup.find_all('a', href=True)
    return [urljoin(url, link['href']) for link in links]

def recursively_parse_and_print_urls(url, driver):
    urls = get_urls_from_page(url, driver)
    print(f"URLs found on {url}:")
    for url in urls:
        print(url)
        recursively_parse_and_print_urls(url, driver)

def randomly_parse_and_print_urls(url, driver):
    driver.get(url)
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    links = soup.find_all('a', href=True)
    rand_link = random.choice(links)
    link = urljoin(url, rand_link['href'])
    print(link)
    randomly_parse_and_print_urls(link, driver)

def iteratively_parse_and_print_urls(url, driver):
    MAX_DEPTH = 5

    stack = [(url, 0)]
    while stack:
        url, depth = stack.pop()
        driver.get(url)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            absolute_link = urljoin(url, link['href'])
            print(absolute_link)
            if depth < MAX_DEPTH:
                stack.append((absolute_link, depth + 1))



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <url>")
        sys.exit(1)

    starting_url = sys.argv[1]

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode (no GUI)
    driver = webdriver.Chrome(options=chrome_options)

    try:
        # for url in get_urls_from_page(starting_url, driver):
        #     print(url)
        # recursively_parse_and_print_urls(starting_url, driver)
        iteratively_parse_and_print_urls(starting_url, driver)

    finally:
        driver.quit()
