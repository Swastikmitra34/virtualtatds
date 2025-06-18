import os
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from utils import html2text

BASE_URL = "https://tds.s-anand.net/#/"


def get_all_pages():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(BASE_URL)

    time.sleep(5)  # Wait for JS rendering
    soup = BeautifulSoup(driver.page_source, "html.parser")
    sidebar = soup.find("aside")
    links = []
    if sidebar:
        for a in sidebar.find_all("a", href=True):
            href = a["href"]
            if href.startswith("#/"):
                links.append("https://tds.s-anand.net/#/" + href[2:])
    driver.quit()
    return links


def scrape_tds():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    urls = get_all_pages()
    print(f"Found {len(urls)} pages.")

    for url in urls:
        driver.get(url)
        time.sleep(2)  # Wait for JS

        html = driver.page_source
        md = html2text(html)

        name = url.split("/")[-1].split("#")[-1] or "index"
        filename = f"data/tds/{name}.md"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"Saved {filename}")

    driver.quit()


if __name__ == "__main__":
    os.makedirs("data/tds", exist_ok=True)
    scrape_tds()
