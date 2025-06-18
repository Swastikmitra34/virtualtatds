from bs4 import BeautifulSoup
from markdownify import markdownify as md

def html2text(html):
    soup = BeautifulSoup(html, "html.parser")
    content_div = soup.find("div", class_="post")
    if not content_div:
        return ""
    return md(str(content_div))
