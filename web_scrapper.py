import urllib.request
from bs4 import BeautifulSoup
import re


def scrapeData():
    url = 'http://www.cpinfo.ie/data/archive.html'
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as res:
        html = BeautifulSoup(res.read(), features="lxml")
    return html.find_all('a', href=re.compile(".zip"))
