import requests
from bs4 import BeautifulSoup
import re
link = "https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm"


def finde(text):
    pattern = r'\b\w+\.txt\b'
    matches = re.findall(pattern, text)
    return matches

online = requests.get(link)
soup = BeautifulSoup(online.content, "html.parser")
find = soup.findAll("li", {"class": "MsoNormal"})
i = 0

while True:
    result = finde(find[i].text)
    osn = ''.join(result)
    print(osn[:len(osn)-4])
    i +=1

