import requests
from bs4 import BeautifulSoup

link = "https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm"


online = requests.get(link)
soup = BeautifulSoup(online.content, "html.parser")
find = soup.findAll("p", {})
finde = soup.findAll("ul", {"type": "disc"})
i = 0
while True:
    s2 = find[i].text
    s3 = " ".join(s2.split())
    print(s3)
    i += 1
