import bs4
import requests

result = requests.get("https://finance.yahoo.com/")

soup = bs4.BeautifulSoup(result.text, "html.parser")

print(soup.select("title"))
