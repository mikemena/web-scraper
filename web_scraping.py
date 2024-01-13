import bs4
import requests

ticker = input("Entet a ticker: ")

result = requests.get(f"https://finance.yahoo.com/quote/{ticker}/")

soup = bs4.BeautifulSoup(result.text, "html.parser")

# get title with tag
print(soup.select("title"))

# get title text only without tag
print(soup.select("title")[0].getText())

# count paragraphs
print(len(soup.select("p")))


# Get element with a specified attribute
current_price = soup.select('[data-test="qsp-price"]')[0].get_text()
print(current_price)


# Get element with a specified class
stock_name = soup.select('[class="D(ib) Fz(18px)"]')[0].get_text()
print(stock_name)
