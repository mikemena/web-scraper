import bs4
import requests

basic_url = "https://books.toscrape.com/catalogue/category/books_1/page-{}.html"

page_number = 1
result = requests.get(basic_url.format(page_number))

soup = bs4.BeautifulSoup(result.text, "html.parser")

books = soup.select(".product_pod")

example = books[0].select("a")[1]["title"]

print(example)
# print(books)
