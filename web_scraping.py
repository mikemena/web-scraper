import bs4
import requests

basic_url = "https://books.toscrape.com/catalogue/category/books_1/page-{}.html"

# List of 4 or 5 star titles
high_rated_titles = []

# Iterate pages
for page in range(1, 51):
    url_page = basic_url.format(page)
    result = requests.get(url_page)
    soup = bs4.BeautifulSoup(result.text, "html.parser")

    # Select book data
    books = soup.select(".product_pod")

    # Iterate books
    for book in books:
        # Check if the book is 4 or 5 star rating
        if len(book.select(".star-rating.Four")) != 0 or len(
            book.select(".star-rating.Five")
        ):
            # Store book title
            book_title = book.select("a")[1]["title"]

            # Add book to list
