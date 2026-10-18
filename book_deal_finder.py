from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from functools import wraps
from pydantic import BaseModel, PositiveFloat, Field
import asyncio
import aiohttp
import pandas as pd


class Book(BaseModel):
    title: str
    price: PositiveFloat
    rating: int = Field(..., ge=1, le=5)
class BaseScraper(ABC):
    def __init__(self):
        pass

    

    @abstractmethod
    def fetch(self, url):
        pass

    @abstractmethod
    def parse(self, html):
        pass
# retry exponetial decorator
def async_retry_decorater(max_attempt: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempt + 1):
                try:
                    return await func(*args, **kwargs)
                except aiohttp.ClientResponseError as e:
                    
                    if e.status is not None and 400 <= e.status < 500 and e.status != 429:
                        raise  
                    print(f"Attempt {attempt} failed: {e}")
                    if attempt == max_attempt:
                        raise
                    await asyncio.sleep(delay * (backoff ** (attempt - 1)))
                except aiohttp.ClientError as e:
                    print(f"Attempt {attempt} failed: {e}")
                    if attempt == max_attempt:
                        raise
                    await asyncio.sleep(delay * (backoff ** (attempt - 1)))
        return wrapper
    return decorator
class BooksToScrapeScraper(BaseScraper):
    BASE_URL = "https://books.toscrape.com/"
    PAGE_URL = "https://books.toscrape.com/catalogue/page-{}.html"

    def __init__(self):
        super().__init__()
        self.session = None
    
                
    @async_retry_decorater()
    async def fetch(self, url):
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.text()

    def parse(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        for book in soup.select('article.product_pod'):
            yield book
    
    async def fetch_all_pages(self, max_pages=50):
        urls = [self.BASE_URL] + [self.PAGE_URL.format(n) for n in range(2, max_pages + 1)]
        sem = asyncio.Semaphore(10)

        async def bounded_fetch(url):
            async with sem:
                return await self.fetch(url)

        async with aiohttp.ClientSession() as session:
            self.session = session
            htmls = await asyncio.gather(*(bounded_fetch(u) for u in urls), return_exceptions=True)
        self.session = None

        books = []
        for html in htmls:
            if isinstance(html, Exception):
                print(f"Skipping a page that failed after retries: {html}")
                continue
            books.extend(self.parse(html))
        return books

def extract_book(book) -> dict:
    title = book.h3.a["title"]
    price = float(book.select_one('p.price_color').text.split('£')[1])
    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    rating = rating_map[book.select_one('p.star-rating')['class'][1]]
    
    return Book(title=title, price=price, rating=rating)


scraper = BooksToScrapeScraper()

tags = asyncio.run(scraper.fetch_all_pages())
df = pd.DataFrame([ extract_book(t).model_dump() for t in tags])
condition = (df['price']<25) & (df['rating']>=4)
deals = df[condition]

deals_df = deals.copy()



deals_df['value_score'] = deals_df['rating'] / deals_df['price']   
deals_df = deals_df.sort_values('value_score', ascending=False)

deals_df.to_csv("deals.csv", index=False)       

print(len(df), len(deals_df))
print(deals_df[['title','price', 'rating', 'value_score']].head(10))