import os
import threading
import queue
import time
import psycopg2
from dotenv import load_dotenv


load_dotenv()

USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 VendrScraper/1.0")
NUM_SCRAPER_THREADS = int(os.getenv("NUM_SCRAPER_THREADS", 4))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", 0.5))

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


class DbWriter:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        self.cur = self.conn.cursor()
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name TEXT,
                category TEXT,
                price_range TEXT,
                description TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def insert_product(self, product):
        self.cur.execute("""
            INSERT INTO products (name, category, price_range, description)
            VALUES (%s, %s, %s, %s)
        """, (product['name'], product['category'], product['price_range'], product['description']))
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


class BaseParser:
    def collect_product_links_from_category(self, category_url):
        raise NotImplementedError

    def parse_product_page(self, product_url):
        raise NotImplementedError

class VendrParser(BaseParser):
    """
    Мок-версія парсера для тестування
    """
    MOCK_PRODUCTS = [
        {"slug": "devops-tool-1", "name": "DevOps Tool 1", "category": "DevOps",
         "price_range": "$50-$200", "description": "Powerful DevOps automation tool."},
        {"slug": "devops-tool-2", "name": "DevOps Tool 2", "category": "DevOps",
         "price_range": "$100-$500", "description": "Enterprise-level DevOps solution."},
        {"slug": "infra-tool-1", "name": "Infrastructure Tool 1", "category": "IT Infrastructure",
         "price_range": "$200-$1000", "description": "Robust server management tool."},
        {"slug": "analytics-tool-1", "name": "Analytics Tool 1", "category": "Data Analytics",
         "price_range": "$150-$600", "description": "Advanced analytics and reporting."},
        {"slug": "analytics-tool-2", "name": "Analytics Tool 2", "category": "Data Analytics",
         "price_range": "$300-$1200", "description": "Big data management platform."},
    ]

    CATEGORY_SLUGS = {
        "DevOps": ["devops-tool-1", "devops-tool-2"],
        "IT Infrastructure": ["infra-tool-1"],
        "Data Analytics": ["analytics-tool-1", "analytics-tool-2"]
    }

    def collect_product_links_from_category(self, category_name):
        """
        Повертає список 'URL'-подібних slug для черги завдань
        """
        slugs = self.CATEGORY_SLUGS.get(category_name, [])
        return [f"https://mock.vendr.com/product/{slug}" for slug in slugs]

    def parse_product_page(self, product_url):
        """
        Повертає словник продукту на основі slug
        """
        slug = product_url.rstrip("/").split("/")[-1]
        for p in self.MOCK_PRODUCTS:
            if p["slug"] == slug:
                return {
                    "name": p["name"],
                    "category": p["category"],
                    "price_range": p["price_range"],
                    "description": p["description"]
                }
        return {"name": "", "category": "", "price_range": "", "description": ""}


class ScraperWorker(threading.Thread):
    def __init__(self, task_queue, result_queue, parser):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.parser = parser

    def run(self):
        while True:
            try:
                url = self.task_queue.get(timeout=3)
            except queue.Empty:
                break
            try:
                product_data = self.parser.parse_product_page(url)
                self.result_queue.put(product_data)
            except Exception as e:
                print(f"Error parsing {url}: {e}")
            finally:
                self.task_queue.task_done()
                time.sleep(RATE_LIMIT_SECONDS)

class DbWriterThread(threading.Thread):
    def __init__(self, result_queue, db_writer):
        super().__init__()
        self.result_queue = result_queue
        self.db_writer = db_writer

    def run(self):
        while True:
            try:
                product = self.result_queue.get(timeout=5)
            except queue.Empty:
                break
            try:
                self.db_writer.insert_product(product)
            except Exception as e:
                print(f"DB insert error: {e}")
            finally:
                self.result_queue.task_done()


def main(test_mode=False):
    parser = VendrParser()
    task_queue = queue.Queue()
    result_queue = queue.Queue()

    categories = ["DevOps", "IT Infrastructure", "Data Analytics"]

    for cat in categories:
        links = parser.collect_product_links_from_category(cat)
        for link in links:
            task_queue.put(link)

    if test_mode:
        while not task_queue.empty():
            url = task_queue.get()
            product = parser.parse_product_page(url)
            print(product)
            task_queue.task_done()
        return

    workers = [ScraperWorker(task_queue, result_queue, parser) for _ in range(NUM_SCRAPER_THREADS)]
    for w in workers:
        w.start()

    db_writer = DbWriter()
    db_thread = DbWriterThread(result_queue, db_writer)
    db_thread.start()

    for w in workers:
        w.join()
    task_queue.join()
    result_queue.join()

    db_writer.close()
    db_thread.join()

if __name__ == "__main__":
    main(test_mode=False)
