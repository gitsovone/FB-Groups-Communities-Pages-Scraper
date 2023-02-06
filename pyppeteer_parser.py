
import re
import time
from typing import List
import html

import mysql.connector
from pyppeteer import launch
from pyppeteer_stealth import stealth

from models import Resource, Post
from config import config
from regex import _post_url_finder_regex

class PyppeteerParser():

    def __init__(self, proxy:str):
        self.user, self.passwd, self.host, self.port = re.findall(
            r"http://([^:]+):([^@]+)@([^:]+):(.+)",
            proxy
        )[0]

    def __del__(self):
        print('exit')
        pass


    def _find_posts(self, resources:List[Resource]) -> None:

        for regex in _post_url_finder_regex:
            print(len(resources.page_html))
            urls = regex.findall(html.unescape(resources.page_html))
            for url in urls:
                resources.posts.append(
                    Post(
                        res_id = resources.id,
                        resource = resources.resource,
                        uri = url.replace('https://www', 'https://m'),
                    )
                )
                print('post added')
        resources.page_html = ''

    async def _scroll_page(self, page) -> None:

        for _ in range(3):
            await page.evaluate("""{window.scrollBy(0, 10000);}""")
            time.sleep(5)

    async def parse_pages_with_browser(self, resources:List[Resource]) -> List[Resource]:
        print('entry')
        browser = await launch({'args': [f"--proxy-server={self.host}:{self.port}", '--no-sandbox'], 'headless': True })
        page = await browser.newPage()
        # page.setDefaultNavigationTimeout(30000)

        await page.authenticate({'username': self.user, 'password': self.passwd})
        # await stealth(page)
        try:
            await page.goto(resources.uri)
            await self._scroll_page(page)
            resources.page_html = await page.content()
        except:
            pass

        await page.close()
        await browser.close()

        self._find_posts(resources)
        self._write_to_db(resources)

    def _write_to_db(self, resources:List[Resource]) -> None:

        self.mysql_db = mysql.connector.MySQLConnection(
            host = config.get('mysql_host'),
            database = config.get('mysql_database'),
            user = config.get('parser_user'),
            passwd = config.get('parser_password'),
        )
        self.mysql_cursor = self.mysql_db.cursor()


        posts_to_db = []
        for post in resources.posts:
            print(post)
            posts_to_db.append((post.res_id, post.resource, post.uri))

        if posts_to_db:
            query = "INSERT IGNORE INTO tmp_posts (res_id, resource, uri) VALUES (%s, %s, %s)"
            self.mysql_cursor.executemany(query, posts_to_db)
            self.mysql_db.commit()
        self.mysql_db.close()
