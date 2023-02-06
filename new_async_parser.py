
import asyncio
import html
import time
import math
import urllib.parse
from datetime import datetime, timedelta
from typing import Union, List, Tuple
from multiprocessing import Process, Pool

import aiohttp
import requests
import mysql.connector

from config import config
from models import Worker, Resource, Post, Attachment
from regex import *
from pyppeteer_parser import PyppeteerParser as PP


class FBParser():

    def __init__(self, worker:Worker):

        self.dt = datetime.now()
        self.parse_start_time = int(datetime.timestamp(self.dt-timedelta(hours=worker.depth)))
        self.activity_period = int(datetime.timestamp(self.dt-timedelta(days=worker.activity)))
        self.worker = worker.id
        self.proxies = worker.proxy
        self.proxy_rotate_url = worker.proxy_rotate_uri
        self.timeout = 60
        self.max_requests = 200
        self.proxy_unrotate_count = 650
        self.requests_count = 650

        self.mysql_db = mysql.connector.MySQLConnection(
            host = config.get('mysql_host'),
            database = config.get('mysql_database'),
            user = config.get('parser_user'),
            passwd = config.get('parser_password'),
        )
        self.mysql_cursor = self.mysql_db.cursor()

        self.posts_exists_in_db = []
        self.db_posts = []
        self.db_attachments = []
        self.posts_exists = 0
        self.posts = 0
        self.attachments = 0
        self.groups= 0
        self.attachment_errors = 0
        self.errors = 0

    def __del__(self):

        print(datetime.now()-self.dt)
        self.mysql_db.close()

    def normalize_url(self, url:str) -> str:

        url = urllib.parse.unquote(url)
        url = html.unescape(url)
        url = url.replace('\3a', ':')
        url = url.replace('\3A', ':')
        url = url.replace('\3d', '=')
        url = url.replace('\3D', '=')
        url = url.replace('\25', '%')
        url = url.replace('\26', '&')
        url = url.replace(' ', '')
        url = url.replace('%3A', ':')
        url = url.replace('%2F', '/')
        url = url.replace('%3a', ':')
        url = url.replace('%2f', '/')
        url = url.replace('%3A', ':')
        url = url.replace('%3F', '?')
        url = url.replace('%3C', '<')
        url = url.replace('%3E', '>')
        url = url.replace('%3D', '=')
        url = url.replace('%26', '&')
        url = url.replace('\/', '/')
        url = url.replace("amp;", "")
        if ('<' in url) or ('>' in url) or ('192.168.20.1' in url):
            return ''
        return url

    def _rotate_proxy(self, requests_count:int) -> None:

        self.requests_count += requests_count
        if self.requests_count < self.proxy_unrotate_count or not self.proxy_rotate_url:
            return

        self.requests_count = requests_count
        errors = 0

        while True:
            if errors == 3:
                break
            try:
                requests.get(self.proxy_rotate_url, timeout=self.timeout)
                r = requests.get('http://icanhazip.com', proxies=self.proxies)
                if r.status_code != 200:
                    errors += 1
                    continue
                else:
                    time.sleep(2)
                    break
            except Exception as e:
                errors += 1

    async def _fetch_page(self, session:aiohttp.ClientSession, page:Union[Resource,Post,Attachment])\
        -> Union[Resource,Post,Attachment]:

        try:
            async with session.get(
                page.uri,
                proxy=self.proxies.get('http'),
                timeout=self.timeout
            ) as response:
                page.page_html = await response.text()
                page.uri = str(response.url)
        except:
            pass

        return page

    async def _load_pages(self, resources:Union[List[Resource],List[Post],List[Attachment]])\
        -> Union[List[Resource],List[Post],List[Attachment]]:

        tasks = []
        self._rotate_proxy(len(resources))

        async with aiohttp.ClientSession() as session:
            for resource in resources:
                tasks.append(self._fetch_page(session, resource))
            return await asyncio.gather(*tasks)

    def _find_posts(self, resource:Resource):

        error = True
        activity_checked = False

        content = html.unescape(resource.page_html)
        content = content.split('top_level_post_id')[1:]
        resource.page_html = ''

        for part in content:
            try:
                match = post_finder_regex.search(part)
                post_id = int(match[1])
                publish_time = int(match[2])
            except Exception as e:
                continue

            if post_id and publish_time:
                error = False

            if publish_time > self.activity_period and not activity_checked:
                resource.activity = 0
                activity_checked = True

            if publish_time > self.parse_start_time:
                resource.post_finded += 1
                post = Post(
                    id = post_id,
                    res_id = resource.id,
                    resource = resource.resource,
                    uri = f'https://m.facebook.com/{post_id}',
                    publish_time = publish_time,
                )

                if post in resource.posts:
                    continue

                if not post.id in self.posts_exists_in_db:
                    resource.posts.append(post)
                    resource.posts_count += 1
                else:
                    self.posts_exists += 1
        if error:
            resource.errors += 1
            self.errors += 1
        else:
            resource.errors = 0
            if not activity_checked:
                if resource.activity < 21:
                    resource.activity += 1

    def _splice_post_content(self, page:str, start:int, identifier:str) -> Tuple[str]:

        o_div = 0
        c_div = 0

        start = page.find(identifier, start)
        if start > 0:
            o_div += 1
        else:
            return '', ''

        t_start = start
        while o_div > c_div:
            end = page.find('</div', t_start+1)
            if end > 0:
                c_div += 1
            else:
                break

            while True:
                t_start = page.find('<div', t_start+1, end)
                if t_start > 0:
                    o_div += 1
                else:
                    break
            t_start = end

        post_page = page[start:end]

        text_area = attachments_area_finder.sub('', post_page)
        attachments_area = attachments_area_finder.search(post_page)
        attachments_area = attachments_area[0] if attachments_area else ''

        return text_area, attachments_area

    def _find_photo_url(self, post_page:str, type:int=0) -> str:

        if not type:
            photo = scontent_regex.search(post_page)
        elif type == 1:
            photo = external_regex.search(post_page)
        else:
            photo = scontent_regex.search(post_page)
            return html.unescape(photo[1]) if photo else ''

        photo = photo[1] if photo else ''

        return self.normalize_url(photo)

    def _find_photos(self, post_page:str) -> List[str]:

        photos = []

        for regex in parse_photo_regex_list:
            tmp_result = regex.findall(post_page)
            if tmp_result:
                for photo in tmp_result:
                    if 'lm.facebook' in photo:
                        continue
                    if not photo.startswith('http'):
                        photo = f"https://m.facebook.com{photo}"
                    photos.append(photo)

        return photos

    def _find_video(self, post_page:str) -> str:

        video = video_regex.search(post_page)
        video = video[1] if video else ''

        return self.normalize_url(video)

    def _find_external_attachment(self, post_page:str) -> str:

        att_1 = external_regex_1.search(post_page)
        att_1 = att_1[1] if att_1 else ''

        att_2 = external_regex_2.search(post_page)
        att_2 = att_2[1] if att_2 else ''

        if att_2 and len(att_2) < len(att_1):
            return self.normalize_url(att_2)
        return self.normalize_url(att_1)

    def _get_page_text(self, post_page:str) -> str:

        text = text_finder_regex.sub(' ', post_page)
        return ' '.join(text.split())

    def _update_post_metadata(self, post:Post) -> None:
        page = post.page_html
        page = page.split('top_level_post_id')
        if len(page) > 1:
            page = page[1]

        try:
            match = post_finder_regex.search(page)
            post.id = int(match[1])
            post.publish_time = int(match[2])
        except:
            pass

    async def _parse_posts(self, resources:Union[List[Resource], List[Post]]) -> List[Post]:

        if isinstance(resources[0], Resource):
            posts = []
            for resource in resources:
                posts.extend(resource.posts)
            posts = await self._load_pages(posts)
        else:
            posts = await self._load_pages(resources)

        for post in posts:
            if not post.id:
                self._update_post_metadata(post)
                if not post.id:
                    continue

            post.uri = f'https://www.facebook.com/{post.id}'
            post_page = html.unescape(post.page_html)
            post_page = urllib.parse.unquote(post_page)

            if len(post_page) < 10000:
                continue

            start = post_page.find('top_level_post_id')
            if start == -1:
                photo = self._find_photo_url(post_page)
                post_page, _ = self._splice_post_content(post_page, 0, '<div class="msg"')
                post.text = self._get_page_text(post_page)

                if photo:
                    post.attachments.append(
                        Attachment(
                            res_id = post.res_id,
                            resource = post.resource,
                            post_id = post.id,
                            uri = photo,
                            type = 1,
                        )
                    )
            else:
                text_area, attachments_area = self._splice_post_content(post_page, start, '<div')

                text_area = header_finder_regex.sub('', text_area)
                text_area = h3_finder_regex.sub('', text_area)
                post.text = self._get_page_text(text_area)

                attachments_area = h4_finder_regex.sub('', attachments_area)
                attachment_text = self._get_page_text(attachments_area)

                photos = self._find_photos(attachments_area)
                if photos:
                    if not post.text and attachment_text:
                        post.text = attachment_text
                        attachment_text = ''
                    elif len(photos)>1:
                        post.text += f"\n{attachment_text}"
                        attachment_text = ''

                    for photo in photos:
                        if photo:
                            post.attachments.append(
                                Attachment(
                                    res_id = post.res_id,
                                    resource = post.resource,
                                    post_id = post.id,
                                    uri = photo,
                                    text = attachment_text,
                                    type = 11,
                                )
                            )
                    attachment_text = ''

                external = self._find_external_attachment(attachments_area)
                if external:
                    post.attachments.append(
                        Attachment(
                            res_id = post.res_id,
                            resource = post.resource,
                            post_id = post.id,
                            uri = external,
                            text = attachment_text,
                            type = 3,
                        )
                    )
                video = self._find_video(attachments_area)
                if video:
                    post.attachments.append(
                        Attachment(
                            res_id = post.res_id,
                            resource = post.resource,
                            post_id = post.id,
                            uri = video,
                            text = attachment_text,
                            type = 2,
                        )
                    )
                if not post.attachments:
                    post.text += attachment_text

            post.page_html = ''

        return posts

    async def reparse_posts(self):

        self.db_posts = []
        parsed_posts = []

        self.mysql_cursor.execute("SELECT post_id, res_id, resource, errors FROM posts WHERE status = 0 AND errors < 10")

        posts_to_parse = [
            Post(
                id = post_id,
                res_id = res_id,
                resource = resource,
                uri = f"https://m.facebook.com/{post_id}",
                errors = errors,
            )
            for post_id, res_id, resource, errors in self.mysql_cursor.fetchall()
        ]

        if not posts_to_parse:
            return

        for i in range(math.ceil(len(posts_to_parse)/self.max_requests)):
            posts = await self._parse_posts(posts_to_parse[i*self.max_requests:(i+1)*self.max_requests])
            if posts:
                parsed_posts.extend(posts)

        attachments = [att for post in parsed_posts for att in post.attachments]
        for i in range(math.ceil(len(attachments)/self.max_requests)):
            await self._parse_attachments(attachments[i*self.max_requests:(i+1)*self.max_requests])

        attachments = [(att.res_id, att.resource, att.post_id, att.uri, att.text, att.type, att.errors) for post in parsed_posts for att in post.attachments]

        self._update_posts(parsed_posts)
        self._write_to_db(attachments=attachments)

    async def _parse_attachments(self, posts:Union[List[Post], List[Attachment]]) -> List[Attachment]:

        if isinstance(posts[0], Post):
            attachments = []
            for post in posts:
                for attachment in post.attachments:
                    if attachment.type == 11:
                        attachments.append(attachment)
            attachments = await self._load_pages(attachments)
        else:
            attachments = [att for att in posts if att.type == 11]
            attachments = await self._load_pages(attachments)

        for attachment in attachments:
            url = self._find_photo_url(attachment.page_html)
            attachment.page_html = ''
            if url:
                attachment.uri = url
                attachment.type = 1
                attachment.errors = 0
            else:
                attachment.errors += 1
        return attachments

    async def reparse_attachments(self):

        parsed_attachments = []
        self.mysql_cursor.execute("SELECT id, uri, errors FROM attachments WHERE type = 11 AND errors < 10")

        attachments_to_parse = [
            Attachment(
                id = id,
                uri = uri,
                type = 11,
                errors = errors,
            )
            for id, uri, errors in self.mysql_cursor.fetchall()
        ]
        if not attachments_to_parse:
            return 0

        for i in range(math.ceil(len(attachments_to_parse)/self.max_requests)):
            attachments = await self._parse_attachments(attachments_to_parse[i*self.max_requests:(i+1)*self.max_requests])
            if attachments:
                parsed_attachments.extend(attachments)

        self._update_attachments(parsed_attachments)

    async def parse_groups(self):

        parsed_resources = []

        query= "SELECT post_id FROM posts WHERE nd_date > %s"
        self.mysql_cursor.execute(query, (self.parse_start_time,))

        self.posts_exists_in_db = [post_id[0] for post_id in self.mysql_cursor.fetchall()]
        self.posts_exists_in_db = set(self.posts_exists_in_db)

        for cycle in range(1, 3):

            parsed_resources = []
            posts_count = 0

            activity = 20
            if self.dt.hour < 8:
                activity += 1

            query = "SELECT res_id, resource, uri, posts_count, activity, errors FROM resource_social WHERE stability = 1 AND type = %s AND activity <= %s AND worker = %s AND errors < 500"

            self.mysql_cursor.execute(query, (cycle, activity, self.worker))

            group_resources = [
                Resource(
                    id = res_id,
                    resource = resource,
                    uri = uri,
                    posts_count = posts_count,
                    activity = activity,
                    errors = errors,
                )
                for res_id, resource, uri, posts_count, activity, errors in self.mysql_cursor.fetchall()
            ]
            self.groups += len(group_resources)

            for i in range(math.ceil(len(group_resources)/self.max_requests)):
                resources = await self._load_pages(group_resources[i*self.max_requests:(i+1)*self.max_requests])

                for resource in resources:
                    posts_count -= resource.posts_count
                    self._find_posts(resource)
                    posts_count += resource.posts_count
                    parsed_resources.append(resource)

                    if posts_count > self.max_requests:
                        posts_to_parse = [post for resource in parsed_resources for post in resource.posts]
                        await self._parse_posts(posts_to_parse)

                        attachments = [att for post in posts_to_parse for att in post.attachments]
                        for i in range(math.ceil(len(attachments)/self.max_requests)):
                            await self._parse_attachments(attachments[i*self.max_requests:(i+1)*self.max_requests])

                        self._write_to_db(parsed_resources)

                        parsed_resources = []
                        posts_count = 0

            posts_to_parse = [post for resource in parsed_resources for post in resource.posts]
            if posts_to_parse:
                await self._parse_posts(posts_to_parse)

                attachments = [att for post in posts_to_parse for att in post.attachments]
                for i in range(math.ceil(len(attachments)/self.max_requests)):
                    await self._parse_attachments(attachments[i*self.max_requests:(i+1)*self.max_requests])

                self._write_to_db(parsed_resources)

    async def find_communities(self) -> None:

        parsed_resources = []
        unparsed_resources = []

        query = "SELECT id, resource, errors FROM resource_social WHERE type = 0 AND stability = 1 AND country_id != 57 AND errors < 10"
        self.mysql_cursor.execute(query)

        resources_to_parse = [
            Resource(
                id = id,
                resource = resource,
                uri = f"https://m.facebook.com/page_content_list_view/more/?page_id={resource}&start_cursor=7&num_to_fetch=10&surface_type=timeline",
                errors = errors,
            )
            for id, resource, errors in self.mysql_cursor.fetchall()
        ]

        # print(len(resources_to_parse))
        for i in range(math.ceil(len(resources_to_parse)/self.max_requests)):
            resources = await self._load_pages(resources_to_parse[i*self.max_requests:(i+1)*self.max_requests])
            for resource in resources:
                self._find_posts(resource)
                if resource.post_finded:
                    print(resource.post_finded)
                    parsed_resources.append((resource.uri, resource.id))
                else:
                    unparsed_resources.append((resource.errors+1, resource.id))
                resource.page_html = ''
                resource.posts = []

        # print(len(parsed_resources))
        # print(len(unparsed_resources))
        if parsed_resources:
            query = "UPDATE resource_social SET type = 2, uri = %s, errors = 0 WHERE id = %s"
            self.mysql_cursor.executemany(query, parsed_resources)
            self.mysql_db.commit()

        if unparsed_resources:
            query = "UPDATE resource_social SET errors = %s WHERE id = %s"
            self.mysql_cursor.executemany(query, unparsed_resources)
            self.mysql_db.commit()

    async def clean_communities(self) -> None:

        parsed_resources = []
        unparsed_resources = []

        query = "SELECT id, resource, errors FROM resource_social WHERE type = 2 AND errors < 3"
        self.mysql_cursor.execute(query)

        resources_to_parse = [
            Resource(
                id = id,
                resource = resource,
                uri = f"https://m.facebook.com/{resource}",
                errors = errors,
            )
            for id, resource, errors in self.mysql_cursor.fetchall()
        ]

        for i in range(math.ceil(len(resources_to_parse)/self.max_requests)):
            resources = await self._load_pages(resources_to_parse[i*self.max_requests:(i+1)*self.max_requests])
            for resource in resources:
                if resource.uri.startswith('https://www.facebook.com'):
                    parsed_resources.append((resource.uri, resource.id))
                else:
                    unparsed_resources.append((resource.errors+1, resource.id))
                resource.page_html = ''

        if parsed_resources:
            query = "UPDATE resource_social SET type = 3, uri = %s WHERE id = %s"
            self.mysql_cursor.executemany(query, parsed_resources)
            self.mysql_db.commit()

        if unparsed_resources:
            query = "UPDATE resource_social SET errors = %s WHERE id = %s"
            self.mysql_cursor.executemany(query, unparsed_resources)
            self.mysql_db.commit()

    async def find_groups(self) -> None:

        parsed_resources = []
        unparsed_resources = []

        query = "SELECT id, resource, errors FROM resource_social WHERE type = 0 AND country_id != 57 AND stability = 1 AND errors < 10"
        self.mysql_cursor.execute(query)

        resources_to_parse = [
            Resource(
                id = id,
                resource = resource,
                uri = f'https://m.facebook.com/{resource}',
                errors = errors,
            )
            for id, resource, errors in self.mysql_cursor.fetchall()
        ]

        for i in range(math.ceil(len(resources_to_parse)/self.max_requests)):
            resources = await self._load_pages(resources_to_parse[i*self.max_requests:(i+1)*self.max_requests])
            for resource in resources:
                if resource.uri.startswith('https://m.facebook.com/groups/'):
                    self._find_posts(resource)
                    if resource.post_finded:
                        resource.uri = resource.uri.replace('/?_rdr', '')
                        print(resource.post_finded)
                        parsed_resources.append((resource.uri, resource.id))
                    else:
                        unparsed_resources.append((resource.errors+1, resource.id))
                resource.page_html = ''
                resource.posts = []

        if parsed_resources:
            query = "UPDATE resource_social SET type = 1, uri = %s, errors = 0 WHERE id = %s"
            self.mysql_cursor.executemany(query, parsed_resources)
            self.mysql_db.commit()

        if unparsed_resources:
            query = "UPDATE resource_social SET errors = %s WHERE id = %s"
            self.mysql_cursor.executemany(query, unparsed_resources)
            self.mysql_db.commit()

    async def _parse_browser_posts(self):

        self.db_posts = []
        parsed_posts = []

        self.mysql_cursor.execute("SELECT id, res_id, resource, uri, errors FROM tmp_posts WHERE status = 0 AND errors < 10 LIMIT 100")

        posts_to_parse = [
            Post(
                res_id = res_id,
                resource = resource,
                uri = uri,
                errors = errors,
                tmp = id,
            )
            for id, res_id, resource, uri, errors in self.mysql_cursor.fetchall()
        ]

        if not posts_to_parse:
            return

        for i in range(math.ceil(len(posts_to_parse)/self.max_requests)):
            posts = await self._parse_posts(posts_to_parse[i*self.max_requests:(i+1)*self.max_requests])
            if posts:
                parsed_posts.extend(posts)

        attachments = [att for post in parsed_posts for att in post.attachments]
        for i in range(math.ceil(len(attachments)/self.max_requests)):
            await self._parse_attachments(attachments[i*self.max_requests:(i+1)*self.max_requests])


        posts = [(11, post.res_id, post.resource, post.id, post.text, post.publish_time, f"{datetime.fromtimestamp(post.publish_time):%Y-%m-%d}", post.uri, post.errors) for post in parsed_posts if ((post.text or post.attachments) and not post.text.startswith("Этот контент сейчас недоступен"))]
        attachments = [(attachment.res_id, attachment.resource, attachment.post_id, attachment.uri, attachment.text, attachment.type+20, attachment.errors) for post in parsed_posts for attachment in post.attachments]
        self._write_to_db(posts=posts, attachments=attachments)

        posts = [(1, 0, post.id) if ((post.text or post.attachments) and not post.text.startswith("Этот контент сейчас недоступен")) else (0, post.errors+1, post.id) for post in parsed_posts]

        query = "UPDATE tmp_posts SET status = %s, errors = %s WHERE id = %s"
        self.mysql_cursor.executemany(query, posts)
        self.mysql_db.commit()

    def _create_process(self, resources:List[Resource]):
        p_parser = PP(self.proxies.get('http'))
        asyncio.get_event_loop().run_until_complete(p_parser.parse_pages_with_browser(resources))

    def parse_with_browser(self) -> None:

        query = "SELECT res_id, resource, uri, errors FROM resources WHERE type = 3 AND worker = %s AND errors < 100"
        self.mysql_cursor.execute(query, (self.worker,))

        group_resources = [
            Resource(
                id = res_id,
                resource = resource,
                uri = uri,
                errors = errors,
            )
            for res_id, resource, uri, errors in self.mysql_cursor.fetchall()
        ]

        for i in range(math.ceil(len(group_resources)/self.proxy_unrotate_count)):
            print('                               Cycle ', i+1)
            self._rotate_proxy(660)
            resources = group_resources[i*self.proxy_unrotate_count:(i+1)*self.proxy_unrotate_count]
            count = 0
            total = len(resources)
            procs = []
            for i in range(10):
                resources_to_parse = resources[count:count+10]
                count += 10
                p = Process(target=self._create_process, args=(resources_to_parse,))
                procs.append(p)
                p.start()
            done = False
            while True:
                if done:
                    break
                for proc in procs:
                    if not proc.is_alive():
                        if count < total:
                            resources_to_parse = resources[count:count+10]
                            count += 10
                            procs.remove(proc)
                            proc = Process(target=self._create_process, args=(resources_to_parse,))
                            procs.append(proc)
                            proc.start()
                        else:
                            for proc in procs:
                                proc.join()
                            done = True
                time.sleep(2)

    def pool_with_browser(self) -> None:

        print('pool')
        query = "SELECT res_id, resource, uri, errors FROM resource_social WHERE type = 3 AND worker = 0 AND errors < 100"
        self.mysql_cursor.execute(query)

        group_resources = [
            Resource(
                id = res_id,
                resource = resource,
                uri = uri,
                errors = errors,
            )
            for res_id, resource, uri, errors in self.mysql_cursor.fetchall()
        ]
        print(len(group_resources))
        for i in range(math.ceil(len(group_resources)/self.proxy_unrotate_count)):
            print('                               Cycle ', i+1)
            print('                               Cycle ', i+1)
            print('                               Cycle ', i+1)
            self._rotate_proxy(660)
            resources = group_resources[i*self.proxy_unrotate_count:(i+1)*self.proxy_unrotate_count]
            print(len(resources))
            with Pool(8) as pool:
                pool.map(self._create_process, resources)

    def write_stat(self) -> None:
        query = "INSERT INTO statistic (groups_checked, groups_parse_errors, new_posts, posts_exists, attachments, time_elapsed) VALUES (%s, %s, %s, %s, %s, %s)"
        time_elapsed = str(datetime.now()-self.dt).split('.')[0]
        data = (self.groups, self.errors, self.posts, self.posts_exists, self.attachments, time_elapsed)
        self.mysql_cursor.execute(query, data)
        self.mysql_db.commit()
        try:
            stat = f"Groups checked: {self.groups} Unparsed: {self.errors}\nPosts added: {self.posts} exists: {self.posts_exists}\nAttachments added: {self.attachments}\nTime elapsed: {datetime.now()-self.dt}\n{datetime.now()}\n\n"
            with open('/home/job-user/fb_parser/stat.txt', 'a') as f:
                f.write(stat)
        except:
            pass

    def _write_to_db(self, parsed_resources:List[Resource]=[], posts:list=None, attachments:list=None) -> None:

        resources_update_data = []
        posts = []

        if not attachments:
            attachments = []

        for res in parsed_resources:
            resources_update_data.append((res.posts_count, res.activity, res.errors, res.id))
            for post in res.posts:
                added = False
                status = 0
                for att in post.attachments:
                    attachments.append((att.res_id, att.resource, att.post_id, att.uri, att.text, att.type, att.errors))
                    if att.type == 11:
                        self.attachment_errors += 1
                    added = True
                if post.text or added:
                    if not post.text.startswith("Этот контент сейчас недоступен"):
                        status = 1
                    elif post.text.startswith("Этот контент сейчас недоступен"):
                        post.errors = 10

                dt = f"{datetime.fromtimestamp(post.publish_time):%Y-%m-%d}"
                posts.append((status, post.res_id, post.resource, post.id, post.text.strip(), post.publish_time, dt, post.uri, post.errors))

        if posts:
            self.posts += len(posts)
            print('    posts to db:', len(posts))
            query = "INSERT IGNORE INTO posts (status, res_id, resource, post_id, text, nd_date, not_date, uri, errors) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

            for i in range(math.ceil(len(posts)/4000)):
                self.mysql_cursor.executemany(query, posts[i*4000:(i+1)*4000])
                self.mysql_db.commit()

        if attachments:
            self.attachments += len(attachments)
            print("    atts to db:", len(attachments))
            query = "INSERT INTO attachments (res_id, resource, post_id, uri, text, type, errors) VALUES (%s, %s, %s, %s, %s, %s, %s)"

            for i in range(math.ceil(len(attachments)/4000)):
                self.mysql_cursor.executemany(query, attachments[i*4000:(i+1)*4000])
                self.mysql_db.commit()

        if resources_update_data:
            query = "UPDATE resource_social SET posts_count = %s, activity = %s, errors = %s WHERE res_id = %s"
            self.mysql_cursor.executemany(query, resources_update_data)
            self.mysql_db.commit()

    def _update_posts(self, posts:List[Post]):

        updated_posts = [(post.text, post.id) for post in posts if ((post.text or post.attachments) and not post.text.startswith("Этот контент сейчас недоступен"))]
        if updated_posts:
            query = "UPDATE posts SET status = 1, errors = 0, text = %s WHERE post_id = %s"
            self.mysql_cursor.executemany(query, updated_posts)
            self.mysql_db.commit()

        error_posts = [(post.errors+1, post.id) for post in posts if not(post.text or post.attachments)]
        not_available_posts = [(10, post.id) for post in posts if post.text.startswith("Этот контент сейчас недоступен")]
        error_posts.extend(not_available_posts)
        if error_posts:
            query = "UPDATE posts SET errors = %s WHERE post_id = %s"
            self.mysql_cursor.executemany(query, error_posts)
            self.mysql_db.commit()

    def _update_attachments(self, attachments):

        updated_attachments = [(attachment.uri, attachment.id) for attachment in attachments if attachment.type == 1]
        if updated_attachments:
            query = "UPDATE attachments SET type = 1, errors = 0, uri = %s WHERE id = %s"
            self.mysql_cursor.executemany(query, updated_attachments)
            self.mysql_db.commit()

        error_attachments = [(attachment.errors+1, attachment.id) for attachment in attachments if attachment.type == 11]
        if error_attachments:
            query = "UPDATE attachments SET errors = %s WHERE id = %s"
            self.mysql_cursor.executemany(query, error_attachments)
            self.mysql_db.commit()
