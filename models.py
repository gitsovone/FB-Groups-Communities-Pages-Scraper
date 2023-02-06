from pydantic import BaseModel
from typing import List, Optional, Any
from multiprocessing import Process

class Attachment(BaseModel):
    id: int = 0
    post_id: int = 0
    res_id: int = 0
    resource: int = 0
    uri: str
    text: str = ''
    type: int = 0
    page_html: str = ''
    errors: int = 0

class Post(BaseModel):
    id: int = 0
    res_id: int = 0
    resource: int = 0
    uri: str
    text: str = ''
    page_html: str = ''
    publish_time: int = 0
    attachments: List[Attachment] = []
    errors: int = 0
    tmp: int = 0

class Resource(BaseModel):
    id: int
    resource: int
    uri: str
    post_finded: int = 0
    posts_count: int = 0
    page_html: str = ''
    posts: List[Post] = []
    errors: int = 0
    activity: int = 0

class Worker(BaseModel):
    id: int
    type: str
    depth: int
    proxy: dict
    proxy_rotate_uri: str
    process: Optional[Any]
    activity: int = 180
