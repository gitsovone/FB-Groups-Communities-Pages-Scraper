
import re


text_finder_regex = re.compile(r'<.+?>')
header_finder_regex = re.compile(r'<header.+?</header>')
h3_finder_regex = re.compile(r'<h3.+?</h3>')
h4_finder_regex = re.compile(r'<h4.+?</h4>')
post_finder_regex = re.compile('\D+(?P<post_id>\d+).+publish_time\D+(?P<publish_time>\d+)')
attachments_area_finder = re.compile(r'<div class[^<>]+tn":"H.+>')
external_regex_1 = re.compile(r'lm.facebook.+?(http.+?)"')
external_regex_2 = re.compile(r'lm.facebook.+?(http.+?)&')
video_regex = re.compile(r'video_redirect.+?(http.+?)"')
scontent_regex = re.compile(r'"([^"]+scontent.+?)"')
external_regex = re.compile(r'"([^"]+external.+?)"')
parse_photo_regex_list = (
    re.compile(r'href="([^"]*/photos/.+?)"'),
    re.compile(r'href="([^"]+photo=.+?)&'),
    re.compile(r'href="([^"]+photo.php.+?)&'),
)

_post_url_finder_regex = [
    re.compile(r'href="([^"]+/posts/.+?)\?'),
    re.compile(r'href="([^"]+story_fbid.+?&.+?)\&'),
]
