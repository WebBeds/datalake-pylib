# ========================= #
# TEAMS UTILITIES           #
# ========================= #

import json
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

def send_alarm(color: str, title: str, summary: str, msg: str, url: str):
    """
    Send alarm to microsoft teams by connector URL

    :param: color -> color of the upper border of the message
    :param: title -> title of the message
    :param: summary -> summary
    :param: msg -> text to be displayed in the message
    :param: url -> the url to post
    
    :return: nothing
    """

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    data = {
        "colour": color,
        "title": title,
        "summary": summary,
        "text": msg
    }

    message = {
        "@context": "https://schema.org/extensions",
        "@type": "MessageCard",
        "themeColor": data["colour"],
        "summary": data["summary"],
        "title": data["title"],
        "text": data["text"]
    }

    req = Request(url, json.dumps(message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted")
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)