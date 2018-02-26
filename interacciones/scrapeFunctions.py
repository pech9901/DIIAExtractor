import urllib.request
import json
import datetime
import time


def unicode_normalize(text):
    return text.translate({0x2018: 0x27, 0x2019: 0x27, 0x201C: 0x22, 0x201D: 0x22,
                            0xa0: 0x20})


# Da formato al tiempo
def formatCreatedTime(createdTime):
    status_published = datetime.datetime.strptime(
        createdTime, '%Y-%m-%dT%H:%M:%S+0000')
    status_published = status_published + \
                       datetime.timedelta(hours=-5)
    status_published = status_published.strftime(
        '%Y-%m-%d %H:%M:%S')
    return status_published


def request_until_succeed(url):
    req = urllib.request.Request(url)
    success = False
    while success is False:
        try:
            response = urllib.request.urlopen(req)
            if response.getcode() == 200:
                success = True
        except Exception as e:
            print(e)
            time.sleep(5)
            print("Error for URL %s: %s" % (url, datetime.datetime.now()))
            print("Retrying.")
    return response.read().decode(response.headers.get_content_charset())


# Dado el ID de un grupo, devuelve un objeto JSON con los datos de su arista /feed - Conjunto de posts con sus datos
def obtieneFacebookPosts(group_id, access_token, num_statuses):
    base = "https://graph.facebook.com"
    node = "/%s/feed" % group_id
    fields = "/?fields=message,link,permalink_url,created_time,type,name,id," + \
             "comments.limit(0).summary(true),shares,reactions." + \
             "limit(0).summary(true),from"
    parameters = "&order=chronological&limit=%s&access_token=%s" % (num_statuses, access_token)
    url = base + node + fields + parameters
    data = json.loads(request_until_succeed(url))
    return data


# Dado el ID de un grupo, devuelve un objeto JSON con los datos de su arista /feed - Conjunto de posts con sus datos
def obtieneFacebookPostsIDAuthorID(group_id, access_token, num_statuses):
    base = "https://graph.facebook.com"
    node = "/%s/feed" % group_id
    fields = "/?fields=id,from"
    parameters = "&order=chronological&limit=%s&access_token=%s" % (num_statuses, access_token)
    url = base + node + fields + parameters
    data = json.loads(request_until_succeed(url))
    return data


# Dado el id de un post (o de un comentario), devuelve un objeto JSON con los datos de su arista /comments
def obtieneFacebookObjectComments(status_id, access_token, num_comments):
    base = "https://graph.facebook.com"
    node = "/%s/comments" % status_id
    fields = "?fields=id,message,like_count,created_time,comments,comment_count,from,attachment"
    parameters = "&order=chronological&limit=%s&access_token=%s" % \
                 (num_comments, access_token)
    url = base + node + fields + parameters
    data = request_until_succeed(url)
    if data is None:
        return None
    else:
        return json.loads(data)
