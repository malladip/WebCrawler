
__author__ = 'Prashant'

import pprint

import re
import json
import ssl
import time
import urllib
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
import urllib.robotparser
import socket

es = Elasticsearch("localhost:9200",timeout = 600,max_retries = 2,revival_delay = 0)

timeout = 5
socket.setdefaulttimeout(timeout)

formats_to_remove = ['.jpg', '.JPG', '.png', '.PNG', '.ogg', '.ogv', 'mp4', '.tif', '.tiff', '.gif', '.jpeg', '.jif', '.jfif',
                     '.jp2', '.jpx', '.j2k', '.j2c', 'fpx', '.pcd', '.pdf']

urls = ["http://en.wikipedia.org/wiki/Climate_change",
        "https://www.google.com/search?client=safari&rls=en&q=CLIMATE+CHANGE&ie=UTF-8&oe=UTF-8#q=CLIMATE+CHANGE&safe=off&rls=en&tbm=nws",
        "http://www.nasa.gov/mission_pages/noaa-n/climate/climate_weather.html#.VP9bRyklBYw",
        "https://nsidc.org/cryosphere/arctic-meteorology/climate_vs_weather.html",
        "http://en.wikipedia.org/wiki/Weather_and_climate"]
crawl_dict = {}

crawler_count = 0
links_visited = {}
in_links_yet_to_visit = {}
children_yet_to_visit = set()


def write_object(data_dict):
    file = "prash"+str(crawler_count)
    with open(file, "w+") as fc:
        json.dump(data_dict,fc)

def remove_unwanted_data(soup):
    for script in soup(["script", "style", "noscript", "nav", "footer", "title"]):
        script.extract()

    nav = soup.find('div', id="jump-to-nav")
    if nav:
        nav.extract()

    share = soup.find('div', id="shareFooterSub")
    if share:
        share.extract()

    site_nav = soup.find('div', id="siteNavCategories")
    if site_nav:
        site_nav.extract()

    site_nav = soup.find('div', id="siteNavMore")
    if site_nav:
        site_nav.extract()

    tablet = soup.find('div', id="portraitMess")
    if tablet:
        tablet.extract()

    share_box = soup.find('div', id="shareBox")
    if share_box:
        share_box.extract()

    site_head = soup.find('header', id="siteHead")
    if site_head:
        site_head.extract()

    footer = soup.find('div', id="footer")
    if footer:
        footer.extract()

    head = soup.find('div', id="mw-head")
    if head:
        head.extract()

    panel = soup.find('div', id="mw-panel")
    if panel:
        panel.extract()

    nav = soup.find('div', id="mw-navigation")
    if nav:
        nav.extract()

    footer = soup.find('div', class_="printfooter")
    if footer:
        footer.extract()

    footer = soup.find('footer')
    if footer:
        footer.extract()

    for script in soup.find_all('span', class_="mw-editsection"):
        script.extract()

    return soup


def build_document(link,title,text):
    document = "<DOC>\n"
    document = document + '<DOCNO> ' + link + ' </DOCNO>\n'
    document = document + '<HEAD>'+title+'</HEAD>\n'
    document = document + '<TEXT>\n'+ text + '\n' + '</TEXT>\n' + '</DOC>\n'
    return document

def write_to_file(doc):
    with open("documents", 'a+') as outfile:
        outfile.write(doc)

def clean_url(url):
    parsed = list(urllib.parse.urlparse(url))
    parsed[0] = parsed[0].lower()
    parsed[1] = parsed[1].lower()
    parsed[2] = re.sub("/{2,}", "/", parsed[2])  #remove 2 or more slashes
    cleaned = urllib.parse.urlunparse(parsed)
    return cleaned

def remove_portnum(url):
    result = urllib.parse.urlparse(url)
    try:
        port = result.port
    except:
        return url
    if port is not None:
        port = str(result.port)
        new_url = url.strip(port).strip(':')
        return new_url
    else:
        return url

def canonicaliz(parent, child):
    child = clean_url(child)
    child = remove_portnum(child)
    new_url = urllib.parse.urljoin(parent, child)
    url_object = urllib.parse.urlparse(new_url)
    canonicalized_url = url_object.scheme + "://" + url_object.netloc + url_object.path
    return canonicalized_url

def get_children_yet_to_visit(soup,parent):
    out_links = set()
    anchors = soup.find_all('a')
    for link in anchors:

        child = link.get('href')
        if child is None: continue
        if '#' in child: continue
        if any(format in child for format in formats_to_remove):
            continue
        child = canonicaliz(parent, child)
        child = child.encode('utf-8', 'ignore')
        child = child.decode("utf-8")
        out_links.add(child)
        if child not in links_visited:
            children_yet_to_visit.add(child)
            if child in in_links_yet_to_visit:
                in_links_yet_to_visit[child].add(parent)
            else:
                in_links_yet_to_visit[child] = set()
                in_links_yet_to_visit[child].add(parent)
        else:
            links_visited[child].add(parent)
    return out_links

def write_data_to_file(crawler_count,url,crawling_links):

    links_visited_file = {}
    for key in links_visited:
        links_visited_file[key] = list(links_visited[key])

    in_links_yet_to_visit_file = {}
    for key in links_visited:
        in_links_yet_to_visit_file[key] = list(links_visited[key])

    children_yet_to_visit_file = list(children_yet_to_visit)

    with open("backup_links_visited.txt", "w+") as fc:
        json.dump(links_visited_file,fc)
    with open("backup_in_links_yet_to_visit.txt", "w+") as fc:
        json.dump(in_links_yet_to_visit_file,fc)
    with open("backup_children_yet_to_visit.txt", "w+") as fc:
        json.dump(children_yet_to_visit_file,fc)
    dict = {"count":crawler_count,"current":url,"links":crawling_links}
    with open("backup_other.txt", "w+") as fc:
        json.dump(dict,fc)


b_count = 0
f_count = 0
import re
import string
file_data_dict = {}
def fill_crawl_dictionary(url,title,text,inlinks,outlinks,raw_html,http_header,crawler_count,crawling_links):
    global f_count
    global b_count
    f_count = f_count + 1

    title = filter(lambda x: x in string.printable, title)
    title = ''.join(title)
    text = filter(lambda x: x in string.printable, text)
    text = ''.join(text)
    raw_html = filter(lambda x: x in string.printable, raw_html)
    raw_html = ''.join(raw_html)
    http_header = filter(lambda x: x in string.printable, http_header)
    http_header = ''.join(http_header)


    file_data_dict[url] = (title,text,raw_html,http_header)
    if f_count == 10:
        write_object(file_data_dict)
        file_data_dict.clear()
        with open("prash_in_out_links.txt", "w+") as fc:
            json.dump(crawl_dict,fc)
        f_count = 0

    inlinks = list(inlinks)
    outlinks = list(outlinks)

    if url in crawl_dict:
        crawl_dict[url][0] = set(crawl_dict[url][0]) | set(inlinks)
        crawl_dict[url][0] = list(crawl_dict[url][0])
        crawl_dict[url][1] = set(crawl_dict[url][1]) | set(outlinks)
        crawl_dict[url][1] = list(crawl_dict[url][1])
    else:
        crawl_dict[url] = (inlinks,outlinks)

    global b_count
    b_count = b_count + 1
    if b_count == 100:
        write_data_to_file(crawler_count,url,crawling_links)
        b_count = 0

    # if es.exists(index="test_index_skpr", doc_type="document", id=url):
    #     doc = es.get(index="test_index_skpr", doc_type="document", id=url)
    #     new_in_links = set(inlinks) | set(doc['_source']['in_links'])
    #     new_out_links = set(outlinks) | set(doc['_source']['out_links'])
    #     es.update(index="test_index_skpr",
    #               doc_type="document",
    #               id=url,
    #               body={"doc": {"in_links": list(new_in_links), "out_links": list(new_out_links)}})
    # else:
    #     contents = {'docno':url ,'HTTPheader': http_header,'title': title, 'text': text,'html_Source':raw_html, 'in_links': list(inlinks), 'out_links': list(outlinks),'author':'prashant'}
    #     es.index(index="test_index_skpr", doc_type="document", id=url, body=contents)

    # inlinks = list(inlinks)
    # outlinks = list(outlinks)
    # if id in crawl_dict:
    #     crawl_dict[id]["inlinks"] = set(crawl_dict[id]["inlinks"]) | set(inlinks)
    #     crawl_dict[id]["inlinks"] = list(crawl_dict[id]["inlinks"])
    #     crawl_dict[id]["outlinks"] = set(crawl_dict[id]["outlinks"]) | set(inlinks)
    #     crawl_dict[id]["outlinks"] = list(crawl_dict[id]["outlinks"])
    # else:
    #     crawl_dict[id] = {"title":title,"text":text,"inlinks":inlinks,"outlinks":outlinks}

def crawl():
    # global crawling_links_outer
    crawling_links = urls[:]
    # if True:
    #     crawling_links = crawling_links_outer[:]
    # print(crawling_links_outer)
    global crawler_count

    while crawler_count <= 20000:
        for link in crawling_links:

            domain = urllib.parse.urlparse(link).netloc
            scheme = urllib.parse.urlparse(link).scheme
            rparse = urllib.robotparser.RobotFileParser()
            rparse.set_url(scheme+"://"+domain+"/robots.txt")

            try:
                rparse.read()

            except IOError:
                continue
            except UnicodeError:
                continue
            except ssl.CertificateError:
                continue
            except:
                continue

            allows_fetch = rparse.can_fetch('*', link)
            if allows_fetch:
                print("sleep")
                time.sleep(1)
                try:
                    response = urllib.request.urlopen(link,timeout = 5)
                except IOError:
                    continue
                except UnicodeError:
                    continue
                except:
                    continue

                try:
                    http_info = response.info()
                    http_header = str(http_info)

                    language = http_info.get("Content-language")
                    type = http_info.get("Content-Type")
                except: continue

                if language and type is not None:
                    if "en" in language and "text" in type:
                        try :
                            handle = response.read()
                            soup = BeautifulSoup(handle)
                            raw_html = str(soup)
                        except:
                            continue
                        try:
                            title = soup.title.string
                            title = title.encode('utf-8', 'ignore')
                            title = title.decode("utf-8")
                            title = str(title)
                        except AttributeError:
                            title = ""
                        except:
                            title = ""
                        soup = remove_unwanted_data(soup)

                        # text_data = soup.get_text()
                        # lines = text_data.splitlines()
                        # text = ""
                        # for line in lines:
                        #     if line:
                        #         line = line.encode("utf-8")
                        #         line = line.decode("utf-8")
                        #         text = text + line + '\n'

                        text = soup.get_text()
                        text = text.replace("\n", " ")
                        lines = (line.strip() for line in text.splitlines())
                        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                        text = '\n'.join(chunk for chunk in chunks if chunk)
                        text = text.encode('utf-8', 'ignore')
                        text = text.decode('utf-8')

                        if link in in_links_yet_to_visit:
                            links_visited[link] = in_links_yet_to_visit[link]
                        else:
                            links_visited[link] = set()

                        out_links = get_children_yet_to_visit(soup,link)
                        fill_crawl_dictionary(link,title,text,links_visited[link],out_links,raw_html,http_header,crawler_count,crawling_links)
                        crawler_count += 1
                        print(crawler_count)
                        print(link)
                        if crawler_count > 20000:
                            break
        next_links = sorted(children_yet_to_visit, key=lambda s: len(in_links_yet_to_visit[s]), reverse=True)
        if not next_links: break
        del crawling_links[:]
        crawling_links = next_links[:]
        children_yet_to_visit.clear()
crawling_links_outer = []
def get_data_from_file():
    global crawler_count
    global crawling_links_outer
    global children_yet_to_visit
    with open("links_visited.txt", "r") as c:
        links_visited_file = json.load(c)
    with open("in_links_yet_to_visit.txt", "r") as c:
        in_links_yet_to_visit_file = json.load(c)
    with open("children_yet_to_visit.txt", "r") as c:
        children_yet_to_visit_file = json.load(c)
    with open("other.txt", "r") as c:
        dict = json.load(c)

    links_visited_file = {}
    for key in links_visited_file:
        links_visited[key] = set(links_visited_file[key])
    in_links_yet_to_visit_file = {}
    for key in in_links_yet_to_visit_file:
        in_links_yet_to_visit[key] = set(in_links_yet_to_visit_file[key])
    children_yet_to_visit = set(children_yet_to_visit_file)
    crawler_count = dict["count"]
    crawling_links_outer = dict["links"][:]

# print(crawling_links_outer)
# get_data_from_file()

crawl()

with open("in_out_links.txt", "w+") as fc:
    json.dump(crawl_dict,fc)

###################################################Reading
# with open("in_out_links.txt", "r") as c:
#     in_out = json.load(c)
# import glob
# files = glob.glob("temp/*")
# for filename in files:
#     with open(filename, "r+") as fc:
#         f = json.load(fc)
#         print("--------------------------------------------------------")
#         print(f["text"])