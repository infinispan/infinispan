#!/usr/bin/python3
import re
import os
from urllib.request import Request, urlopen

"""
    Finds broken links in documentation. Takes ~13 minutes. Run from root infinispan directory.
"""

rootDir = 'documentation/target/generated-docs/'

def isBad(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0 Chrome/41.0.2228.0 Safari/537.36'})
    try:
        if urlopen(req).status != 200:
            return True
    except Exception as e:
        return True
    return False

def findUrls(str):
    return re.findall('<a href="(https?://[^"]*)"', str)

badUrls = 0
urls = dict() # url -> files containing that url
for dirName, subdirList, fileList in os.walk(rootDir):
    for file in fileList:
        if file.endswith('.html'):
            for url in findUrls(''.join(open(dirName + file).readlines())):
                if url.rfind('http://localhost') != -1:
                    continue
                if not urls.__contains__(url):
                    urls.update({url: set()})
                urls.get(url).add(dirName + file)

for url in urls.keys():
    if isBad(url):
        print(url, urls.get(url))
        badUrls += 1

print('Number of unique URLS checked: ', len(urls))
print('Number of bad URLs: ', badUrls)

