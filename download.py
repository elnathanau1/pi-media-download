import concurrent.futures
import csv
import json
import sys
from os import path as ospath
import os
from pathlib import Path
from bs4 import BeautifulSoup

import requests
import re
from clint.textui import progress
from retrying import retry

SEASON_ENDPOINT = "https://aultima-api-flask.herokuapp.com/get/season/episodes"
EPISODE_ENDPOINT = "https://aultima-api-flask.herokuapp.com/get/episode"
DOWNLOAD_ROOT = "/Users/eau/Documents/Development/aultima-api-flask/temp/"


# @retry(stop_max_attempt_number=5, wait_random_min=1000, wait_random_max=4000)
def get_download_link(url):
    try:
        print("Retrieving download link for %s" % url)
        ep_page = requests.get(url)
        soup = BeautifulSoup(ep_page.content, 'html.parser')
        active = soup.find("div", {"class": "part active"})
        video_url = 'https:' + active['data-video']

        video_page = requests.get(video_url)
        pattern = 'file: \'(.*?)\''
        a = re.search(pattern, video_page.text)
        return a.group(1)

    except Exception as e:
        print("Failed to get download link for %s" % url)
        print(e)


def download_show(show_name, season, aultima_show_url):
    # create download folder if doesn't exist
    download_location = DOWNLOAD_ROOT + show_name + '/Season ' + season + '/'
    print("Creating %s if not exist" % download_location)
    Path(download_location).mkdir(parents=True, exist_ok=True)

    # getting episodes in season
    print("Making /season API call")
    r = requests.get(aultima_show_url)
    soup = BeautifulSoup(r.content, 'html.parser')
    show_id = soup.find("li", {"class": "addto-later addto noselect"})['data-id']
    ep_list_url = "https://7anime.io/load-list-episode/?id=" + show_id

    r = requests.get(ep_list_url)
    soup = BeautifulSoup(r.content, 'html.parser')

    episode_list = []
    ep_link_tags = soup.findAll("a")
    for ep_link_tag in ep_link_tags:
        file_name = show_name + "_S" + season + "_E" + ep_link_tag.text + ".mp4"
        ep_url = ep_link_tag['href']
        episode_list.append({'name': file_name, 'url': ep_url})

    # make episode api call - threaded
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for episode in episode_list:
            if ospath.exists(download_location + episode['name']):
                print("Skipping %s because file already exists" % episode['name'])
                continue

            future = executor.submit(get_download_link, episode['url'])
            futures.append((episode['name'], future))

    download_list = []
    for name, future in futures:
        result = future.result()
        if result is not None:
            download_list.append((name, result))

    deleted_files = False
    # download mp4 from google
    for name, download_link in download_list:
        print("Downloading: %s" % name)
        r = requests.get(download_link, stream=True)
        path = download_location + name
        with open(path, 'wb') as f:
            total_length = int(r.headers.get('content-length'))
            for chunk in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length / 1024) + 1):
                if chunk:
                    f.write(chunk)
                    f.flush()

        # if file too small (under 2k), delete it
        if ospath.getsize(path) < 2 * 1024:
            os.remove(path)
            deleted_files = True

    if deleted_files:
        raise Exception("Downloaded files that were empty")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please provide download file name")
        exit(0)

    if not ospath.exists(sys.argv[1]):
        print("Please make sure file exists")
        exit(0)

    DOWNLOAD_ROOT = sys.argv[2]

    exceptions = 1
    retries = 0
    MAX_RETRIES = 5
    while exceptions > 0 | retries < MAX_RETRIES:
        retries += 1
        exceptions = 0
        with open(sys.argv[1], mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_num = 0
            for row in csv_reader:
                print(row)
                try:
                    download_show(row['show_name'], row['season'], row['url'])
                except:
                    exceptions += 1
                    print("Failed to download show: %s" % row['show_name'])
