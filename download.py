import concurrent.futures
import csv
import json
import sys
from os import path as ospath
from pathlib import Path

import requests
from clint.textui import progress
from retrying import retry

SEASON_ENDPOINT = "https://aultima-api-flask.herokuapp.com/get/season/episodes"
EPISODE_ENDPOINT = "https://aultima-api-flask.herokuapp.com/get/episode"
DOWNLOAD_ROOT = "/Users/eau/Documents/Development/aultima-api-flask/temp/"


@retry(stop_max_attempt_number=5, wait_random_min=1000, wait_random_max=4000)
def get_download_link(url):
    try:
        print("Retrieving download link for %s" % url)
        new_parameters = {
            "url": url
        }
        episode_json = requests.post(EPISODE_ENDPOINT, json=new_parameters).content.decode()
        download_link = json.loads(episode_json)['download_link']
        return download_link
    except:
        print("Failed to get download link for %s" % url)


def download_show(show_name, season, aultima_show_url):
    # create download folder if doesn't exist
    download_location = DOWNLOAD_ROOT + show_name + '/Season ' + season + '/'
    print("Creating %s if not exist" % download_location)
    Path(download_location).mkdir(parents=True, exist_ok=True)

    # make season API call
    print("Making /season API call")
    parameters = {
        "url": aultima_show_url,
        "show_name": show_name,
        "season": season
    }

    episodes = requests.post(SEASON_ENDPOINT, json=parameters).content.decode()
    episode_list = json.loads(episodes)

    # make episode api call - threaded
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
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


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please provide download file name")
        exit(0)

    if not ospath.exists(sys.argv[1]):
        print("Please make sure file exists")
        exit(0)

    DOWNLOAD_ROOT = sys.argv[2]

    with open(sys.argv[1], mode='r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_num = 0
        for row in csv_reader:
            print(row)
            download_show(row['show_name'], row['season'], row['url'])
