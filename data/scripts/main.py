import os
import csv
import random
import pandas as pd
import requests
from lxml import html
from bs4 import BeautifulSoup


def read_file(path):
    f = open(path, "r")
    agents = [a for a in f.read().split("\n")]
    f.close()
    return agents


def get_response(headers, url, timeout=10, max_retry=3):
    lastException = None
    for _ in range(max_retry):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            return response.text
        except Exception as e:
            lastException = e
    raise lastException

import re

def extract_data(links, games_path, headers):
    for link in links:
        body = [["Month", "Avg.Players", "Gain", "% Gain", "Peak Players"]]
        response = get_response(headers, link + "#All")
        soup = BeautifulSoup(response, "lxml")

        a = soup.find_all('a')
        name = a[1].text.replace(" ", "")
        name = re.sub('\W+', '', name) + ".csv"

        for row in soup.select('tbody tr'):
            row_text = [x.text for x in row.find_all('td')]
            row_text[0] = row_text[0].strip()

            body.append(row_text)

        with open(os.path.join(games_path, name), "w", newline='') as file:
            writer = csv.writer(file, delimiter = ",")
            writer.writerows(body)

        print(name)


def run():
    agents = os.path.join("agents", "agents.txt")
    steam = os.path.join(os.getcwd(), "..", "steam.txt")
    twitch = os.path.join(os.getcwd(), "..", "twitch.txt")

    steam_games_path = os.path.join(os.getcwd(), "..", "steam_games")
    twitch_games_path = os.path.join(os.getcwd(), "..", "twitch_games")

    user_agents = read_file(agents)
    steam_links = read_file(steam)
    twitch_links = read_file(twitch)

    headers = {'User-Agent': random.choice(user_agents)}

    extract_data(steam_links, steam_games_path, headers)
    extract_data(twitch_links, twitch_games_path, headers)


if __name__ == '__main__':
    run()
