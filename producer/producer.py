import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

LEAGUE_ID = 4328
BASE_URL = "https://www.thesportsdb.com/api/v1/json/123"

def getStandings():
    url = f"{BASE_URL}/lookuptable.php?l={LEAGUE_ID}&s=2024-2025"
    response = requests.get(url)
    return response.json().get("table", [])

def getUpcomingFixtures():
    url = f"{BASE_URL}/eventsnextleague.php?id={LEAGUE_ID}"
    response = requests.get(url)
    return response.json().get("events", [])

def getTeams():
    url = f"{BASE_URL}/lookup_all_teams.php?id={LEAGUE_ID}"
    response = requests.get(url)
    return response.json().get("teams", [])

def produce(topic, data):
    for item in data:
        producer.send(topic, item)

if __name__ == "__main__":
    while True:
        standings = getStandings()
        fixtures = getUpcomingFixtures()
        teams = getTeams()

        produce("plStandings", standings)
        produce("plFixtures", fixtures)
        produce("plTeams", teams)

        print("Produced Standings, Fixtures and Teams to Kafka")
        time.sleep(3600) # every hour