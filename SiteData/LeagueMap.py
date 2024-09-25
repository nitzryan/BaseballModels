import requests
import json

response = requests.get('https://statsapi.mlb.com/api/v1/leagues?')
if not response.ok:
    print(f"Invalid Response: {response.status_code}")
    exit(1)
    
json_data = response.json()

league_map = {}
for league in json_data["leagues"]:
    league_map[league["id"]] = league["abbreviation"]
    
json_maps = json.dumps(league_map, indent=2)
with open("league_map.json", "w") as file:
    file.write(json_maps)