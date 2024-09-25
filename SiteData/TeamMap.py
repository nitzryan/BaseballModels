import requests
import json

response = requests.get('https://statsapi.mlb.com/api/v1/teams?')
if not response.ok:
    print(f"Invalid Response: {response.status_code}")
    exit(1)
    
json_data = response.json()

team_map = {}
for team in json_data["teams"]:
    team_map[team["id"]] = team["abbreviation"]
    
json_maps = json.dumps(team_map, indent=2)
with open("team_map.json", "w") as file:
    file.write(json_maps)