import requests

url = "http://zgombjrpi.eastus.cloudapp.azure.com/rpiguessr/backend/player-add.php"
data = {
    "player_name": "lol",
    "score": 0
}

response = requests.post(url, data=data)

# Print the response to check if the request was successful
print(response.text)