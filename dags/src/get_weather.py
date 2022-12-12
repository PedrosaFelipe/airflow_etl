import requests
import json
from datetime import datetime
import os


# API params
url = "https://api.weatherapi.com/v1/current.json?"
parameters = {'q'  : 'London',
              'key': '7fd1fed3b4c44f2891e182426220112',
              'aqi': 'no'
              }

response = requests.get( url, parameters )


if response.status_code == 200:

	# get data
	json_data = response.json()
	filename = str(datetime.now().date() ) + '.json'

	tot_name = os.path.join(os.path.dirname(__file__) , 'data', filename)

	with open(tot_name , 'w') as outputfile:
		json.dump(json_data , outputfile)

else:
	print(response.status_code)
	print("Error in API Call")