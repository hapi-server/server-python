import requests

# Specify query parameters.
start = ''
stop = ''

# Assemble the query string.
# query_url = 'http://localhost:8000/hapi'
# query_url = 'http://localhost:8000/hapi/about'
# query_url = 'http://localhost:8000/hapi/capabilities'
# query_url = 'http://localhost:8000/hapi/catalog'
# query_url = 'http://localhost:8000/hapi/info?dataset=trajectory'
# query_url = 'http://localhost:8000/hapi/data?dataset=trajectory&start=2024-01-01T00:00Z&stop=2024-01-01T01:00Z&parameters=Time,SPP.x,SPP.y,SPP.z'
query_url = 'http://localhost:8000/hapi/data?dataset=trajectory&start=2024-01-01T00:00Z&stop=2024-01-02T01:00Z&parameters=Time,SPP.x&format=json'

# Fetch and print the result.
r = requests.get(query_url, timeout=30)
print(r.text)
