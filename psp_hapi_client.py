import requests

# Specify query parameters.
start = ''
stop = ''

# Assemble the query string.
# query_string = 'hapi'
# query_string = 'hapi/about'
# query_string = 'hapi/capabilities'
# query_string = 'hapi/catalog'
# query_string = 'hapi/info?dataset=trajectory'
query_string = 'hapi/data?dataset=trajectory&start=2024-01-01T00:00Z&stop=2024-01-01T01:00Z&parameters=Time,SPP.x,SPP.y,SPP.z'

# Assemble the query URL.
base_url = 'http://localhost:8000'
query_url = '/'.join([base_url, query_string])
print(f"{query_url=}")

# Fetch and print the result.
r = requests.get(query_url, timeout=30)
print(r.text)
