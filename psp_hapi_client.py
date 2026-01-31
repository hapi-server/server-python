import json
import requests

# Assemble the query string.
# query_url = 'http://localhost:8000/hapi'
# query_url = 'http://localhost:8000/hapi/about'
# query_url = 'http://localhost:8000/hapi/capabilities'
# query_url = 'http://localhost:8000/hapi/catalog'
# query_url = 'http://localhost:8000/hapi/info?dataset=trajectory'
query_url = 'http://localhost:8000/hapi/data?dataset=trajectory_1minute&start=2024-01-01T00:00Z&stop=2024-01-01T01:00Z&parameters=Time,SPP.x,SPP.y,SPP.z'
# query_url = 'http://localhost:8000/hapi/data?dataset=trajectory&start=2024-01-01T00:00Z&stop=2024-01-02T01:00Z&parameters=Time,SPP.x&format=json'

# Fetch and print the result.
r = requests.get(query_url, timeout=30)
if "format=json" in query_url:
    # Parse JSON results.
    results = json.loads(r.text)
else:
    # Parse CSV results.
    lines = r.text.split('\r\n')
    results = []
    for r in lines:
        fs = r.split(",")
        new_r = []
        new_r.append(fs[0])  # Time as a string
        new_r.append([float(f) for f in fs[1:]])
        results.append(new_r)
print(results)
