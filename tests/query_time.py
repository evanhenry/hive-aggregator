import couchdb
import matplotlib.pyplot as plt
import time

# Setup CouchDB
COUCHDB_DATABASE = 'hivemind-plus'
try:
  server = couchdb.Server()
  couch = server[COUCHDB_DATABASE]
except Exception as error:
  couch = server.create(COUCHDB_DATABASE)

# Render query for time
INTERVAL = 10000
keys = ['Time']
values = dict(zip(keys,[[]]))
map_nodes = "function(doc) { if (doc.Time >= " + str(time.time() - INTERVAL) + ") emit(doc); }"
matches = couch.query(map_nodes)
for row in matches:
  for key in keys:
    try:
      values[key].append([row.key[key], row.key[key]])
    except Exception:
      pass
plt.plot(sorted(values['Time']))
plt.show()
