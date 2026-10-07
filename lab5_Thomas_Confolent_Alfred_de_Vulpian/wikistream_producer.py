# %% Dependencies
import socket
import json
import time
import argparse
import sys
from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams
from datetime import datetime, timedelta

parser = argparse.ArgumentParser()
parser.add_argument('--duration', type=float, default=10)
parser.add_argument('--wiki', default='fr.wikipedia.org')
parser.add_argument('--page', default=None)
args, _ = parser.parse_known_args()

if hasattr(sys.stdout, 'reconfigure'):
  sys.stdout.reconfigure(encoding='utf-8')

# %% Helper Functions
# Function for change the message from python dict to json
value_serializer = lambda val: json.dumps(val).encode('utf-8')

# %% Producder Instantiation
# We put the configuration of producer
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'compression.type': 'lz4'
}

# We create the producer
producer = Producer(conf)

# %% Create wikistreams query
stream = EventStreams(
  streams=['recentchange']
)
stream.register_filter(server_name=args.wiki, type='edit')

# %% Query EventStream
# We run one query for see the raw message and one formatted example
change = next(stream)
print('Raw Message: ' + str(change))
print(
  '\n' # Add line breakd between outputs
  'Formatted Message: {type} on page "{title}" by "{user}" at {meta[dt]}.'
  .format(**change)
)

# %% Streaming Query
duration = args.duration # Streaming duration in minutes
start_time = datetime.now() # Current clock time
stop_time = start_time + timedelta(minutes=duration) #start+duration=stop

# We read the stream during <duration> minutes
# We can uncomment print for see the output in interactive environment
topic='wikistreams'
while datetime.now() < stop_time:
  change = next(stream)
  if args.page and args.page.lower() not in change.get('title', '').lower():
    continue
  producer.produce(
    topic=topic,
    value=value_serializer(change)
  )
  producer.poll(0)
  print(' ')
  print(change)

# We send remaining messages before close the script
print('\n sending the last messages')
producer.flush()
print('\n closing producer')
print('')
