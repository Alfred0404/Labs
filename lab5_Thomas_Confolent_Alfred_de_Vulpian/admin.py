# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config =  {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
topic='wikistreams'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

# We wait that Kafka confirm the creation of topic
for future in futures.values():
  try:
    future.result()
  except Exception as error:
    if 'TOPIC_ALREADY_EXISTS' not in str(error):
      raise

# %%
x = admin_client.list_topics()
for  t in x.topics.keys():
  print(t)

# %%
#admin_client.delete_topics([topic])

# %%
