import memcache

def bigtable_read_data(request):
    # Connect to the Memcached instance
    #client = memcache.Client(["127.0.0.1:11211"])
    client = memcache.Client([""+request.headers.get("ip_port")+""])
    # Set a key-value pair
    client.set("my-key", "my-value")
    # Retrieve the value of a key
    value = client.get("my-key")
    print(value)

#gcloud functions deploy bigtable_read_data \ --runtime python310 --trigger-http

#curl "https://REGION-PROJECT_ID.cloudfunctions.net/bigtable_read_data" -H "instance_id: test-instance" -H "table_id: test-table"
