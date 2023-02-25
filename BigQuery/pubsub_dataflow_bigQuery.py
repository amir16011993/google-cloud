from __future__ import absolute_import

 

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from google.cloud import pubsub_v1

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            help='The Bigtable project ID, this can be different than your '
                 'Dataflow project',
            default='my-kubernetes-codelab-370814')
        parser.add_argument(
            '--dataset_id',
            help='The dataset instance ID',
            default='query_dataset')
        parser.add_argument(
            '--table_id',
            help='The Bigtable table ID in the instance.',
            default='data')
        parser.add_argument(
            '--topic_name',
            help='The topic name in the instance.',
            default='sub1')

class PubSubToBigQuery(beam.DoFn):

    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = "my-kubernetes-codelab-370814"
        self.dataset_id = "Bigquery_dataset"
        self.table_id = "data"
        self.client = None
    
    def process(self, element):
        # Apply some custom processing to the element
        return [{
            'field1': element['field1'],
            'field2': element['field2'],
            'field3': element['field3']
        }]

def run(argv=None):
    options = MyOptions(argv)
    project_id = options.project_id
    dataset_id = options.dataset_id
    table_id = options.table_id
    topic_name= options.topic_name

 

    pipeline_options = PipelineOptions(flags=argv)
    with beam.Pipeline(options=pipeline_options) as p:
        (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=topic_name) | 'Transform' >> beam.ParDo(PubSubToBigQuery()) |  'Write to BigQuery' >> WriteToBigQuery(table=''+str(project_id)+':'+str(dataset_id)+'.'+str(table_id), schema='field1:STRING, field2:INTEGER, field3:FLOAT' , create_disposition='CREATE_IF_NEEDED', write_disposition='WRITE_APPEND'))

    #client, dataset and table  defining
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    
    #quering the table
    query = f"SELECT * FROM `{dataset_id}.{table_id}`"
    rows = client.query(query).result()
    for row in rows:
        print(row)

if __name__ == "__main__":
    run()


#extras

#deploy the pipeline : 
"""python my_pipeline.py --project=project-id --runner=DataflowRunner --region=us-central1 --temp_location=gs://my-bucket/temp --staging_location=gs://my-bucket/staging"""

#pulish to pubsub topic publisher = pubsub_v1.PublisherClient()
"""topic_path = publisher.topic_path(project_id, topic_name)
publisher.publish(topic_path, data=data)"""
