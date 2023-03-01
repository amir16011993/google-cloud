import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigtable
from apache_beam.io.gcp.internal.clients import bigtable_data
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.internal.clients import dataflow
from apache_beam.io.gcp.bigtableio import ReadFromBigTable
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import PTransform
from apache_beam.transforms.window import FixedWindows

PROJECT_ID = 'YOUR_PROJECT_ID'
BIGTABLE_INSTANCE = 'YOUR_BIGTABLE_INSTANCE_ID'
BIGTABLE_TABLE = 'YOUR_BIGTABLE_TABLE_NAME'
BIGQUERY_DATASET = 'YOUR_BIGQUERY_DATASET_NAME'
BIGQUERY_TABLE = 'YOUR_BIGQUERY_TABLE_NAME'

# Define the Bigtable source query.
query = bigtable_data.ReadRowsRequest()
query.table_name = 'projects/{}/instances/{}/tables/{}'.format(PROJECT_ID, BIGTABLE_INSTANCE, BIGTABLE_TABLE)

# Define the BigQuery target table schema.
schema_json = '''
[
  {
    "name": "field1",
    "type": "STRING"
  },
  {
    "name": "field2",
    "type": "INTEGER"
  },
  {
    "name": "field3",
    "type": "FLOAT"
  }
]
'''
table_schema = parse_table_schema_from_json(schema_json)

class FormatData(PTransform):
    # Format the Bigtable data into a dictionary that can be written to BigQuery.
    def expand(self, row):
        data = {}
        data['field1'] = row.row_key.decode('utf-8')
        data['field2'] = int(row.cells['cf']['field2'][0].value.decode('utf-8'))
        data['field3'] = float(row.cells['cf']['field3'][0].value.decode('utf-8'))
        return [data]

def run():
    # Define the pipeline options.
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Define the pipeline.
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the data from Bigtable.
        data = p | 'Read from Bigtable' >> ReadFromBigTable(project_id=PROJECT_ID, instance_id=BIGTABLE_INSTANCE, table_id=BIGTABLE_TABLE)

        # Format the data.
        formatted_data = data | 'Format Data' >> FormatData()

        # Write the data to BigQuery.
        formatted_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(table=BIGQUERY_TABLE, dataset=BIGQUERY_DATASET, project=PROJECT_ID, schema=table_schema, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

if __name__ == '__main__':
    run()
