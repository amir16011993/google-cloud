import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigtable
from apache_beam.io.gcp.internal.clients import bigtable_data
from apache_beam.io.gcp.internal.clients import bigquery
from google.cloud import bigquery
from apache_beam.io.gcp.internal.clients import dataflow
from apache_beam.io.gcp.bigtableio import ReadFromBigTable
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import PTransform
from apache_beam.transforms.window import FixedWindows

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            help='The BigQuery project ID, this can be different than your '
                 'Dataflow project',
            default='my-kubernetes-codelab-370814')
        parser.add_argument(
            '--dataset_id',
            help='The dataset instance ID',
            default='query_dataset')
        parser.add_argument(
            '--BQtable_id',
            help='The BigtQuery table ID in the instance.',
            default='data')
        parser.add_argument(
            '--Bigtable_Instance',
            help='The BigTable Instance.',
            default='bt_instance')
        parser.add_argument(
            '--BigTable_Table',
            help='The BigTable table.',
            default='bt_table')
        
# Format the Bigquery data so that it can be written to Bigtable.
def FormatData(element):
    row_key = element['row_key']
    columns = {}
    for key, value in element.items():
        if key != 'row_key':
            columns[key] = str(value).encode('utf-8')

    return (row_key.encode('utf-8'), columns)

def run(argv=None):
    # Define the Bigtable source query.
    options = MyOptions(argv)
    project_id = options.project_id
    dataset_id = options.dataset_id
    table_id = options.BQtable_id
    bigtable_instance = options.Bigtable_Instance
    bigtable_table = options.Bigtable_Table

    # Define BigQuery
    #table = 'your-project-id.your-dataset.your-table'
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    query = 'SELECT * FROM {}'.format(table_ref)
    # Define the Bigtable source query.
    query = bigtable_data.ReadRowsRequest()
    query.table_name = 'projects/{}/instances/{}/tables/{}'.format(project_id, bigtable_instance, bigtable_table)

    # Define the pipeline options.
    pipeline_options = PipelineOptions()

    # Define the pipeline.
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the data from BigQuery.
        (p | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True) 
        | 'Transform' >> beam.Map(FormatData) | 'Write to BigTable' >> apache_beam.io.gcp.bigtableio.WriteToBigTable(project_id=project_id, instance_id=bigtable_instance, table_id=bigtable_table))

if __name__ == '__main__':
    run()
