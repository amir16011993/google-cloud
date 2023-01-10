from __future__ import absolute_import

 

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            help='The Bigtable project ID, this can be different than your '
                 'Dataflow project',
            default='my-kubernetes-codelab-370814')
        parser.add_argument(
            '--instance_id',
            help='The Bigtable instance ID',
            default='dataflow')
        parser.add_argument(
            '--table_id',
            help='The Bigtable table ID in the instance.',
            default='data')

class BigtableInsert(beam.DoFn):

    def __init__(self, project_id, instance_id, table_id):
        self.project_id = "my-kubernetes-codelab-370814"
        self.instance_id = "dataflow"
        self.table_id = "data"
        self.client = None

 

    def process(self, element, *args, **kwargs):
        row_key, value = element.split(',')
        if not self.client:
            self.client = bigtable.Client(project=self.project_id, admin=True)
            self.instance = self.client.instance(self.instance_id)
            self.table = self.instance.table(self.table_id)
        column_family_id = 'cf1'
        column_id = 'column1'.encode()
        row = self.table.row(row_key)
        row.set_cell(column_family_id, column_id, value)
        row.commit()

 

def run(argv=None):
    options = MyOptions(argv)
    project_id = options.project_id
    instance_id = options.instance_id
    table_id = options.table_id

 

    pipeline_options = PipelineOptions(flags=argv)
    with beam.Pipeline(options=pipeline_options) as p:
        data = p | 'Create Data' >> beam.Create(['row1,value1', 'row2,value2', 'row3,value3'])
        data | 'Insert Data' >> beam.ParDo(BigtableInsert(project_id, instance_id, table_id))

    client = bigtable.Client(project=project_id, admin=True)
    instance=client.instance(instance_id)
    row_filter= row_filters.CellsColumnLimitFilter(1)
    table = instance.table(table_id)
    print("Scanning for all greetings:")
    partial_rows= table.read_rows(filter_=row_filter)

    for row in partial_rows:
        cell=row.cells["cf1"]['column1'.encode()][0]
        print(cell.value.decode("utf-8"))

if __name__ == "__main__":
    run()
