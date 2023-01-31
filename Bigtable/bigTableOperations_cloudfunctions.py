from google.cloud import bigtable
from google.cloud.bigtable.row_set import RowSet

client = bigtable.Client()


def bigtable_read_data(request):
    instance = client.instance(request.headers.get("instance_id"))
    table = instance.table(request.headers.get("table_id"))

    #1-create initial data
    prefix = 'phone#'
    end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)

    outputs = []
    row_set = RowSet()
    row_set.add_row_range_from_keys(prefix.encode("utf-8"),
                                    end_key.encode("utf-8"))

    
    #read created data from the created rowset
    rows = table.read_rows(row_set=row_set)
    for row in rows:
        output = 'Rowkey: {}, os_build: {}'.format(
            row.row_key.decode('utf-8'),
            row.cells["stats_summary"]["os_build".encode('utf-8')][0]
            .value.decode('utf-8'))
        outputs.append(output)

    return '\n'.join(outputs)

    #----------------------------------------------------------

    #2-read data from existing bigTable
    """
    def readbigTable(project_id,instance_id, table_id,request):
        client = bigtable.Client(project=project_id, admin=True)
        instance=client.instance(instance_id)
        row_filter= row_filters.CellsColumnLimitFilter(1)
        table = instance.table(table_id)
        print("Scanning for all greetings:")
        partial_rows= table.read_rows(filter_=row_filter)

        for row in partial_rows:
            cell=row.cells["cf1"]['column1'.encode()][0]
            print(cell.value.decode("utf-8"))"""

    #----------------------------------------------------------
    
    #3-write data to existing bigTable with dataflow
    """
    def writeTobigTable(element, project_id,instance_id, table_id,request):
        row_key, value = element.split(',')
        client = bigtable.Client(project=self.project_id, admin=True)
        instance = self.client.instance(self.instance_id)
        table = self.instance.table(self.table_id)
        column_family_id = 'cf1'
        column_id = 'column1'.encode()
        row = self.table.row(row_key)
        row.set_cell(column_family_id, column_id, value)
        row.commit()"""



