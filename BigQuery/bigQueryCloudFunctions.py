from google.cloud import bigquery

client = bigquery.Client()

#1-Create Table in bigquery
def bigquery_create_table(request):
    table_id= request.headers.get("table_id")
    dataset_id= request.headers.get("dataset_id")
    schema = [
    bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
]

    table = bigquery.Table(dataset_id+"."+table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#2-read data from existing bigTable
def bigquery_read_data(request):

    #client, dataset and table  defining
    #table_ref = client.dataset(request.headers.get("dataset_id")).table(request.headers.get("table_id"))
    dataset_id= request.headers.get("dataset_id")
    table_id= request.headers.get("table_id")
    #quering the table
    query = f"SELECT * FROM `{dataset_id}.{table_id}`"
    rows = client.query(query).result()
    for row in rows:
        print(row)

    return (rows)

    #----------------------------------------------------------

#3-inset and update data for existing bigQuery table
def insert_update_bigquery_table(project_id,instance_id, table_id,request):
    dataset_id= request.headers.get("dataset_id")
    table_id= request.headers.get("table_id")
    #quering the table
    #ALTER TABLE dataset.NewArrivals ALTER COLUMN quantity SET DEFAULT 100;
    #INSERT dataset.NewArrivals (product, quantity, warehouse)
    #VALUES('top load washer', DEFAULT, 'warehouse #1'),
    #('dryer', 200, 'warehouse #2'),
    #('oven', 300, 'warehouse #3');
    query_alter = f"ALTER TABLE FROM `{dataset_id}.{table_id}` ALTER COLUMN quantity SET DEFAULT 100"
    query_insert = f"INSERT `{dataset_id}.{table_id}` (product, quantity, warehouse)
    VALUES('top load washer', DEFAULT, 'warehouse #1'),
    #('dryer', 200, 'warehouse #2'),
    #('oven', 300, 'warehouse #3')"
    result_alter = client.query(query_alter).result()
    result_insert = client.query(query_insert).result()
    return (result_alter,result_insert)
