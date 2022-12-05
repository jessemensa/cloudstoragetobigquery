import apache_beam as beam 
import os # access to operating system, pass variables, access information 
from apache_beam.options.pipeline_options import PipelineOptions # pipeline options 

# to get the project => navigate to google cloud console PROJECT INFO 
pipeline_options = {
    'project': 'dataflow-1-370714', # PROJECT ID 
    'runner': 'DataflowRunner', # RUNNER
    'region': 'europe-west2', 
    'staging_location': 'gs://dataflow-one-je/temp', # STAGING LOCATION => where during execution, dataflow will store the temporary files 
    'temp_location': 'gs://dataflow-one-je/temp', # TEMP LOCATION => where deploying pipeline, put the files in the temp location 
    'template_location': 'gs://dataflow-one-je/templates/onebatchjobfile', # TEMPLATE LOCATION => where the template will be stored
    'save_main_sessions': True 
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options) # create pipeline options from dictionary 
p1 = beam.Pipeline(options=pipeline_options) # add pipeline options to pipeline



serviceAccount = "/Users/jessmensa/Desktop/batchdataflow/dataflow-1-370714-39edceda5acd.json" # key 
# when writing file to gcp storage, will be hitting the API
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount # environment variable

class split_lines(beam.DoFn):
    def process(self, record):
        return [record.split(',')] 


# function recieves a beam do function 
# when using a beam function, every class that function is created must return a single record 
class Filter(beam.DoFn):
    # function process and parameters 
    def process(self, record):
        if int(record[8]) > 0:
            return [record] 

def dict_level1(record):
    dict_ = {} 
    dict_['airport'] = record[0] 
    dict['list'] = record[1] 
    return(dict_) 

def unest_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return[(key * '_' + k, v) for k, v in unest_dict(value).items()] 
        else:
            return([key, value]) 
    items = [item for k, v in record.items() for item in expand(k, v)] 
    return dict(items) 

def dict_level0(record):
    dict_ = {} 
    dict_['airport'] = record['airport'] 
    dict_['list_Delayed_num'] = record['list_Delayed_num'][0] 
    dict_['list_Delayed_time'] = record['list_Delayed_time'][0]
    return(dict_) 

table_schema = 'airport:STRING, list_Delayed_num:INTEGER, list_Delayed_time:INTEGER' 
table = 'dataflow-1-370714:onebatchjobfile.onebatchjobfile' # BIGQUERY TABLE PATH 
    

p1 = beam.Pipeline() 


# point to file in google cloud storage 
Delayed_time = (
    p1 
    | "Import Data time" >> beam.io.ReadFromText("gs://dataflow-one-je/input/voos_sample.csv", skip_header_lines=1) # readfromtext method to read the csv file 
    | "Split by comma time" >> beam.Map(lambda record: record.split(',')) 
    | "Filter Delays time" >> beam.ParDo(Filter()) 
    | "Create key value-value time" >> beam.Map(lambda record: (record[4], int(record[8]))) 
    | "sum by key time" >> beam.CombinePerKey(sum) 
)


Delayed_num = (
    p1 
    | "Import Data" >> beam.io.ReadFromText("gs://dataflow-one-je/input/voos_sample.csv", skip_header_lines=1) # readfromtext method to read the csv file 
    | "Split by comma" >> beam.Map(lambda record: record.split(',')) 
    | "Filter Delays" >> beam.ParDo(Filter())
    | "Create key value pair" >> beam.Map(lambda record: (record[4], int(record[8]))) 
    | "combine by key" >> beam.combiners.Count.PerKey() 
)

# SENDING DATA TO BIGQUERY 
# DATA MUST BE IN A SIMPLE KEY VALUE PAIR 

Delay_table = (
    {'Delayed_num': Delayed_num, 'Delayed_time': Delayed_time}
    | "Group by" >> beam.CoGroupByKey() 
    | "Unnest 1" >> beam.Map(lambda record: dict_level1(record)) 
    | "Unnest 2" >> beam.Map(lambda record: unest_dict(record)) 
    | "Unnest 3" >> beam.Map(lambda record: dict_level0(record)) 
    | "Write to BigQuery" >> beam.io.WriteToBigQuery(
        table, 
        schema=table_schema, 
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, # WRITE APPEND 
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, # CREATE IF NEEDED 
        custom_gcs_temp_location='gs://dataflow-one-je/temp'
    )
)

p1.run() 

# WHEN IT RUN, ITS GOING TO BE HITTING THE STORAGE BUCKET API AND ASK FOR GOOGLE APPLICATION CREDENTIALS 
# THEN WE HAND OVER THE CREDENTIALS AND IT WORKS 