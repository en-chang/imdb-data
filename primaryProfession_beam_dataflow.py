import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class SplitPrimProfFn(beam.DoFn):
  def process(self, element):  
    castmemb_record = element
    name = castmemb_record.get('nConst')
    jobs = castmemb_record.get('primaryProfession')
    if (jobs == None): return
    jobs = jobs.strip()
    jobs = jobs.split(',')
    job_dicts = []
    for job in jobs:
      job_dicts.append({'nConst':name, 'primaryProfession':job})
    return(job_dicts)

def run():         
    PROJECT_ID = 'starry-center-266501' # change to your project id
    BUCKET = 'gs://imdb-beam' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'split-prim-prof-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
    
    sql = 'SELECT nConst, primaryProfession FROM imdb_modeled.People'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write PCollection to log file
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format the student's date of birth  
    split_primProfs_pcoll = query_results | 'Split up primary professions' >> beam.ParDo(SplitPrimProfFn())

    # write PCollection to log file
    split_primProfs_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'split_primProfs_pcoll.txt')

    dataset_id = 'imdb_modeled'
    table_id = 'primaryProfession_Beam_DF'
    schema_id = 'nConst:STRING,primaryProfession:STRING'

    # write PCollection to new BQ table
    split_primProfs_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()