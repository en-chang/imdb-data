import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class SplitWritersFn(beam.DoFn):
  def process(self, element):  
    writer_record = element
    writers = writer_record.get('writers')
    title = writer_record.get('tConst')
    writers = writers.strip()
    writers = writers.split(',')
    if writers[0] == "\\N":
      return
    writer_dicts = []
    for writer in writers:
      writer_dicts.append({'tConst':title, 'writer':writer})
    return(writer_dicts)


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
    google_cloud_options.job_name = 'split-writers-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
    
    sql = 'SELECT writers, tConst FROM imdb_modeled.Writes WHERE writers IN (SELECT DISTINCT nConst FROM imdb_modeled.People)'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write PCollection to log file
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format the student's date of birth  
    split_directors_pcoll = query_results | 'Split up writers' >> beam.ParDo(SplitWritersFn())

    # write PCollection to log file
    split_directors_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'split_writers_pcoll.txt')

    dataset_id = 'imdb_modeled'
    table_id = 'Writes_Beam_DF'
    schema_id = 'Writer:STRING,tConst:STRING'

    # write PCollection to new BQ table
    split_directors_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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