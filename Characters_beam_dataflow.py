import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class SplitCharactersFn(beam.DoFn):
  def process(self, element):  
    castmemb_record = element
    characters = castmemb_record.get('characters')
    characters = characters.get('characters')
    name = castmemb_record.get('nConst')
    title = castmemb_record.get('tConst')
    job = castmemb_record.get('job')
    castmemb_record['characters'] = characters
    
    if characters != "\\N":
      characters = characters.strip() # remove white space
      characters = characters[1:-1] # remove brackets
      characters = characters.split(',') # split into an array
      characters = characters[0]
      characters = characters[1:-1] # remove quotes
      castmemb_record['characters'] = characters
      print('if:', castmemb_record)
      return [castmemb_record]
    else: # character is "\N"
      print('else:', castmemb_record)
      return [castmemb_record]
          
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
    google_cloud_options.job_name = 'split-characters-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
    
    sql = 'SELECT DISTINCT tConst, nConst, job, characters FROM imdb_modeled.Characters'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # apply ParDo to split the directors titles  
    # call pardo, pipe query results to pardo
    split_characters_pcoll = query_results | 'Return title: director i dictonaries' >> beam.ParDo(SplitCharactersFn()) 

    # write PCollection to log file
    split_characters_pcoll | 'Write log 1' >> WriteToText(DIR_PATH + 'split_characters_pcoll.txt') 

    dataset_id = 'imdb_modeled'
    table_id = 'Characters_Beam_DF'
    schema_id = 'tConst:STRING, nConst:STRING, job:STRING, characters:STRING'

    # write PCollection to new BQ table
    split_characters_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
