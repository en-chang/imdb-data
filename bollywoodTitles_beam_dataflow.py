import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatDateFn(beam.DoFn):
  def process(self, element):  
    title_record = element
    title = title_record.get('title')
    crores = title_record.get('croresGrossed')
    month = title_record.get('MMM')
    day = title_record.get('DD')
    year = str(title_record.get('YYYY'))
    if day is None: 
      day = '01'
    else:
      day = str(day)
    if crores is None: 
      crores = 0
    month_dict = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06','JUL':'07','AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12','MM':'01'}	
    if month is None:month = '01'
    else:month = month_dict[month.upper()]
    date = year + '-' + month + '-' + day
    return [{'title':title, 'releaseDate':date, 'croresGrossed':crores}]

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
    google_cloud_options.job_name = 'format-date-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)
    
    sql = 'SELECT * FROM bollywood_modeled.bollywoodTitles'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write PCollection to log file
    query_results | 'Write log 1' >> WriteToText(DIR_PATH + 'query_results.txt')

    # apply ParDo to format the student's date of birth  
    formatDate_pcoll = query_results | 'Format the dates' >> beam.ParDo(FormatDateFn())

    # write PCollection to log file
    formatDate_pcoll | 'Write log 2' >> WriteToText(DIR_PATH + 'formatDate_pcoll.txt')

    dataset_id = 'bollywood_modeled'
    table_id = 'bollywoodTitles_Beam_DF'
    schema_id = 'title:STRING,releaseDate:DATE,croresGrossed:NUMERIC'

    # write PCollection to new BQ table
    formatDate_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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