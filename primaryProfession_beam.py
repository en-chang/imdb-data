import logging
import apache_beam as beam
from apache_beam.io import WriteToText


class SplitPrimProfFn(beam.DoFn):
  def process(self, element):  
    castmemb_record = element
    name = castmemb_record.get('nConst')
    jobs = castmemb_record.get('primaryProfession')
    jobs = jobs.strip()
    jobs = jobs.split(',')
    job_dicts = []
    for job in jobs:
      job_dicts.append({'nConst':name, 'primaryProfession':job})
    return(job_dicts)

          
def run():
     PROJECT_ID = 'starry-center-266501' # change to your project id # creating pipeline through direct runner

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT nConst, primaryProfession FROM imdb_modeled.People limit 100' # passing a query. Shouldn't process more than 1000 records w DR
        # directors is an array of strings, tConst is a string 
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # apply ParDo to split the directors titles  
     # call pardo, pipe query results to pardo
     split_primaryProfession_pcoll = query_results | 'Return castMember: primaryProfession i dictonaries' >> beam.ParDo(SplitPrimProfFn()) 

     # write PCollection to log file
     split_primaryProfession_pcoll | 'Write log 1' >> WriteToText('formatted_professions_pcoll.txt') 

     dataset_id = 'imdb_modeled'
     table_id = 'primaryProfession_Beam'
     schema_id = 'nConst:STRING, primaryProfession:STRING'

     # write PCollection to new BQ table
     split_primaryProfession_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                table=table_id, 
                                                schema=schema_id, 
                                                project=PROJECT_ID,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, 
                                                batch_size=int(100))
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()