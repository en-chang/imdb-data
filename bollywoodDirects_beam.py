import logging
import apache_beam as beam
from apache_beam.io import WriteToText


class SplitDirectorsFn(beam.DoFn):
  def process(self, element):  
    director_record = element
    title = director_record.get('title')
    directors = director_record.get('director').strip().split(',')
    directors_dicts = []
    for director in directors:
      directors_dicts.append({'title':title, 'director':director})
    return(directors_dicts)

def run():
     PROJECT_ID = 'starry-center-266501' # change to your project id # creating pipeline through direct runner

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM bollywood_modeled.bollywoodDirects limit 100' # query the bollywood_modeled table
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # apply ParDo to split the directors titles  
     # call pardo, pipe query results to pardo
     split_director_pcoll = query_results | 'Return title: director dictonaries' >> beam.ParDo(SplitDirectorsFn()) 

     # write PCollection to log file
     split_director_pcoll | 'Write log 1' >> WriteToText('formatted_director_pcoll.txt') 

     dataset_id = 'bollywood_modeled'
     table_id = 'bollywoodDirects_Beam'
     schema_id = 'title:STRING, director:STRING'

     # write PCollection to new BQ table
     split_director_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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