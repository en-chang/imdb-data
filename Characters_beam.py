import logging
import apache_beam as beam
from apache_beam.io import WriteToText

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
     PROJECT_ID = 'starry-center-266501' # change to your project id # creating pipeline through direct runner

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT tConst, nConst, job, characters FROM imdb_modeled.Characters limit 100' # passing a query. Shouldn't process more than 1000 records w DR
        # characters is an array of strings, tConst and nConst are strings 
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # apply ParDo to split the directors titles  
     # call pardo, pipe query results to pardo
     split_characters_pcoll = query_results | 'Return title: director i dictonaries' >> beam.ParDo(SplitCharactersFn()) 

     # write PCollection to log file
     split_characters_pcoll | 'Write log 1' >> WriteToText('split_characters_pcoll.txt') 

     dataset_id = 'imdb_modeled'
     table_id = 'Characters_Beam'
     schema_id = 'tConst:STRING, nConst:STRING, job:STRING, characters:STRING'

     # write PCollection to new BQ table
     split_characters_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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
