import logging
import apache_beam as beam
from apache_beam.io import WriteToText


class SplitTitlesFn(beam.DoFn):
  def process(self, element):  
    castmemb_record = element
    name = castmemb_record.get('nConst')
    titles = castmemb_record.get('knownForTitles')
    titles = titles.strip()
    titles = titles.split(',')
    title_dicts = []
    for title in titles:
      title_dicts.append({'nConst':name, 'tConst':title})
    return(title_dicts)

          
def run():
     PROJECT_ID = 'starry-center-266501' # change to your project id # creating pipeline through direct runner

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT nConst, knownForTitles FROM imdb_modeled.People limit 100' # passing a query. Shouldn't process more than 1000 records w DR
        # directors is an array of strings, tConst is a string 
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # apply ParDo to split the directors titles  
     # call pardo, pipe query results to pardo
     split_knownTitles_pcoll = query_results | 'Return castMember: title i dictonaries' >> beam.ParDo(SplitTitlesFn()) 

     # write PCollection to log file
     split_knownTitles_pcoll | 'Write log 1' >> WriteToText('formatted_titles_pcoll.txt') 

     dataset_id = 'imdb_modeled'
     table_id = 'knownForTitles_Beam'
     schema_id = 'nConst:STRING, tConst:STRING'

     # write PCollection to new BQ table
     split_knownTitles_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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