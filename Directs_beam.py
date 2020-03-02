import logging
import apache_beam as beam
from apache_beam.io import WriteToText

class FormatTitlesFn(beam.DoFn): # Split the title's directors 
  def process(self, element): 
    director_record = element
    directors = director_record.get('directors')
    title = director_record.get('tConst')
    # print('director: ' + director) # for debugging
    
    directors = directors.strip() # remove white space
    directors = directors.split(',') # create array with director's nConst as elements
    director_record['directors'] = directors[0]
    # create key, value pairs
    directors_tuple = (title, director_record) # form a (key, value) tuple
    return [directors_tuple]

class DedupTitlesFn(beam.DoFn): # PArDO to get rid duplicates 
  def process(self, element):
    tConst, directors = element # student_obj is an _UnwindowedValues type
    directors_list = list(directors) # cast to list type to extract record
    director_record = directors_list[0] # grab first director record
#     print('director_record: ' + str(director_record))
    return [director_record]  
           
def run():
     PROJECT_ID = 'starry-center-266501' # change to your project id # creating pipeline through direct runner

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT directors, tConst FROM imdb_modeled.Directs limit 100' # passing a query. Shouldn't process more than 1000 records w DR
        # directors is an array of strings, tConst is a string 
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # apply ParDo to format the directors titles  
     # call pardo, pipe query results to pardo
     formatted_titles_pcoll = query_results | 'Format Titles' >> beam.ParDo(FormatTitlesFn()) 

     # write PCollection to log file
     formatted_titles_pcoll | 'Write log 1' >> WriteToText('formatted_titles_pcoll.txt') 

     # group students by sid
     grouped_titles_pcoll = formatted_titles_pcoll | 'Group by tConst' >> beam.GroupByKey() # every input to GB needs to be a key-val pair
        # gives a new pcollect with keys
     # write PCollection to log file
     grouped_titles_pcoll | 'Write log 2' >> WriteToText('grouped_titles_pcoll.txt')
        # want to know what grouped_student_pcoll
     # remove duplicate student records
     distinct_title_pcoll = grouped_titles_pcoll | 'Dedup student records' >> beam.ParDo(DedupTitlesFn()) # pipe to another ParDo

     # write PCollection to log file
     distinct_title_pcoll | 'Write log 3' >> WriteToText('distinct_title_pcoll.txt')

     dataset_id = 'imdb_modeled'
     table_id = 'Directs_Beam'
     schema_id = 'directors:STRING, tConst:STRING'

     # write PCollection to new BQ table
     distinct_title_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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