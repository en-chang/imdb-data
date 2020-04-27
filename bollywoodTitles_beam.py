import logging
import apache_beam as beam
from apache_beam.io import WriteToText


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
    return [{'title':title, 'date':date, 'croresGrossed':crores}]

    
          
def run():
     PROJECT_ID = 'starry-center-266501' # change to your project id # creating pipeline through direct runner

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM bollywood_modeled.bollywoodTitles limit 100' # query the bollywood_modeled table
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True) # direct runner is not running in parallel on several workers. DR is local

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source) # read results and assign them to a new p-collection

     # apply ParDo to split the directors titles  
     # call pardo, pipe query results to pardo
     format_date_pcoll = query_results | 'format dates' >> beam.ParDo(FormatDateFn()) 

     # write PCollection to log file
     format_date_pcoll | 'Write log 1' >> WriteToText('formated_dates_pcoll.txt') 

     dataset_id = 'bollywood_modeled'
     table_id = 'bollywoodTitles_Beam'
     schema_id = 'title:STRING, date:DATE, croresGrossed:NUMERIC'

     # write PCollection to new BQ table
     format_date_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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