import argparse
import logging
import json
import apache_beam as beam


#from apache_beam.io import ReadFromTextWithFilename
#from apache_beam.io import WriteToText
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import storage

  
def parse_file(file, output_path):
    filename = file.metadata
    file_content = file.read().decode('utf-8')
    filename = filename.path.split("/")[4].replace(".txt", "")
    json_file = []
    json_file_str = ""
    message = ""
    lines = file_content.splitlines()



    for line in lines:
      if line.startswith("###"):
          clean_line = line.replace("###", "")
          msg_date = clean_line.split(" | ")[0].strip()
          interlocutor = clean_line.split(" | ")[1].strip()          
      elif line != '':
         message += line
      else: #msg finished. Close object
        line = {"msg_date" : msg_date, "Interlocutor": interlocutor, "conversation_id": filename, "msg_text": message}
        json_file_str += json.dumps(line) + "\n"
        json_file.append(json.dumps(line))
        message = ""

    line = {"msg_date" : msg_date, "Interlocutor": interlocutor, "conversation_id": filename, "msg_text": message}
    json_file.append(json.dumps(line))
    json_file_str += json.dumps(line)

    bucket = storage.Client().get_bucket(output_path.split("/")[2])
    blob = bucket.blob(f'{output_path.split("/")[3]}/{filename}.json')

    blob.upload_from_string(json_file_str, content_type='application/json')

    return json_file






def run(argv=None, save_main_session=True):
    """Main entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://aotelop_conversation_logs/txt/*.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://aotelop_conversation_logs/json/',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)   
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session    
    with beam.Pipeline() as pipeline:
        (pipeline
         | 'MatchFiles' >> fileio.MatchFiles(known_args.input)
         | 'ReadMatches' >> fileio.ReadMatches()
         | 'ProcessFile' >> beam.ParDo(lambda file: parse_file(file, known_args.output))
        )
    #| 'WriteToJson' >> WriteToText(known_args.output, file_name_suffix='.json')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()