import os
import argparse
import logging
from google.cloud import bigquery




def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://aotelop_conversation_logs/txt/*.txt',
        help='Input files to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://aotelop_conversation_logs/json/',
        help='Output path to write results to.')
    parser.add_argument(
        '--bqproject',
        dest='bqproject',
        default='aotelop-challenge',
        help='BigQuery project')
    known_args, pipeline_args = parser.parse_known_args(argv)
 

    #Run beam
    os.system(f'python dataflow/beam_txt_to_json_by_file.py --output {known_args.output} --input {known_args.input}')

    #Run create external table
    client = bigquery.Client()
    query = """
    CREATE OR REPLACE EXTERNAL TABLE `""" + known_args.bqproject + """.aotelop_edwh_raw.tb_ext_conversations` 
    OPTIONS (
      format = 'JSON',
      uris = ['""" + known_args.output + """*.json']
    );
    """
    print(query)
    query_result = client.query(query)
    print(f'Result: {query_result}')


    #Run dbt
    os.environ["AOTELOP_CHALLENGE_BQ_PROJECT"] = known_args.bqproject
    os.chdir('dbt/dbt_challenge')
    os.system('dbt run')

    #Show result
    query = """select * from `""" + known_args.bqproject + """.aotelop_edwh_gold.tb_gl_conversations_metrics`"""
    query_result = client.query(query)
    for row in query_result:
        print(f'Mean: {row[0]} seconds')
    


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()