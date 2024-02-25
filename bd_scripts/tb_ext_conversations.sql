CREATE OR REPLACE EXTERNAL TABLE `aotelop-challenge.aotelop_edwh_raw.tb_ext_conversations` 
OPTIONS (
  format = 'JSON',
  uris = ['gs://aotelop_conversation_logs/json/*.jsonl']
);