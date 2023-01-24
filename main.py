import logging
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

GCLOUD_JOB_NAME = 'may-job-name'
GCLOUD_PROJECT = 'my-gcp-project'
GCLOUD_BQ_DATASET = 'my-bq-dataset'
GCLOUD_BQ_TABLE = 'my-bq-table'
GCLOUD_GCS_LOCATION = 'gs://my-bucket'
GCLOUD_TEMP_LOCALTION = GCLOUD_GCS_LOCATION + '/dataflow/tmp'
GCLOUD_REGION = 'asia-northeast1'

class BeamOptions(PipelineOptions):
  """ Custom argv params """
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
      '--input',
      default=f'{GCLOUD_PROJECT}:{GCLOUD_BQ_DATASET}.{GCLOUD_BQ_TABLE}',
      help=(
          'Input BigQuery table to process specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    parser.add_argument(
      '--output',
      default=f'{GCLOUD_GCS_LOCATION}/results/output.txt',
    )

def run():
  options = BeamOptions(
    runner = 'DataflowRunner',
    project = GCLOUD_PROJECT,
    job_name = GCLOUD_JOB_NAME,
    staging_location = GCLOUD_TEMP_LOCALTION,
    temp_location = GCLOUD_TEMP_LOCALTION,
    region = GCLOUD_REGION
  )
  

  
  with beam.Pipeline(options=options) as p:

    # Read the table rows into a PCollection.
    lines = p | 'read' >> beam.io.ReadFromBigQuery(table=options.input)
    lines | "write files" >> beam.io.WriteToText(options.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()