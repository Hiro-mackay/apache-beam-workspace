import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ComputeWordLength(beam.DoFn):
  def __init__(self):
    pass

  def process(self, element):
    yield len(element)


def main():
  options = PipelineOptions(
    runner = "DirectRunner"
  )
  
  p = beam.Pipeline(options=options)
  
  (
    p
    | "ReadText" >> beam.io.ReadFromText("./input.txt")
    | "ComputeWordLength" >> beam.ParDo(ComputeWordLength())
    | "WriteToText" >> beam.io.WriteToText("./output", file_name_suffix=".txt", shard_name_template="")
  )
  
  p.run()
  
if __name__ == "__main__":
  main()