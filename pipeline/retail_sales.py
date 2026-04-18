import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import argparse

class TransformData(beam.DoFn):
    def process(self, element):
        fields = element.split(',')

        if fields[0] == 'order_id':
            return

        price = float(fields[3])
        quantity = int(fields[4])
        revenue = price * quantity

        yield {
            "order_id": fields[0],
            "product_id": fields[1],
            "category": fields[2],
            "price": price,
            "quantity": quantity,
            "order_date": fields[5],
            "revenue": revenue,
            "load_date": datetime.utcnow().strftime('%Y-%m-%d')
        }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:

        (
            p
            | "Read CSV" >> beam.io.ReadFromText(args.input)
            | "Transform" >> beam.ParDo(TransformData())
            | "Write to BQ" >> beam.io.WriteToBigQuery(
                args.output,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()