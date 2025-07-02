
from apache_beam.options.pipeline_options import PipelineOptions

from deltacat.examples.experimental.iceberg.converter.beam import app


if __name__ == "__main__":
    import argparse
    import logging

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-text",
        default="Default input text",
        help="Input text to write to Iceberg table.",
    )
    parser.add_argument(
        "--mode",
        default="write",
        choices=["write", "read"],
        help="Pipeline mode: 'write' to write data to Iceberg table, 'read' to read from it.",
    )
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        save_main_session=True, 
        #setup_file="./setup.py",
    )
    app.run(
        input_text=args.input_text,
        beam_options=beam_options,
        mode=args.mode,
    )