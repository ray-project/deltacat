from apache_beam.options.pipeline_options import PipelineOptions

from deltacat.examples.experimental.iceberg.converter.beam import app


if __name__ == "__main__":
    import argparse
    import logging
    import os

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description="DeltaCat Beam Iceberg Converter Example using REST Catalog",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start REST catalog server first:
  docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest
  
  # Write sample data with DeltaCat optimizer (automatic duplicate resolution):
  python main.py --mode write --input-text "Custom Name"
  
  # Read data back:
  python main.py --mode read
  
  # Use custom REST catalog server:
  python main.py --mode write --rest-uri http://localhost:9000
  
  # Use custom warehouse path:
  python main.py --mode write --warehouse-path /tmp/my_warehouse
        """,
    )
    
    parser.add_argument(
        "--input-text",
        default="Example User",
        help="Custom name to include in sample data (default: Example User).",
    )
    
    parser.add_argument(
        "--mode",
        default="write",
        choices=["write", "read"],
        help="Pipeline mode: 'write' to write data to Iceberg table, 'read' to read from it (default: write).",
    )
    
    parser.add_argument(
        "--rest-uri",
        default="http://localhost:8181",
        help="REST catalog server URI (default: http://localhost:8181).",
    )
    
    parser.add_argument(
        "--warehouse-path",
        default=None,
        help="Custom warehouse path (default: temporary directory).",
    )
    
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        beam_args,
        save_main_session=True, 
        #setup_file="./setup.py",
    )
    
    print("🚀 DeltaCat Beam Iceberg Converter Example")
    print("=" * 50)
    print(f"Mode: {args.mode}")
    print(f"REST Catalog URI: {args.rest_uri}")
    print(f"Warehouse Path: {args.warehouse_path or 'temporary directory'}")
    print(f"Input Text: {args.input_text}")
    print()
    
    # Remind user about prerequisites
    if args.mode == "write":
        print("📋 Prerequisites:")
        print("   Make sure the Iceberg REST catalog server is running:")
        print("   docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest")
        print()
    
    app.run(
        input_text=args.input_text,
        beam_options=beam_options,
        mode=args.mode,
        rest_catalog_uri=args.rest_uri,
        warehouse_path=args.warehouse_path,
    )