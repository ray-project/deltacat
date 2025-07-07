"""
Spark SQL utilities for Iceberg table operations.

This module provides Beam DoFn classes that use Spark SQL to work with Iceberg tables,
"""

import os
import apache_beam as beam
from apache_beam import Row


class SparkSQLIcebergRead(beam.DoFn):
    """
    Custom Beam DoFn that uses Spark SQL to read Iceberg tables.
    """

    def __init__(
        self,
        table_name: str,
        catalog_uri: str = "http://localhost:8181",
        warehouse: str = "warehouse/",
    ):
        """
        Initialize the Spark SQL reader.

        Args:
            table_name: Name of the Iceberg table
            catalog_uri: URI of the Iceberg REST catalog
            warehouse: Warehouse path
        """
        self.table_name = table_name
        self.catalog_uri = catalog_uri
        self.warehouse = warehouse
        self.spark = None

    def setup(self):
        """Set up Spark session (called once per worker)."""
        try:
            from pyspark.sql import SparkSession
            import importlib.metadata

            # Get Spark version for dependency resolution
            try:
                spark_version = ".".join(
                    importlib.metadata.version("pyspark").split(".")[:2]
                )
            except Exception:
                spark_version = "3.5"  # Default fallback

            scala_version = "2.12"
            iceberg_version = "1.6.0"

            print(f"🔧 Setting up Spark session for reading {self.table_name}")
            print(f"   - Spark version: {spark_version}")
            print(f"   - Iceberg version: {iceberg_version}")

            # Set Spark packages for Iceberg runtime
            os.environ["PYSPARK_SUBMIT_ARGS"] = (
                f"--packages org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version} "
                f"pyspark-shell"
            )

            # Create Spark session with Iceberg REST catalog configuration
            self.spark = (
                SparkSession.builder.appName(f"DeltaCAT Read - {self.table_name}")
                .config("spark.sql.session.timeZone", "UTC")
                .config(
                    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                # Configure REST catalog
                .config(
                    "spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog"
                )
                .config("spark.sql.catalog.rest.type", "rest")
                .config("spark.sql.catalog.rest.uri", self.catalog_uri)
                .config("spark.sql.catalog.rest.warehouse", self.warehouse)
                # Set REST as default catalog
                .config("spark.sql.defaultCatalog", "rest")
                # Local mode configuration (within Beam workers)
                .config("spark.master", "local[1]")  # Single thread per worker
                .config("spark.sql.adaptive.enabled", "true")
                # Networking binding
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                .getOrCreate()
            )

            print(f"✅ Spark session created successfully")

        except Exception as e:
            print(f"❌ Failed to set up Spark session: {e}")
            raise

    def teardown(self):
        """Clean up Spark session (called once per worker)."""
        if self.spark:
            try:
                self.spark.stop()
                print("✅ Spark session stopped")
            except Exception as e:
                print(f"⚠️ Error stopping Spark session: {e}")

    def process(self, element):
        """
        Process element (read from Iceberg table using Spark SQL).

        Args:
            element: Input element (not used, just triggers the read)

        Yields:
            Records from the Iceberg table
        """
        try:
            if not self.spark:
                raise RuntimeError("Spark session not initialized")

            print(f"📖 Reading table {self.table_name} using Spark SQL")

            # Read from Iceberg table using Spark SQL
            df = self.spark.sql(f"SELECT * FROM {self.table_name}")

            # Collect all records
            records = df.collect()

            print(f"📊 Successfully read {len(records)} records from {self.table_name}")

            # Convert Spark rows to Beam Row objects and yield
            for row in records:
                row_dict = row.asDict()
                # Convert to Beam Row for consistency with write mode
                beam_row = Row(**row_dict)
                yield beam_row

        except Exception as e:
            print(f"❌ Failed to read from table {self.table_name}: {e}")
            raise


class SparkSQLIcebergRewrite(beam.DoFn):
    """
    Custom Beam DoFn that uses Spark SQL to rewrite Iceberg table data files.

    This uses Spark's rewrite_data_files procedure to materialize positional deletes
    by rewriting data files. The result is a "clean" table without positional deletes.
    """

    def __init__(self, catalog_uri, warehouse_path, table_name):
        self.catalog_uri = catalog_uri
        self.warehouse_path = warehouse_path
        self.table_name = table_name

    def setup(self):
        """Initialize Spark session for rewrite operations."""
        try:
            from pyspark.sql import SparkSession
            import importlib.metadata

            print(f"🔧 Setting up Spark session for rewriting {self.table_name}")

            # Detect Spark version for appropriate Iceberg runtime
            spark_version = importlib.metadata.version("pyspark")
            major_minor = ".".join(spark_version.split(".")[:2])
            print(f"   - Spark version: {major_minor}")
            print(f"   - Iceberg version: 1.6.0")

            # Configure Spark with Iceberg
            self.spark = (
                SparkSession.builder.appName("IcebergRewrite")
                .config(
                    "spark.jars.packages",
                    f"org.apache.iceberg:iceberg-spark-runtime-{major_minor}_2.12:1.6.0",
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.iceberg.spark.SparkSessionCatalog",
                )
                .config("spark.sql.catalog.spark_catalog.type", "rest")
                .config("spark.sql.catalog.spark_catalog.uri", self.catalog_uri)
                .config(
                    "spark.sql.catalog.spark_catalog.warehouse", self.warehouse_path
                )
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )

            print("✅ Spark session created successfully")

        except ImportError as e:
            raise RuntimeError(
                f"PySpark is required for rewrite mode. Install with: pip install pyspark"
            ) from e
        except Exception as e:
            raise RuntimeError(f"Failed to create Spark session: {e}") from e

    def process(self, element):
        """Rewrite table data files to materialize positional deletes."""
        try:
            print(
                f"📋 Rewriting table {self.table_name} to materialize positional deletes"
            )

            # Use Spark's rewrite_data_files procedure with delete_file_threshold=1
            # This forces rewrite even when there's only 1 positional delete file
            rewrite_sql = f"""
            CALL spark_catalog.system.rewrite_data_files(
                table => '{self.table_name}',
                options => map('delete-file-threshold', '1')
            )
            """

            print(f"🔄 Executing rewrite procedure with delete_file_threshold=1...")
            print(f"   SQL: {rewrite_sql.strip()}")
            print(
                f"   Rationale: Forces rewrite even with single positional delete file"
            )

            result = self.spark.sql(rewrite_sql)

            # Collect results to see what was rewritten
            rewrite_result = result.collect()[0]
            print(f"📊 Rewrite result: {rewrite_result}")

            # Check if we actually rewrote anything
            if rewrite_result.rewritten_data_files_count > 0:
                print(
                    f"✅ Successfully rewrote {rewrite_result.rewritten_data_files_count} data files"
                )
                print(
                    f"   - Added {rewrite_result.added_data_files_count} new data files"
                )
                print(f"   - Rewrote {rewrite_result.rewritten_bytes_count} bytes")
                print(f"   - Positional deletes have been materialized!")
            else:
                print(f"⚠️  No files were rewritten (rewritten_data_files_count=0)")
                print(f"   - This may indicate no positional deletes exist")
                print(f"   - Or the table may already be in optimal state")

            yield f"Rewrite completed for {self.table_name}"

        except Exception as e:
            print(f"❌ Error during rewrite: {e}")
            import traceback

            traceback.print_exc()
            yield f"Rewrite failed for {self.table_name}: {e}"

    def teardown(self):
        """Clean up Spark session."""
        if hasattr(self, "spark"):
            print("✅ Spark session stopped")
            self.spark.stop()
