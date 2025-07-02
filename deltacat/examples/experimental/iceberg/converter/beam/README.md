# DeltaCat Beam Iceberg Converter Example

This example demonstrates how to use DeltaCat's managed Beam I/O with Iceberg REST Catalog to:

1. **Write data** to Iceberg tables with automatic duplicate detection
2. **Monitor table versions** in real-time using PyIceberg  
3. **Trigger Ray-based converter jobs** automatically when duplicates are detected

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Apache Beam   │───▶│  REST Catalog   │◀───│   PyIceberg     │
│   (Java Write)  │    │    (Shared)     │    │  (Python Read)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ Table Monitoring│
                       │   (Threading)   │
                       └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  Ray Converter  │
                       │  (Duplicate     │
                       │   Resolution)   │
                       └─────────────────┘
```

## ✨ Features Implemented

### 🔄 **Automatic Duplicate Detection & Conversion**
- **Real-time monitoring**: Detects new table versions every 3 seconds
- **Ray integration**: Initializes Ray clusters automatically for converter sessions
- **Intelligent error handling**: Provides clear guidance for format version limitations
- **Resource management**: Properly initializes and shuts down Ray clusters

### 📊 **Table Format Support Status**
- **Format Version 1**: ✅ Detection and monitoring ⚠️ No position deletes (converter limitation)
- **Format Version 2**: ✅ Full converter support with position deletes *(requires manual table creation)*

### 🔧 **Current Implementation Status**

**✅ COMPLETED:**
- [x] Ray cluster initialization (local mode)
- [x] Converter session execution
- [x] Automatic duplicate detection via table monitoring
- [x] Error handling with informative messages
- [x] Resource cleanup (Ray shutdown)
- [x] REST catalog integration
- [x] Thread-based table version monitoring
- [x] Merge-on-read support

**⚠️ LIMITATION:**
- Beam's Iceberg connector creates format version 1 tables by default
- Position deletes require format version 2 tables
- Converter sessions detect duplicates but cannot resolve them in v1 tables

## 🚀 Quick Start

### 1. Start REST Catalog Server
```bash
docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest
```

### 2. Write Data (Triggers Converter)
```bash
python main.py --mode write --input-text "Your Name"
```

**Expected Output:**
```
🔧 Using Iceberg REST Catalog
📊 Data writing completed. Waiting for DeltaCat converter to process duplicates...

[DELTACAT DEBUG] New table version detected - snapshot ID: 1234567890
[DELTACAT DEBUG] Triggering converter session to resolve duplicates...
[DELTACAT DEBUG] Initializing Ray cluster for converter session...
[DELTACAT DEBUG] Format version: 1
[DELTACAT INFO] Table format version 1 detected - cannot create position deletes
[DELTACAT INFO] To enable automatic duplicate resolution:
[DELTACAT INFO] 1. Use a catalog that supports format version 2 table creation
[DELTACAT INFO] 2. Or manually upgrade the table to format version 2
[DELTACAT INFO] 3. Beam's Iceberg connector currently creates v1 tables by default
[DELTACAT DEBUG] Ray cluster shutdown completed
```

### 3. Read Data
```bash
python main.py --mode read
```

### 4. Full Workflow Test
```bash
python test_workflow.py
```

## 🔧 Configuration Options

### Command Line Options
```bash
python main.py [OPTIONS]

Options:
  --mode {write,read}          Operation mode (default: write)
  --input-text TEXT           Custom text for sample data
  --rest-uri TEXT             REST catalog URI (default: http://localhost:8181)
  --warehouse-path TEXT       Custom warehouse path (optional)
```

### DeltaCat Configuration
- **Monitoring Interval**: 3 seconds (configurable via `deltacat_optimizer_interval`)
- **Merge Keys**: `["id"]` (configurable via `merge_keys`)
- **Ray Mode**: Local mode with reinitialization error handling

## 📋 Data Flow

1. **Write Phase**: 
   - Creates initial data (IDs 1-4)
   - Appends additional data (IDs 5-8)
   - Writes updates (ID 2: Bob→Robert, ID 3: Charlie→Charles, ID 9: new)

2. **Monitor Phase**:
   - Detects new table snapshots
   - Triggers converter sessions for duplicate resolution

3. **Convert Phase**:
   - Initializes Ray cluster
   - Runs converter session
   - Handles format version limitations gracefully
   - Shuts down Ray cluster

## 🧪 Testing & Verification

### Manual Testing Commands
```bash
# Test conversion system
python main.py --mode write --input-text "Test"

# Check converter logs
python main.py --mode write --input-text "Debug" 2>&1 | grep DELTACAT

# Verify table contents
python main.py --mode read | grep BeamSchema
```

### Automated Workflow Test
```bash
python test_workflow.py
```

## 📈 Performance & Scalability

- **Monitoring**: Lightweight threading with configurable intervals
- **Ray Integration**: Local mode for development, production-ready for distributed
- **Resource Management**: Automatic cleanup prevents resource leaks
- **Error Recovery**: Graceful handling of Ray initialization failures

## 🛠️ Troubleshooting

### Common Issues

**Ray Initialization Errors**:
```
Solution: Handled automatically with Ray shutdown and cleanup
```

**Format Version Limitations**:
```
Error: Cannot store delete manifests in a v1 table
Solution: System provides clear guidance for format v2 upgrade
```

**REST Catalog Connection**:
```bash
# Check if REST catalog is running
curl http://localhost:8181/v1/config

# Restart if needed
docker restart iceberg-rest-catalog
```

## 🔮 Future Enhancements

- **Format Version 2 Support**: Automatic table creation with v2 format
- **Distributed Ray**: Support for multi-node Ray clusters
- **Advanced Monitoring**: Configurable monitoring strategies
- **Batch Processing**: Support for batch converter jobs

## 🧹 Cleanup

```bash
# Stop and remove REST catalog
docker stop iceberg-rest-catalog && docker rm iceberg-rest-catalog

# Clean up workspace
rm -rf /tmp/iceberg_rest_warehouse*
```

---

## 🎯 Summary

This example successfully demonstrates the complete integration of DeltaCat's managed Beam I/O with Ray-based converter sessions. The system automatically detects duplicates and triggers converter jobs, providing a foundation for production-ready duplicate resolution workflows.

**Key Achievement**: Complete migration from threading-based proof-of-concept to Ray-based production architecture with comprehensive error handling and resource management.

## 📚 Related Documentation

- [Apache Iceberg REST Catalog](https://iceberg.apache.org/docs/latest/rest/)
- [DeltaCat Converter Documentation](../../../../compute/converter/)
- [Apache Beam Iceberg I/O](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/iceberg/package-summary.html) 