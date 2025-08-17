import os
import torch
from transformers import AutoFeatureExtractor, AutoModelForImageClassification
from deltacat.storage.rivulet import Dataset
import pyarrow as pa
from typing import List
from PIL import Image
import io
import pathlib
from deltacat.storage.rivulet.schema.schema import Datatype
from transformers import AutoImageProcessor, AutoModelForImageClassification


#tar_path = "deltacat/tests/test_utils/resources/imagenet1k-train-0000.tar"
tar_path = "deltacat/tests/test_utils/resources/nestedjson.tar"

# Load the dataset from the tar file
ds = Dataset.from_webdataset(
    name="bird_species_test",     # Name of the dataset
    file_uri=tar_path,            # Location of the tar file
    merge_keys="filename"         # Merge batches using the 'filename' key
)

# Print the available fields in the dataset
print(ds.fields)

# Load the image processor and classification model from HuggingFace
processor = AutoImageProcessor.from_pretrained("chriamue/bird-species-classifier")  
model = AutoModelForImageClassification.from_pretrained("chriamue/bird-species-classifier")  
model.eval()  

# Function to classify bird species from a record batch
def compute_bird_species(batch: pa.RecordBatch) -> List[str]:
    # Extract the binary image column
    image_column = batch.column("image_binary").to_pylist()

    # Initialize list to store PIL Image objects
    pil_images = []
    for img_binary in image_column:
        try:
            # Convert binary data to image and convert to RGB
            img = Image.open(io.BytesIO(img_binary)).convert("RGB")
            pil_images.append(img)
        except Exception as e:
            # Print error if image decoding fails
            print(f"Error reading image: {e}")

    # If images were successfully decoded
    if pil_images:
        # Preprocess images and run them through the model
        inputs = processor(images=pil_images, return_tensors="pt")
        with torch.no_grad():  # Disable gradient computation
            outputs = model(**inputs)

        # Get the predicted label indices
        predicted_ids = torch.argmax(outputs.logits, dim=1).tolist()

        # Map indices to human-readable class labels
        predicted_labels = [model.config.id2label[idx] for idx in predicted_ids]

        return predicted_labels
    else:
        # Return empty list if no images were valid
        return []

# Add new fields to the dataset: filename and predicted bird species
ds.add_fields([
    ("filename", Datatype.string()),           # String type for filename
    ("bird_species", Datatype.string())        # String type for predicted label
], schema_name="bird_species_classifier", merge_keys=["filename"])  # Schema name and merge key

# Initialize writer to store output under the new schema
dataset_writer = ds.writer(schema_name="bird_species_classifier")

# Iterate over each Arrow batch in the dataset
for batch in ds.scan().to_arrow():
    print(batch)  # Print the batch contents
    filenames = batch.column("filename").to_pylist()  # Extract filenames
    bird_labels = compute_bird_species(batch)  # Run classification on batch

    rows_to_write = []  # Prepare rows to be written
    if bird_labels:
        # Create a list of dictionaries combining filename and predicted species
        rows_to_write = [
            {
                "filename": fname,
                "bird_species": bird_labels[idx]
            }
            for idx, fname in enumerate(filenames)
        ]
    print("ROWS", rows_to_write)  # Print rows to be written
    dataset_writer.write(rows_to_write)  # Write the output to dataset

dataset_writer.flush() 

# Export the results to a local JSON file
ds.export(file_uri="./bird_classification_species_predictions.json", format="json")

print("Bird species classification complete.")
