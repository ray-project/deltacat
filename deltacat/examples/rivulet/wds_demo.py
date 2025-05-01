import os
import torch
#rom transformers import AutoFeatureExtractor, AutoModelForImageClassification
from deltacat.storage.rivulet import Dataset
import pyarrow as pa
from typing import List
from PIL import Image
import io
import pathlib
from deltacat.storage.rivulet.schema.schema import Datatype
#from transformers import AutoImageProcessor, AutoModelForImageClassification


tar_path = "deltacat/tests/test_utils/resources/imagenet1k-train-0000.tar"
ds = Dataset.from_webdataset(
   name="bird_species_test",
   file_uri=tar_path,
   merge_keys="filename"
)


print(ds.fields)


processor = AutoImageProcessor.from_pretrained("chriamue/bird-species-classifier")
model = AutoModelForImageClassification.from_pretrained("chriamue/bird-species-classifier")
model.eval()


def compute_bird_species(batch: pa.RecordBatch) -> List[str]:
   image_column = batch.column("filename").to_pylist()
   pil_images = [Image.open(str(tar_path[:-4] + '/' + img_bytes.split('/')[1].replace("JPEG","jpg"))).convert("RGB") for img_bytes in image_column if "JPEG" in img_bytes]
   if pil_images:
       inputs = processor(images=pil_images, return_tensors="pt")
       with torch.no_grad():
           outputs = model(**inputs)


       predicted_ids = torch.argmax(outputs.logits, dim=1).tolist()
       predicted_labels = [model.config.id2label[idx] for idx in predicted_ids]
       return predicted_labels
   
ds.add_fields([
   ("filename", Datatype.string()),
   ("bird_species", Datatype.string())
], schema_name="bird_species_classifier", merge_keys=["filename"])


dataset_writer = ds.writer(schema_name="bird_species_classifier")


## iterate through every batch
for batch in ds.scan().to_arrow():
   print(batch)
   filenames = batch.column("filename").to_pylist()
   bird_labels = compute_bird_species(batch)
   rows_to_write = []
   if bird_labels:
       rows_to_write = [
           {
               "filename": fname,
               "bird_species": bird_labels[idx]
           }
           for idx, fname in enumerate(filenames)
       ]
   print("ROWS", rows_to_write)
   dataset_writer.write(rows_to_write)


dataset_writer.flush() # not implemented
ds.export(file_uri="./bird_classification_species_predictions.json", format="json")


print("Bird species classification complete.")