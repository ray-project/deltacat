import os
import json
import tarfile
import io
import numpy as np
from PIL import Image

# Create mock data directory
if not os.path.exists('mock_data'):
    os.makedirs('mock_data')

# Sample IDs for medical papers (similar to the example)
sample_ids = [
    "",
    "PMC4129566_00003",
    "PMC4872614_00002",
    "PMC5018106_00004",
    "PMC5360221_00001"
]

# Generate sample data
for sample_id in sample_ids:
    # Generate a mock medical image (simple shapes with random colors)
    img = Image.new('RGB', (256, 256), color=(255, 255, 255))
    
    # Add some random shapes to make it look like a medical scan
    img_array = np.array(img)
    
    # Add a circular region 
    center_x, center_y = np.random.randint(80, 176, 2)
    radius = np.random.randint(40, 70)
    
    for x in range(img_array.shape[0]):
        for y in range(img_array.shape[1]):
            if (x - center_x)**2 + (y - center_y)**2 < radius**2:
                # Make it look like an anomaly or region of interest
                img_array[x, y] = (np.random.randint(80, 150), 
                                   np.random.randint(80, 150), 
                                   np.random.randint(150, 220))
    
    # Save the image
    img = Image.fromarray(img_array)
    img.save(f'mock_data/{sample_id}.png')
    
    # Generate corresponding JSON metadata
    metadata = {
        "paper_id": sample_id.split('_')[0],
        "figure_id": sample_id,
        "caption": f"Medical scan showing {['lung tissue', 'brain section', 'liver sample', 'kidney structure', 'cell culture'][np.random.randint(0, 5)]}",
        "modality": np.random.choice(["MRI", "CT Scan", "X-ray", "Ultrasound", "Microscopy"]),
        "patient_age_group": np.random.choice(["pediatric", "adult", "geriatric"]),
        "image_size": [256, 256],
        "color_space": "RGB",
        "publication_year": np.random.randint(2010, 2023),
        "annotations": [
            {
                "label": np.random.choice(["tumor", "lesion", "inflammation", "normal tissue", "artifact"]),
                "confidence": round(np.random.uniform(0.7, 0.99), 2),
                "bounding_box": [
                    center_x - radius,
                    center_y - radius,
                    center_x + radius,
                    center_y + radius
                ]
            }
        ]
    }
    
    with open(f'mock_data/{sample_id}.json', 'w') as f:
        json.dump(metadata, f, indent=2)