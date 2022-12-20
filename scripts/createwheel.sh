#!/bin/bash

echo "Creating wheel"
python deltacat/setup.py sdist bdist_wheel
