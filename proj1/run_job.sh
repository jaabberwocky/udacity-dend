#!/bin/bash

echo "Resetting tables..."
python create_tables.py
echo "Running etl jobs..."
python etl.py
echo "All done!"
