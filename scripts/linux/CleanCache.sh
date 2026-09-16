#!/bin/bash

# This script removes all bin and obj folders from the project.

echo "Cleaning all 'bin' and 'obj' folders..."

# Find and remove all 'bin' directories
find . -type d -name "bin" -exec rm -rf {} + 

# Find and remove all 'obj' directories
find . -type d -name "obj" -exec rm -rf {} + 

echo "Cache cleaning complete."