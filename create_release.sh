#!/bin/bash

# This script packages release artifacts as intended to be hooked up with github actions in order to create pre-releases for internal deployment testing or official releases for daac distribution.

# Package the Lambdas and Layers
echo "Packaging Lambdas and Layers..."
./package_lambdas_and_layers.sh

rm -rf gap_detection_module/artifacts

DATE=$(date +"%m-%d")
ZIP_NAME="gesdisc_cumulus_gap_detection_$DATE.zip"

rm -f "$ZIP_NAME"

# Add entire artifacts folder (preserving its folder structure)
zip -r "$ZIP_NAME" artifacts

# Add contents of terraform folder at root level (no terraform folder inside zip)
cd gap_detection_module
zip -r "../$ZIP_NAME" ./*

echo "Created release archive: $ZIP_NAME"