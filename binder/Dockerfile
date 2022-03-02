# Base image to use for demos.

# Tag will be automatically generated through pre-commit hook if any changes
# happened in the docker/ folder
FROM ghcr.io/projectnessie/nessie-binder-demos:982fe3adb1e8b58263c74d0b3fc4b6b9ab97f652

# Create the necessary folders for the demo, this will be created and owned by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/ notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/
