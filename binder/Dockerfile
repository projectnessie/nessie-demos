# Base image to use for demos.

# Tag will be automatically generated through pre-commit hook if any changes
# happened in the docker/ folder
FROM ghcr.io/projectnessie/nessie-binder-demos:7a09c2b037867ca6880b453ec576389f908a0407

# Create the necessary folders for the demo, this will be created and ownded by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/ notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/
