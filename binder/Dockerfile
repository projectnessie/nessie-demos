# Base image to use for demos.

# Tag will be automatically generated through pre-commit hook if any changes
# happened in the docker/ folder
FROM ghcr.io/projectnessie/nessie-binder-demos:b06db7472c61543d26e3ce8e3e8a3ea6ef488fef

# Create the necessary folders for the demo, this will be created and owned by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/*.ipynb notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/
