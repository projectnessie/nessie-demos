# Base image to use for demos.

# Tag will be automatically generated through pre-commit hook if any changes
# happened in the docker/ folder
FROM ghcr.io/projectnessie/nessie-binder-demos:dd32c4413d91c22676121f62119bcc7f167e4752

LABEL org.opencontainers.image.source=https://github.com/projectnessie/nessie-demos
LABEL org.opencontainers.image.description="Base image for mybinder.org / Jupyter notebook"
LABEL org.opencontainers.image.licenses=Apache-2.0

# Create the necessary folders for the demo, this will be created and owned by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/*.ipynb notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/
