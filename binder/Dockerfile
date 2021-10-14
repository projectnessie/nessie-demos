# Base image to use for demos.

# Tag will be automatically generated through pre-commit hook if any changes
# happened in the docker/ folder
FROM projectnessie/nessie-demos-base-image:468352fe9f08d4885c199d43521cb1d95513d572

# Create the necessary folders for the demo, this will be created and ownded by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/ notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/
