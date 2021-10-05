# We use the pre-existing Nessie Demos Docker image that through CI
FROM omarsmak/nessie-demos-base-image:1

# Create the necessary folders for the demo, this will be created and ownded by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/ notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/