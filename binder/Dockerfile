# Base image to use for demos. DO NOT MANUALLY MODIFY!

##START_BASE_IMAGE##
FROM omarsmak/nessie-demos-base-image:0f79a4c97d30
##END_BASE_IMAGE##

# Create the necessary folders for the demo, this will be created and ownded by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/ notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/
