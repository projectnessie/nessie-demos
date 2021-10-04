# We use the pre-existing Nessie Demos Docker image that through CI
FROM omarsmak/nessie-demos-base-image:latest

# Setup jupyter-repo2docker user.
ARG NB_USER

# Copy the python notebooks into the existing notebooks folder
COPY --chown=$NB_USER:$NB_USER notebooks notebooks

# Copy all the needed datasets for the demos
COPY --chown=$NB_USER:$NB_USER datasets datasets 