# Base image to use for demos in (my)binder.

# Tag will be automatically generated through pre-commit hook if any changes
# happened in the docker/ folder
FROM ghcr.io/projectnessie/nessie-binder-demos:dd32c4413d91c22676121f62119bcc7f167e4752

# Create the necessary folders for the demo, this will be created and owned by {NB_USER}
RUN mkdir -p notebooks && mkdir -p datasets

# Copy the python notebooks into the existing notebooks folder
COPY notebooks/*.ipynb notebooks/

# Copy all the needed datasets for the demos
COPY datasets/ datasets/

# install the notebook package
RUN pip install --no-cache --upgrade pip && \
    pip install --no-cache notebook jupyterlab traitlets

ARG NB_USER
ARG NB_UID

# Verify that the requested notebook user name+id match the current ones
RUN id -nu | grep -q $NB_USER
RUN id -u | grep -q $NB_UID

# Add a custom entrypoint script (required by binder due to custom args)
USER root
COPY binder/entrypoint-demos.sh /entrypoint-demos.sh
RUN chmod +x /entrypoint-demos.sh
WORKDIR ${HOME}
USER ${USER}
ENTRYPOINT ["/entrypoint-demos.sh"]
