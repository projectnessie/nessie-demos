#!/bin/bash

INPUT_IMAGE_NAME=$1
INPUT_REPO_DIR=$2
INPUT_NOTEBOOK_USER=$3
INPUT_BINDER_DOCKER_FILE="binder/Dockerfile"

# Check for docker image name
if [ -z "$INPUT_IMAGE_NAME" ]; then
  echo "Input the image name for the docker image as the first parameter"
  exit 1
fi 

# Check for repo dir
if [ -z "$INPUT_REPO_DIR" ]; then
  echo "Input the repo dir to be used to build docker image as the second parameter"
  exit 1
fi 

# Set jupyter username
if [ -z "$INPUT_NOTEBOOK_USER" ]; 
    then
        # The default username being used by mybinder.org is 'jovyan'
        NB_USER="jovyan"

    else
        NB_USER="${INPUT_NOTEBOOK_USER}"
fi

# Get GH commet SHA
SHORT_SHA=$(echo "${GITHUB_SHA}" | cut -c1-12)

# Set Docker image full name
DOCKER_FULL_IMAGE_NAME="${INPUT_IMAGE_NAME}:${SHORT_SHA}"

# Build and push docker image
#jupyter-repo2docker --image-name ${DOCKER_FULL_IMAGE_NAME} --no-run --push --user-id 1000 --user-name ${NB_USER} ${INPUT_REPO_DIR}

# Update dockerfile
START_TEMPLATE="##START_BASE_IMAGE##"
END_TEMPLATE="##END_BASE_IMAGE##"
DOCKER_FROM_STATEMENT="FROM ${DOCKER_FULL_IMAGE_NAME}"
sed -i "/${START_TEMPLATE}/,/${END_TEMPLATE}/c\\${START_TEMPLATE}\n${DOCKER_FROM_STATEMENT}\n${END_TEMPLATE}" ${INPUT_BINDER_DOCKER_FILE}

cat ${INPUT_BINDER_DOCKER_FILE}

# Commit the updated Dockerfile
git config user.email "github-actions[bot]@users.noreply.github.com"
git config user.name "github-actions[bot]"
git add ${INPUT_BINDER_DOCKER_FILE}
git commit -m "Update image tag"
git push