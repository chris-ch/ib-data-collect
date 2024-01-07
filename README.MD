# Interactive Brokers Products Database
## Initialisation
### Google Cloud project
You need to setup a Google Cloud project and a Firebase Authentication first.

The project is using a secrets config file `secrets.auto.tfvars` used by both
the PyCharm runner and terraform:

```
GOOGLE_PROJECT_ID="<project id>"
GOOGLE_MESSAGING_SENDER_ID=<messaging sender id for Firebird>
GOOGLE_API_KEY="<Google API Key>"
GOOGLE_REGION="us-central1"
GOOGLE_ZONE="us-central1-a"
LOCAL_LOGGING="True"
```

### Development setup
First install all required packages using:

`> poetry update`

This will generate the `poetry.lock` file.
You also need to generate the requirements.txt file in app:

`> poetry export --format requirements.txt --output app/requirements.txt --without-hashes`

You will need to execute the above command any time you modify the poetry packages.

### Local Cloud using Podman
Install Podman and create a machine:

```Shell
# Creating a Host VM and mounting Google credentials directory
podman machine init --cpus=2 --disk-size=12 --memory=4096 -v $HOME/.config/gcloud:/gcloud:ro gcp
podman machine set --rootful gcp
podman machine start gcp
```

#### Using the _Dockerfile_

```Shell
# Building docker image on local repo
podman build -t ibc-docker .
# Starting Docker container on the Host VM using the mounted directory for Google credentials
source secrets.auto.tfvars && podman run --env GOOGLE_APPLICATION_CREDENTIALS=/mnt/gcloud/application_default_credentials.json -v /gcloud:/mnt/gcloud -p 8080:${APP_PORT} --env FLASK_DEBUG=True --env LOCAL_LOGGING=True --env GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID} --env GOOGLE_CLOUD_PROJECT=${GOOGLE_PROJECT_ID} --env PORT=${APP_PORT} ibc-docker
podman run --env-file secrets.auto.tfvars -v /gcloud/application_default_credentials.json:/home/cnb/creds.json:ro -it -p 8080:${APP_PORT} -u root ibc-pack

podman run --env-file secrets.auto.tfvars -v /gcloud:/mnt/gcloud -p 8080:8080 ibc-docker
```

```Shell
# Building docker image on local repo
podman build -t ibc-docker .
# Starting Docker container on the Host VM using the mounted directory for Google credentials
source secrets.auto.tfvars && podman run --env GOOGLE_APPLICATION_CREDENTIALS=/mnt/gcloud/application_default_credentials.json -v /gcloud:/mnt/gcloud -p 8080:${APP_PORT} --env FLASK_DEBUG=True --env LOCAL_LOGGING=True --env GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID} --env GOOGLE_CLOUD_PROJECT=${GOOGLE_PROJECT_ID} --env PORT=${APP_PORT} ibc-docker
podman run --env-file secrets.auto.tfvars -v /gcloud/application_default_credentials.json:/home/cnb/creds.json:ro -it -p 8080:${APP_PORT} -u root ibc-pack

podman run --env-file secrets.auto.tfvars -v /gcloud:/mnt/gcloud -p 8080:8080 ibc-docker
```

#### Using _buildpacks_

```Shell
# buildpack needs requirements.txt in order to properly create the python libs
poetry export --format requirements.txt --output app/requirements.txt --without-hashes
pack build --builder=gcr.io/buildpacks/builder --path=app --workspace=/app ibc-pack
source secrets.auto.tfvars && podman run --env GOOGLE_APPLICATION_CREDENTIALS=/home/cnb/creds.json --env GOOGLE_CLOUD_PROJECT=${GOOGLE_PROJECT_ID} --env PORT=${APP_PORT} -v /gcloud/application_default_credentials.json:/home/cnb/creds.json:ro -it -p 8080:${APP_PORT} -u root ibc-pack
podman run --env-file secrets.auto.tfvars -v /gcloud/application_default_credentials.json:/home/cnb/creds.json:ro -it -p 8080:${APP_PORT} -u root ibc-pack

```

### Terraform setup

`> terraform init`


### Running local tests
You can then access the app through `http://localhost:8080/`.
