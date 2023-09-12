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
podman machine init --cpus=2 --disk-size=12 --memory=4096 -v $HOME/.config/gcloud:/gcp:ro gcp
podman machine set --rootful gcp
podman machine start gcp
# inspecting content: podman machine ssh gcp
```

Building an app image and run it:

```Shell
pack build --builder=gcr.io/buildpacks/builder --env GOOGLE_FUNCTION_SIGNATURE_TYPE=http --env GOOGLE_FUNCTION_TARGET=main --path=app --workspace=/app app
podman run --env GOOGLE_PROJECT_ID=local-dev --env GOOGLE_APPLICATION_CREDENTIALS=/home/cnb/creds.json -v /gcp/application_default_credentials.json:/home/cnb/creds.json:ro -it -ePORT=8080 -p8080:8080 -u root app
```

### Terraform setup

`> terraform init`


### Running local tests
**From directory** `app/`:

`app> functions-framework-python --target main --debug`

You can then access the app through `http://localhost:8080/`.

