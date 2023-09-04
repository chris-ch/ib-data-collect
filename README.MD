# Interactive Brokers Products Database
## Initialisation
### Google Cloud project
You need to setup a Google Cloud project and a Firebase Authentication first.

### Development setup
First install all required packages using:

`> poetry update`

This will generate the `poetry.lock` file.
You also need to generate the requirements.txt file in app:

`> poetry export --format requirements.txt --output app/requirements.txt --without-hashes`

You will need to execute the above command any time you modify the poetry packages.

### Terraform setup

`> terraform init`


### Running local tests
**From directory** `app/`:

`app> functions-framework-python --target main --debug`

You can then access the app through `http://localhost:8080/`.

