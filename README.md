# ingestor

Ingests the Organization data from Google Sheets into json for the frontend

## Local Installation

### (Optional) Virtualenv

It's good practice to create a virtualenv so this doesn't mess with your other python environments

This step requires installing [pyenv](https://github.com/pyenv/pyenv) and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

```bash
make virtualenv_init
```

Hot tip: Make sure ur editor picks up the correct virtualenv

### Generate Credentials

Go [here](https://medium.com/@vince.shields913/reading-google-sheets-into-a-pandas-dataframe-with-gspread-and-oauth2-375b932be7bf), save the json to the root of this repo

## Development

### Development Requirements

```bash
make install_reqs_dev
```

### Testing

```bash
make test
```

Or to continuously re-run the tests on change:

```bash
make test_watch
```

### Linting

Please run the linter before pushing code.

```bash
make lint
```

## Usage

Just run a build in https://github.com/CrisisRelief/ingestor/actions

```txt
usage: core.py [-h] [-C CREDS_FILE] [-c CONFIG_FILE] [-s SCHEMA_FILE]
               [-t TAXONOMY_FILE] [-S INPUT_SOURCE] [-a] [-o OUTPUT_FILE]
               [-f OUTPUT_FORMAT] [-n NAME] [-L LIMIT]

optional arguments:
  -h, --help            show this help message and exit
  -C CREDS_FILE, --creds-file CREDS_FILE
  -c CONFIG_FILE, --config-file CONFIG_FILE
  -s SCHEMA_FILE, --schema-file SCHEMA_FILE
  -t TAXONOMY_FILE, --taxonomy-file TAXONOMY_FILE
  -S INPUT_SOURCE, --input-source INPUT_SOURCE
  -a, --append-output   append to existing output file, updating records based
                        on primary_key_field
  -o OUTPUT_FILE, --output-file OUTPUT_FILE
                        where to output the result (default: stdout)
  -f OUTPUT_FORMAT, --output-format OUTPUT_FORMAT
  -n NAME, --name NAME
  -L LIMIT, --limit LIMIT
```

### Config File

see `tests/data/dumy-config.yaml` for an example of a config file.

The schema mappings support Jinja2 template strings with the following available variables:

- **record** a dictionary of the row in the sheet.
- **category_ids** a list of category ids matched from the taxonomies defined in

### Schema File

see `tests/data/dumy-schema.yaml` for an example of a schema file.
