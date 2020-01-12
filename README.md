# ingestor
Ingests the Organization data from Google Sheets into json for the frontend

## Local Installation

### (Optional) Virtualenv

It's good practice to create a virtualenv so this doesn't mess with your other python environments

This step requires installing [pyenv](https://github.com/pyenv/pyenv) and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

```
make virtualenv_init
```

Hot tip: Make sure ur editor picks up the correct virtualenv

### Generate Credentials

Go [here](https://medium.com/@vince.shields913/reading-google-sheets-into-a-pandas-dataframe-with-gspread-and-oauth2-375b932be7bf), save the json to the root of this repo

### Development Requirements

```
make install_reqs_dev
```

## Testing

```
make test
```

Or to continuously re-run the tests on change:

```
make test_watch
```

# Usage

Just run a build in https://github.com/CrisisRelief/ingestor/actions
