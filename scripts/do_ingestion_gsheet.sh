#!/bin/bash

eval "$(/home/github/.pyenv/libexec/pyenv init -)"
cd /home/github/crisisapp-ingestor/ || exit 1
python -m ingestion.core \
    --name 'ingestion' \
    --config-file 'config.yml' \
    --creds-file '/etc/creds/crisisapp-264723-d73f4f94e829.json' \
    --output-file '/var/www/crisis.app/json/organisations.json'
