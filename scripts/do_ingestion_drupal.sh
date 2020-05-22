#!/bin/bash

eval "$(/home/github/.pyenv/libexec/pyenv init -)"
cd /home/github/crisisapp-ingestor/ || exit 1
python -m ingestion.update_taxonomies \
    --creds-file '/etc/creds/creds-drupal-dev.yml' \
    --config-file 'config-drupal-dev.yml' \
    --taxonomy-file 'taxonomy-drupal-dev.yml'
python -m ingestion.core \
    --name 'ingestion-drupal-dev' \
    --creds-file '/etc/creds/creds-drupal-dev.yml' \
    --schema-file 'schema-drupal-vue.yml' \
    --config-file 'config-drupal-dev.yml' \
    --taxonomy-file 'taxonomy-drupal-dev.yml' \
    --input-source 'drupal' \
    --output-file '/var/www/crisis.app/json/organisations.json' \
    --append-output
