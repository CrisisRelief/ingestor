#!/bin/bash

eval "$(/home/github/.pyenv/libexec/pyenv init -)"
cd /home/github/crisisapp-ingestor/ || exit 1
pip install -r requirements.txt
scripts/do_ingestion_gsheet.sh
scripts/do_ingestion_drupal_dev.sh
