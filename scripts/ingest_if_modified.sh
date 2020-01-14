#!/bin/bash

if [ -f ".last_mod" ]; then
    python -m ingestion.mod_dump "$@" > .this_mod
    if [ "$(diff .this_mod .last_mod)" == "" ]; then
        echo "will not ingest, sheet was not modified"
        exit
    fi
    mv .this_mod .last_mod
else
    python -m ingestion.mod_dump "$@" > .last_mod
fi

python -m ingestion.core "$@"
