import argparse
import sys

import yaml

from .core import epprint
from .drupal import Drupal


def parse_config(config_file):
    with open(config_file) as stream:
        conf = yaml.safe_load(stream)
    return conf


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-C', '--creds-file', default='credentials.json')
    parser.add_argument(
        '-c', '--config-file', default='config.yml')
    parser.add_argument(
        '-t', '--taxonomy-file', default='taxonomy-drupal.yml')

    return parser.parse_args(argv)


def main(args):
    conf = parse_config(args.config_file)
    epprint(conf)

    with open(args.creds_file) as stream:
        creds = yaml.safe_load(stream)
    drupal = Drupal(
        base_url=conf['drupal_base_url'],
        username=creds['username'],
        password=creds['password']
    )

    taxonomies = drupal.get_taxonomy_terms(conf['taxonomy_vocab'])
    epprint(taxonomies)
    with open(args.taxonomy_file, 'w+') as stream:
        yaml.dump(taxonomies, stream)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
