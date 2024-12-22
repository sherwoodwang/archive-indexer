import os
import sys
import argparse

from . import Archive, Processor

def archive_indexer():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive')
    parser.add_argument('subcommand')

    args = parser.parse_args()

    archive_path = args.archive
    if archive_path is None:
        if os.environ.get('ARINDEXER_ARCHIVE') is not None:
            archive_path = os.environ.get('ARINDEXER_ARCHIVE')
        else:
            archive_path = os.getcwd()

    with Processor() as processor:
        with Archive(processor, archive_path) as archive:
            if args.subcommand == 'rebuild':
                archive.rebuild()
            else:
                print(f'Unknown subcommand: {args.subcommand}', file=sys.stderr)
                sys.exit(1)

if __name__ == '__main__':
    archive_indexer()