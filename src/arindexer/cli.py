import argparse
import os
import sys
from pathlib import Path

from . import Archive, Processor, FileMetadataDifferencePattern, FileDifferenceKind, StandardOutput


def archive_indexer():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('subcommand')
    parser.add_argument('arguments', nargs='*')

    args = parser.parse_args()

    archive_path = args.archive
    if archive_path is None:
        if os.environ.get('ARINDEXER_ARCHIVE') is not None:
            archive_path = os.environ.get('ARINDEXER_ARCHIVE')
        else:
            archive_path = os.getcwd()

    output = StandardOutput()
    if args.verbose:
        output.verbosity = 1

    with Processor() as processor:
        with Archive(processor, archive_path, output=output) as archive:
            if args.subcommand == 'rebuild':
                archive.rebuild()
            elif args.subcommand == 'find-duplicates':
                _find_duplicates(archive, args.arguments)
            elif args.subcommand == 'inspect':
                for record in archive.inspect():
                    print(record)
            else:
                print(f'Unknown subcommand: {args.subcommand}', file=sys.stderr)
                sys.exit(1)


def _find_duplicates(archive: Archive, arguments):
    parser = argparse.ArgumentParser()
    parser.add_argument('--ignore')
    parser.add_argument('file_or_directory', nargs='*')
    args = parser.parse_args(arguments)

    diffptn = FileMetadataDifferencePattern()
    if args.ignore:
        for kind in args.ignore.split(','):
            kind = kind.strip()
            if not kind:
                continue

            diffptn.ignore(FileDifferenceKind(kind))
    else:
        diffptn.ignore_trivial_attributes()

    for file_or_directory in args.file_or_directory:
        archive.find_duplicates(Path(file_or_directory), ignore=diffptn)


if __name__ == '__main__':
    archive_indexer()
