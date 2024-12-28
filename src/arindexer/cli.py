import argparse
import os
from pathlib import Path

from . import Archive, Processor, FileMetadataDifferencePattern, FileDifferenceKind, StandardOutput


def archive_indexer():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive')
    parser.add_argument('--verbose', action='store_true')
    subparsers = parser.add_subparsers()

    parser_find_duplicates = subparsers.add_parser('rebuild')
    parser_find_duplicates.set_defaults(method=lambda a, _1, _2: a.rebuild())

    parser_find_duplicates = subparsers.add_parser('find-duplicates')
    parser_find_duplicates.add_argument('--ignore')
    parser_find_duplicates.add_argument('--show-possible-duplicates', action='store_true')
    parser_find_duplicates.add_argument('file_or_directory', nargs='*')
    parser_find_duplicates.set_defaults(method=_find_duplicates)

    parser_find_duplicates = subparsers.add_parser('inspect')
    parser_find_duplicates.set_defaults(method=lambda a, _1, _2: a.inspect())

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
            args.method(archive, output, args)


def _find_duplicates(archive: Archive, output: StandardOutput, args):
    diffptn = FileMetadataDifferencePattern()
    if args.ignore:
        for kind in args.ignore.split(','):
            kind = kind.strip()
            if not kind:
                continue

            diffptn.ignore(FileDifferenceKind(kind))
    else:
        diffptn.ignore_trivial_attributes()

    saved_showing_possible_duplicates = output.showing_possible_duplicates
    try:
        if args.show_possible_duplicates:
            output.showing_possible_duplicates = True

        for file_or_directory in args.file_or_directory:
            archive.find_duplicates(Path(file_or_directory), ignore=diffptn)
    finally:
        output.showing_possible_duplicates = saved_showing_possible_duplicates


if __name__ == '__main__':
    archive_indexer()
