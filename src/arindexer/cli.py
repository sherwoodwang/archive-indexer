import argparse
import os
from pathlib import Path

from . import Archive, Processor, FileMetadataDifferencePattern, FileMetadataDifferenceType, StandardOutput, \
    ArchiveIndexNotFound


def archive_indexer():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive')
    parser.add_argument('--verbose', action='store_true')
    subparsers = parser.add_subparsers()

    parser_rebuild = subparsers.add_parser('rebuild')
    parser_rebuild.set_defaults(method=lambda a, _1, _2: a.rebuild(), create=True)

    parser_refresh = subparsers.add_parser('refresh')
    parser_refresh.set_defaults(method=lambda a, _1, _2: a.refresh(), create=True)

    parser_find_duplicates = subparsers.add_parser('find-duplicates')
    parser_find_duplicates.add_argument('--ignore')
    parser_find_duplicates.add_argument('--show-possible-duplicates', action='store_true')
    parser_find_duplicates.add_argument('file_or_directory', nargs='*')
    parser_find_duplicates.set_defaults(method=_find_duplicates, create=False)

    parser_inspect = subparsers.add_parser('inspect')
    parser_inspect.set_defaults(method=_inspect, create=False)

    args = parser.parse_args()

    archive_path = args.archive
    if archive_path is None:
        archive_path = os.environ.get('ARINDEXER_ARCHIVE')

    output = StandardOutput()
    if args.verbose:
        output.verbosity = 1

    def load_archive(processor):
        if archive_path is None:
            working_directory = os.getcwd()
            first_exception = None
            attempt = Path(working_directory)
            while True:
                try:
                    return Archive(processor, attempt, output=output)
                except ArchiveIndexNotFound as e:
                    if attempt == attempt.parent:
                        if args.create:
                            return Archive(processor, working_directory, create=True, output=output)
                        else:
                            if first_exception is not None:
                                raise first_exception
                            else:
                                raise

                    attempt = attempt.parent
                    if first_exception is None:
                        first_exception = e
        else:
            return Archive(processor, archive_path, output=output)

    with Processor() as processor:
        with load_archive(processor) as archive:
            args.method(archive, output, args)


def _find_duplicates(archive: Archive, output: StandardOutput, args):
    diffptn = FileMetadataDifferencePattern()
    if args.ignore:
        for kind in args.ignore.split(','):
            kind = kind.strip()
            if not kind:
                continue

            diffptn.add(FileMetadataDifferenceType(kind))
    else:
        diffptn.add_trivial_attributes()

    saved_showing_content_wise_duplicates = output.showing_content_wise_duplicates
    try:
        if args.show_possible_duplicates:
            output.showing_content_wise_duplicates = True

        for file_or_directory in args.file_or_directory:
            archive.find_duplicates(Path(file_or_directory), ignore=diffptn)
    finally:
        output.showing_content_wise_duplicates = saved_showing_content_wise_duplicates


def _inspect(archive: Archive, output: StandardOutput, args):
    for record in archive.inspect():
        print(record)


if __name__ == '__main__':
    archive_indexer()
