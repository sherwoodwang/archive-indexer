import abc
import asyncio
import os
import stat
import urllib.parse
from asyncio import TaskGroup, Semaphore, Lock, Condition
from enum import StrEnum
from pathlib import Path
from typing import Iterator, Iterable

import msgpack
import plyvel

from ._processor import Processor


class Throttler:
    def __init__(self, task_group: TaskGroup, concurrency: int):
        self._task_group = task_group
        self._semaphore = Semaphore(concurrency)

    async def schedule(self, coro, name=None, context=None):
        await self._semaphore.acquire()

        async def wrapper():
            try:
                return await coro
            finally:
                self._semaphore.release()

        try:
            self._task_group.create_task(wrapper(), name=name, context=context)
        except:
            self._semaphore.release()
            raise


class LockTable:
    class _Lock:
        def __init__(self, parent, entry):
            self._parent: LockTable = parent
            self._entry = entry

        async def __aenter__(self):
            async with self._parent._lock:
                if self._entry in self._parent._entries:
                    self._parent._entries[self._entry].append(self)
                else:
                    self._parent._entries[self._entry] = [self]

                while self._parent._entries[self._entry][0] is not self:
                    await self._parent._entry_releasing.wait()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            async with self._parent._lock:
                backlog = self._parent._entries[self._entry]
                if len(backlog) > 1:
                    self._parent._entries[self._entry] = backlog[1:]
                    self._parent._entry_releasing.notify_all()
                else:
                    del self._parent._entries[self._entry]

    def __init__(self):
        self._lock = Lock()
        self._entry_releasing = Condition(self._lock)
        self._entries = dict()

    def lock(self, entry):
        return LockTable._Lock(self, entry)


class FileDifferenceKind(StrEnum):
    ATIME = 'atime'
    CTIME = 'ctime'
    MTIME = 'mtime'
    BIRTHTIME = 'birthtime'


class FileMetadataDifferencePattern:
    def __init__(self):
        self._ignored_patterns: set[FileDifferenceKind] = set()

    def ignore_trivial_attributes(self):
        self.ignore(FileDifferenceKind.ATIME)
        self.ignore(FileDifferenceKind.CTIME)

    def ignore_all(self):
        for kind in FileDifferenceKind:
            kind: FileDifferenceKind
            self.ignore(kind)

    def ignore(self, kind: FileDifferenceKind):
        self._ignored_patterns.add(kind)

    def is_ignored(self, diff_desc: tuple[str, any, any]) -> bool:
        return FileDifferenceKind(diff_desc[0]) in self._ignored_patterns

    def filter(self, diffs: Iterable[tuple[str, any, any]]) -> list[tuple[str, any, any]]:
        return [diff for diff in diffs if not self.is_ignored(diff)]


FileMetadataDifferencePattern.ALL = FileMetadataDifferencePattern()
FileMetadataDifferencePattern.ALL.ignore_all()


class FileHandle:
    def __init__(self, parent, name: str | None, st: os.stat_result):
        self.parent: FileHandle = parent
        self.name: str = name
        self.stat: os.stat_result = st
        self.exclusion: set[str] = set()

        self._scanned = False
        self._child_count: int = 0

    def exclude(self, filename):
        self.exclusion.add(filename)

    def is_excluded(self, filename):
        return filename in self.exclusion

    def relative_path(self) -> Path:
        path = None
        handle = self
        while handle:
            if handle.name is not None:
                if path is None:
                    path = Path(handle.name)
                else:
                    path = Path(handle.name) / path
            handle = handle.parent
        return path

    def register_child(self):
        if self._scanned:
            raise ValueError("cannot register a new child after scanning is done")

        self._child_count += 1

    def set_scanned(self):
        self._scanned = True

    def is_file(self):
        return stat.S_ISREG(self.stat.st_mode)


class Output(metaclass=abc.ABCMeta):
    def __init__(self):
        self.verbosity = 0
        self.showing_possible_duplicates = False

    @abc.abstractmethod
    def _produce(self, record: list[str]):
        raise NotImplementedError()

    def produce_duplicate(self, path, equivalent, diffs):
        record = [str(path)]

        if self.verbosity >= 1:
            record.append(f"## identical file: {equivalent}")
            for diff in diffs:
                record.append(f"## ignored difference - {diff[0]}: {diff[1]} != {diff[2]}")

        self._produce(record)

    def produce_possible_duplicate(self, path, candidate, major_diffs, diffs):
        if self.showing_possible_duplicates:
            record = [f'# possible duplicate: {str(path)}']

            if self.verbosity >= 1:
                record.append(f"## file with identical content: {candidate}")

                for diff in major_diffs:
                    record.append(f"## difference - {diff[0]}: {diff[1]} != {diff[2]}")

                for diff in diffs:
                    if diff in major_diffs:
                        continue

                    record.append(f"## ignored difference - {diff[0]}: {diff[1]} != {diff[2]}")

            self._produce(record)


class StandardOutput(Output):
    def __init__(self):
        super().__init__()

    def _produce(self, record):
        for part in record:
            print(part)


class Archive:
    __CONFIG_PREFIX = b'c:'
    __FILE_HASH_PREFIX = b'h:'

    __CONFIG_HASH_ALGORITHM = 'hash-algorithm'

    def __init__(self, processor: Processor, path: str, output: Output | None = None):
        archive_path = Path(path)

        if output is None:
            output = StandardOutput()

        if not archive_path.exists():
            raise FileNotFoundError(f"Archive {archive_path} does not exist")

        if not archive_path.is_dir():
            raise NotADirectoryError(f"Archive {archive_path} is not a directory")

        index_path = archive_path / '.aridx'
        index_path.mkdir(exist_ok=True)

        database_path = index_path / 'database'

        database = None
        try:
            database = plyvel.DB(str(database_path), create_if_missing=True)
            config_database: plyvel.DB = database.prefixed_db(Archive.__CONFIG_PREFIX)
            file_hash_database: plyvel.DB = database.prefixed_db(Archive.__FILE_HASH_PREFIX)
        except:
            if database is not None:
                database.close()
            raise

        self._processor = processor
        self._archive_path = archive_path
        self._output = output
        self._alive = True
        self._database = database
        self._config_database = config_database
        self._file_hash_database = file_hash_database

        self._hash_algorithms = {
            'sha256': (32, self._processor.sha256)
        }
        self._default_hash_algorithm = 'sha256'

    def __del__(self):
        self.close()

    def __enter__(self):
        if not self._alive:
            raise BrokenPipeError(f"Archive was closed")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if not self._alive:
            return

        self._alive = False
        self._file_hash_database = None
        self._database.close()
        self._database = None

    def rebuild(self):
        asyncio.run(self._do_rebuild())

    async def _do_rebuild(self):
        self._truncate()

        async with TaskGroup() as tg:
            throttler = Throttler(tg, self._processor.concurrency * 2)

            lock_table = LockTable()

            _, calculate_digest = self._hash_algorithms[self._default_hash_algorithm]

            async def handle_file(path: Path, handle: FileHandle):
                digest = await calculate_digest(path)

                async with lock_table.lock(digest):
                    next_ec_id = 0
                    for ec_id, paths in self._lookup_file_equivalents(digest):
                        if next_ec_id <= ec_id:
                            next_ec_id = ec_id + 1

                        if await self._processor.compare_content(path, self._archive_path / paths[0]):
                            paths.append(handle.relative_path())
                            self._register_file_equivalents(digest, ec_id, paths)
                            break
                    else:
                        self._register_file_equivalents(digest, next_ec_id, [handle.relative_path()])

            for path, handle in self._walk_archive():
                if handle.is_file():
                    await throttler.schedule(handle_file(path, handle))
                else:
                    # TODO
                    pass

        self._write_config(Archive.__CONFIG_HASH_ALGORITHM, self._default_hash_algorithm)

    def find_duplicates(self, input: Path, ignore: FileMetadataDifferencePattern | None = None):
        asyncio.run(self._do_find_duplicates(input, ignore=ignore))

    async def _do_find_duplicates(self, input: Path, ignore: FileMetadataDifferencePattern | None):
        if ignore is None:
            ignore = FileMetadataDifferencePattern()

        hash_algorithm = self._read_config(Archive.__CONFIG_HASH_ALGORITHM)

        if hash_algorithm is None:
            raise RuntimeError("The index hasn't been build")

        if hash_algorithm not in self._hash_algorithms:
            raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

        async with TaskGroup() as tg:
            throttler = Throttler(tg, self._processor.concurrency * 2)

            _, calculate_digest = self._hash_algorithms[self._default_hash_algorithm]

            async def handle_file(path: Path, handle: FileHandle):
                digest = await calculate_digest(path)

                for ec_id, paths in self._lookup_file_equivalents(digest):
                    if await self._processor.compare_content(self._archive_path / paths[0], path):
                        for candidate in paths:
                            diffs = await self._processor.compare_metadata(self._archive_path / candidate, path)
                            major_diffs = [diff for diff in diffs if not ignore.is_ignored(diff)]
                            if not major_diffs:
                                equivalent = candidate
                                break
                            else:
                                self._output.produce_possible_duplicate(path, candidate, major_diffs, diffs)
                        else:
                            continue
                        break
                else:
                    return

                self._output.produce_duplicate(path, equivalent, diffs)

            for path, handle in self._walk(input):
                if path.is_dir():
                    # TODO
                    #  1. avoid creating archived directories
                    #  2. change mode after all files are linked
                    pass
                elif path.is_file():
                    await throttler.schedule(handle_file(path, handle))
                else:
                    # TODO
                    pass

    def inspect(self) -> Iterator[str]:
        hash_algorithm = self._read_config(Archive.__CONFIG_HASH_ALGORITHM)
        if hash_algorithm in self._hash_algorithms:
            hash_length, _ = self._hash_algorithms[hash_algorithm]
        else:
            hash_length = None

        for key, value in self._database.iterator():
            key: bytes
            if key.startswith(Archive.__CONFIG_PREFIX):
                entry = key[len(Archive.__CONFIG_PREFIX):].decode()
                yield f'config {entry} {value.decode()}'
            elif key.startswith(Archive.__FILE_HASH_PREFIX):
                digest_and_ec_id = key[len(Archive.__FILE_HASH_PREFIX):]
                paths = ' '.join((
                    '/'.join((urllib.parse.quote_plus(part) for part in path))
                    for path in msgpack.loads(value)))
                if hash_length is not None:
                    hex_digest = digest_and_ec_id[:hash_length].hex()
                    ec_id = int.from_bytes(digest_and_ec_id[hash_length:])
                    yield f'file-hash {hex_digest} {ec_id} {paths}'
                else:
                    hex_digest_and_ec_id = digest_and_ec_id.hex()
                    yield f'file-hash *{hex_digest_and_ec_id} {paths}'
            else:
                yield f'OTHER {key} {value}'

    def _truncate(self):
        self._write_config(Archive.__CONFIG_HASH_ALGORITHM, None)

        with self._file_hash_database.iterator() as it:
            batch = self._file_hash_database.write_batch()
            for key, _ in it:
                batch.delete(key)
            batch.write()

    def _write_config(self, entry: str, value: str | None) -> None:
        if value is None:
            self._config_database.delete(entry.encode())
        else:
            self._config_database.put(entry.encode(), value.encode())

    def _read_config(self, entry: str) -> str | None:
        value = self._config_database.get(entry.encode())

        if value is not None:
            value = value.decode()

        return value

    def _register_file_equivalents(self, digest: bytes, ec_id: int, paths: list[Path]) -> None:
        data = [[str(part) for part in path.parts] for path in paths]
        data.sort()
        data = msgpack.dumps(data)
        self._file_hash_database.put(digest + ec_id.to_bytes(length=4).lstrip(b'\0'), data)

    def _lookup_file_equivalents(self, digest: bytes) -> Iterable[tuple[int, list[Path]]]:
        ec_db: plyvel.DB = self._file_hash_database.prefixed_db(digest)
        for key, data in ec_db.iterator():
            ec_id = int.from_bytes(key)
            data: list[list[str]] = msgpack.loads(data)
            yield ec_id, [Path(*parts) for parts in data]

    def _walk_archive(self) -> Iterator[tuple[Path, FileHandle]]:
        handle = FileHandle(None, None, self._archive_path.stat())
        handle.exclude('.aridx')
        yield from self.__walk_recursively(self._archive_path, handle)

    def _walk(self, path: Path) -> Iterator[tuple[Path, FileHandle]]:
        handle = FileHandle(None, None, path.stat())
        yield path, handle
        yield from self.__walk_recursively(path, handle)

    def __walk_recursively(self, path: Path, parent: FileHandle) -> Iterator[tuple[Path, FileHandle]]:
        child: Path
        for child in path.iterdir():
            if parent.is_excluded(child.name):
                continue

            st = child.stat(follow_symlinks=False)
            handle = FileHandle(parent, child.name, st)
            parent.register_child()
            if stat.S_ISDIR(st.st_mode):
                yield child, handle
                yield from self.__walk_recursively(child, handle)
                handle.set_scanned()
            else:
                yield child, handle
