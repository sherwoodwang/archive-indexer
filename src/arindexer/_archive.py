import asyncio
import os
import stat
from asyncio import TaskGroup, Semaphore
from pathlib import Path
from typing import Iterator

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


class Tracker:
    def __init__(self):
        self.verbosity = 0

    def log_skipping(self, path, equivalent):
        if self.verbosity >= 1:
            print(f"skipping: {path}\n\tidentical file: {equivalent}")

    def log_keeping(self, path: Path):
        if self.verbosity >= 1:
            print(f"keeping: {path}")

class Archive:
    __CONFIG_PREFIX = b'c:'
    __FILE_HASH_PREFIX = b'h:'

    __CONFIG_HASH_ALGORITHM = 'hash-algorithm'

    def __init__(self, processor: Processor, path: str, tracker: Tracker | None = None):
        archive_path = Path(path)

        if tracker is None:
            tracker = Tracker()

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
        self._tracker = tracker
        self._alive = True
        self._database = database
        self._config_database = config_database
        self._file_hash_database = file_hash_database

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

            async def handle_file(path: Path, handle: FileHandle):
                self._register_file(handle, await self._processor.sha256(path))

            for path, handle in self._walk_archive():
                if handle.is_file():
                    await throttler.schedule(handle_file(path, handle))
                else:
                    # TODO
                    pass

        self._write_config(Archive.__CONFIG_HASH_ALGORITHM, 'sha256')

    def filter(self, input: Path, output: Path):
        asyncio.run(self._do_filter(input, output))

    async def _do_filter(self, input: Path, output: Path):
        if output.exists():
            raise FileExistsError(f"File {output} already exists")

        hash_algorithm = self._read_config(Archive.__CONFIG_HASH_ALGORITHM)

        if hash_algorithm is None:
            raise RuntimeError("The index hasn't been build")

        if hash_algorithm != 'sha256':
            raise RuntimeError(f"Unknown hash algorithm: {hash_algorithm}")

        async with TaskGroup() as tg:
            throttler = Throttler(tg, self._processor.concurrency * 2)

            async def handle_file(path: Path, handle: FileHandle):
                digest = await self._processor.sha256(path)
                for candidate in self._lookup_file(digest):
                    candidate = Path(*candidate)
                    if await self._processor.compare(self._archive_path / candidate, path):
                        self._tracker.log_skipping(handle.relative_path(), candidate)
                        break
                else:
                    (output / handle.relative_path()).hardlink_to(path)
                    self._tracker.log_keeping(handle.relative_path())

            for path, handle in self._walk(input):
                if path.is_dir():
                    # TODO
                    #  1. avoid creating archived directories
                    #  2. change mode after all files are linked
                    relative_path = handle.relative_path()
                    if relative_path is None:
                        output.mkdir()
                    else:
                        (output / relative_path).mkdir()
                elif path.is_file():
                    await throttler.schedule(handle_file(path, handle))
                else:
                    # TODO
                    pass

    def inspect(self):
        for key, value in self._database.iterator():
            key: bytes
            if key.startswith(Archive.__CONFIG_PREFIX):
                entry = key[len(Archive.__CONFIG_PREFIX):].decode()
                print('config', entry, value.decode())
            elif key.startswith(Archive.__FILE_HASH_PREFIX):
                hex_digest = key[len(Archive.__FILE_HASH_PREFIX):].hex()
                print('file-hash', hex_digest, msgpack.loads(value))
            else:
                print('OTHER', key, value)

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

    def _register_file(self, handle: FileHandle, digest: bytes) -> None:
        current = self._lookup_file(digest)
        current.append([str(part) for part in handle.relative_path().parts])
        data = msgpack.dumps(current)
        self._file_hash_database.put(digest, data)

    def _lookup_file(self, digest: bytes) -> list[list[str]]:
        data = self._file_hash_database.get(digest)
        if data is None:
            current = []
        else:
            current = msgpack.loads(data)

        return current

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
