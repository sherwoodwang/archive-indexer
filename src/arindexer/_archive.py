import asyncio
import os
import stat
from asyncio import TaskGroup, Semaphore
from pathlib import Path
from typing import Iterator

import msgpack
import plyvel

from ._processor import Processor


class ThrottlingTaskGroup(TaskGroup):
    def __init__(self, concurrency: int):
        super().__init__()
        self._semaphore = Semaphore(concurrency)

    async def schedule(self, coro, name=None, context=None):
        await self._semaphore.acquire()

        async def wrapper():
            try:
                return await coro
            finally:
                self._semaphore.release()

        try:
            super().create_task(wrapper(), name=name, context=context)
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


class Archive:
    def __init__(self, processor: Processor, path: str):
        archive_path = Path(path)

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
            file_hash_database: plyvel.DB = database.prefixed_db(b'file-hash:')
        except:
            if database is not None:
                database.close()
            raise

        self._processor = processor
        self._alive = True
        self._database = database
        self._file_hash_database = file_hash_database
        self._archive_path = archive_path

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

        async with ThrottlingTaskGroup(self._processor.concurrency) as tg:
            async def handle_file(path: Path, handle: FileHandle):
                self._register_file(handle, await self._processor.sha256(path))

            for path, handle in self._walk_archive():
                if handle.is_file():
                    await tg.schedule(handle_file(path, handle))
                else:
                    # TODO
                    pass

    def filter(self, input: Path, output: Path):
        asyncio.run(self._do_filter(input, output))

    async def _do_filter(self, input: Path, output: Path):
        if output.exists():
            raise FileExistsError(f"File {output} already exists")

        async with ThrottlingTaskGroup(self._processor.concurrency) as tg:
            async def handle_file(path: Path, handle: FileHandle):
                digest = await self._processor.sha256(path)
                for candidate in self._lookup_file(digest):
                    candidate = Path(*candidate)
                    if await self._processor.compare(self._archive_path / candidate, path):
                        break
                else:
                    (output / handle.relative_path()).hardlink_to(path)

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
                    await tg.schedule(handle_file(path, handle))
                else:
                    # TODO
                    pass

    def inspect(self):
        for key, value in self._database.iterator():
            key: bytes
            if key.startswith(b'file-hash:'):
                hex_digest = key[len('file-hash:'):].hex()
                print('file-hash', hex_digest, msgpack.loads(value))
            else:
                print('OTHER', key, value)

    def _truncate(self):
        with self._file_hash_database.iterator() as it:
            batch = self._file_hash_database.write_batch()
            for key, _ in it:
                batch.delete(key)
            batch.write()

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
