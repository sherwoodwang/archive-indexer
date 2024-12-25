import asyncio
import filecmp
import hashlib
import multiprocessing
import pathlib
from typing import Awaitable


def compute_sha256_for_path(path: pathlib.Path):
    with open(path, "rb") as f:
        # noinspection PyTypeChecker
        return hashlib.file_digest(f, hashlib.sha256).digest()


def compare_files_with_paths(a: pathlib.Path, b: pathlib.Path):
    return filecmp.cmp(a, b, shallow=False)


class Processor:
    def __init__(self, concurrency: int | None = None):
        if concurrency is None:
            concurrency = multiprocessing.cpu_count()

        self._concurrency = concurrency
        self._pool: multiprocessing.Pool = multiprocessing.Pool(self._concurrency)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._pool.close()

    @property
    def concurrency(self):
        return self._concurrency

    def sha256(self, path: pathlib.Path) -> Awaitable[bytes]:
        return self._evaluate(compute_sha256_for_path, path)

    def compare(self, a: pathlib.Path, b: pathlib.Path) -> Awaitable[bytes]:
        return self._evaluate(compare_files_with_paths, a, b)

    def _evaluate(self, func, *args):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        self._pool.apply_async(func, args=args,
                               callback=lambda v: loop.call_soon_threadsafe(future.set_result, v),
                               error_callback=lambda e: loop.call_soon_threadsafe(future.set_exception, e))

        return future
