import filecmp
import hashlib
import inspect
import multiprocessing
import pathlib
import threading
from typing import Iterable, Coroutine


def compute_sha256_for_path(path: pathlib.Path):
    with open(path, "rb") as f:
        # noinspection PyTypeChecker
        return hashlib.file_digest(f, hashlib.sha256).digest()


def compare_files_with_paths(a: pathlib.Path, b: pathlib.Path):
    return filecmp.cmp(a, b, shallow=False)


class Promise:
    def __init__(self):
        self.completion: tuple[bool, any, any] | None = None


class Pipeline:
    def __init__(self, promise: Promise):
        self.__promise = promise
        self._callbacks = []

    def on_result(self, callback):
        self._callbacks.append(lambda s, v, e: callback(v) if s else None)

    def on_error(self, callback):
        self._callbacks.append(lambda s, v, e: callback(e) if not s else None)

    def __await__(self):
        while (completion := self.__promise.completion) is None:
            yield self

        succeeded, value, error = completion
        if succeeded:
            return value
        else:
            raise error

    def execute(self):
        succeeded, value, error = self.__promise.completion

        while self._callbacks:
            callback = self._callbacks.pop()

            result = callback(succeeded, value, error)

            if inspect.iscoroutine(result):
                def loop(coroutine: Coroutine):
                    def resume(value):
                        try:
                            derived = coroutine.send(value)
                        except StopIteration:
                            return

                        if not isinstance(derived, Pipeline):
                            raise TypeError('The generated value is not a Pipeline')

                        derived.on_result(resume)

                    resume(None)

                loop(result)


class Context:
    def __init__(self, pool: multiprocessing.Pool, concurrency: int):
        self._pool: multiprocessing.Pool = pool
        self._concurrency: int = concurrency

        self._lock = threading.Lock()
        self._ongoing = 0

        self._fetcher = threading.Condition(self._lock)
        self._scheduler = threading.Condition(self._lock)
        self._resolved = []

    def sha256(self, path: pathlib.Path) -> Pipeline:
        return self._evaluate(compute_sha256_for_path, path)

    def compare(self, a: pathlib.Path, b: pathlib.Path) -> Pipeline:
        return self._evaluate(compare_files_with_paths, a, b)

    def fetch_ready_pipelines(self, blocking=False) -> Iterable[Pipeline]:
        while True:
            with self._lock:
                while blocking and self._ongoing > 0 and not self._resolved:
                    self._fetcher.wait()

                retval = self._resolved
                self._resolved = []

            if not retval:
                return

            yield from retval

    def _evaluate(self, func, *args):
        promise = Promise()
        pipeline = Pipeline(promise)

        def resolve(digest: bytes):
            promise.completion = (True, digest, None)

            with self._lock:
                self._ongoing -= 1
                self._resolved.append(pipeline)
                self._fetcher.notify()
                self._scheduler.notify()

        def report(error):
            promise.completion = (False, None, error)

            with self._lock:
                self._ongoing -= 1
                self._resolved.append(pipeline)
                self._fetcher.notify()
                self._scheduler.notify()

        with self._lock:
            if self._ongoing >= self._concurrency:
                self._scheduler.wait()

            self._ongoing += 1
            self._pool.apply_async(func, args=args, callback=resolve, error_callback=report)

        return pipeline


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

    def make_context(self) -> Context:
        return Context(self._pool, self._concurrency)
