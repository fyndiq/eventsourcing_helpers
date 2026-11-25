import signal
import threading
import time
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from types import FrameType
from typing import Callable, Generic, Hashable, Iterable, Protocol, TypeVar

MessageT = TypeVar("MessageT")


class ShutdownObserver(Protocol):
    def is_shutting_down(self) -> bool:
        """Return whether the process is shutting down."""


class BatchProcessingIncomplete(RuntimeError):
    """Raised when shutdown prevents a whole batch from finishing."""


@dataclass(frozen=True)
class ConcurrentBatchConfig:
    max_workers: int = 1
    drain_timeout: float = 30.0

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if self.drain_timeout < 0:
            raise ValueError("drain_timeout must be greater than or equal to 0")


class NoopShutdownObserver:
    def is_shutting_down(self) -> bool:
        return False


class SignalShutdownObserver:
    """
    Small shutdown seam for current confluent-kafka-helpers releases.

    The observer records SIGTERM locally so batch scheduling can stop.
    A future confluent-kafka-helpers shutdown API can replace this class without
    changing the concurrent batch processor.
    """

    def __init__(
        self,
        signals: Iterable[signal.Signals] = (signal.SIGTERM,),
    ) -> None:
        self._shutdown = threading.Event()
        self._previous_handlers = {}
        for shutdown_signal in signals:
            self._previous_handlers[shutdown_signal] = signal.getsignal(shutdown_signal)
            signal.signal(shutdown_signal, self._handle_signal)

    def _handle_signal(self, signum: int, frame: FrameType | None) -> None:
        self._shutdown.set()
        previous_handler = self._previous_handlers.get(signal.Signals(signum))
        if callable(previous_handler):
            previous_handler(signum, frame)

    def is_shutting_down(self) -> bool:
        return self._shutdown.is_set()

    def request_shutdown(self) -> None:
        self._shutdown.set()


def kafka_message_key(message) -> Hashable:
    return message._meta.key


class ConcurrentBatchHandler(Generic[MessageT]):
    def __init__(
        self,
        handler: Callable[[MessageT], None],
        *,
        key: Callable[[MessageT], Hashable] = kafka_message_key,
        config: ConcurrentBatchConfig | None = None,
        shutdown_observer: ShutdownObserver | None = None,
        executor_factory: Callable[..., ThreadPoolExecutor] = ThreadPoolExecutor,
    ) -> None:
        if not callable(handler):
            raise TypeError("handler must be callable")
        if not callable(key):
            raise TypeError("key must be callable")
        self._handler = handler
        self._key = key
        self._config = config or ConcurrentBatchConfig()
        self._shutdown_observer = shutdown_observer or NoopShutdownObserver()
        self._executor_factory = executor_factory
        self._executor: ThreadPoolExecutor | None = None
        self._closed = False

    def __call__(self, messages: list[MessageT]) -> None:
        if not messages:
            return
        if self._shutdown_observer.is_shutting_down():
            raise BatchProcessingIncomplete("shutdown requested before scheduling batch")
        executor = self._get_executor()
        stop_batch = threading.Event()
        groups = self._group_by_key(messages)
        processed_count = self._wait_for_batch(executor, groups, stop_batch)

        if processed_count != len(messages):
            raise BatchProcessingIncomplete("shutdown left batch unfinished")

    def close(self) -> None:
        self._closed = True
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._closed:
            raise RuntimeError("ConcurrentBatchHandler is closed")
        if self._executor is None:
            self._executor = self._executor_factory(max_workers=self._config.max_workers)
        return self._executor

    def _group_by_key(self, messages: list[MessageT]) -> list[list[MessageT]]:
        grouped: OrderedDict[Hashable, list[MessageT]] = OrderedDict()
        for message in messages:
            grouped.setdefault(self._key(message), []).append(message)
        return list(grouped.values())

    def _handle_group(self, messages: list[MessageT], stop_batch: threading.Event) -> int:
        processed_count = 0
        for message in messages:
            if stop_batch.is_set() or self._shutdown_observer.is_shutting_down():
                break
            self._handler(message)
            processed_count += 1
        return processed_count

    def _wait_for_batch(
        self,
        executor: ThreadPoolExecutor,
        groups: list[list[MessageT]],
        stop_batch: threading.Event,
    ) -> int:
        futures: dict[Future[int], int] = {}
        next_group_index = self._schedule_groups(executor, groups, 0, futures, stop_batch)
        pending = set(futures)
        processed_count = 0
        drain_deadline = None

        while pending:
            timeout = self._wait_timeout(drain_deadline)
            done, pending = wait(pending, timeout=timeout, return_when=FIRST_COMPLETED)

            for future in done:
                try:
                    processed_count += self._future_processed_count(future)
                except BaseException:
                    stop_batch.set()
                    self._cancel_pending(pending)
                    raise

            if self._shutdown_observer.is_shutting_down():
                stop_batch.set()
                self._cancel_pending(pending)
                if drain_deadline is None:
                    drain_deadline = time.monotonic() + self._config.drain_timeout
            elif not stop_batch.is_set():
                next_group_index = self._schedule_groups(
                    executor, groups, next_group_index, futures, stop_batch
                )
                pending = {future for future in pending | set(futures) if not future.done()}

            if drain_deadline is not None and time.monotonic() >= drain_deadline:
                stop_batch.set()
                self._cancel_pending(pending)
                break

        for future in pending:
            if future.done() and not future.cancelled():
                processed_count += self._future_processed_count(future)

        return processed_count

    def _schedule_groups(
        self,
        executor: ThreadPoolExecutor,
        groups: list[list[MessageT]],
        next_group_index: int,
        futures: dict[Future[int], int],
        stop_batch: threading.Event,
    ) -> int:
        active_count = sum(not future.done() for future in futures)
        while all(
            (
                active_count < self._config.max_workers,
                next_group_index < len(groups),
                not stop_batch.is_set(),
                not self._shutdown_observer.is_shutting_down(),
            )
        ):
            group = groups[next_group_index]
            future = executor.submit(self._handle_group, group, stop_batch)
            futures[future] = len(group)
            next_group_index += 1
            active_count += 1
        return next_group_index

    def _wait_timeout(self, drain_deadline: float | None) -> float:
        if drain_deadline is None:
            return 0.05
        return max(0, min(0.05, drain_deadline - time.monotonic()))

    def _future_processed_count(self, future: Future[int]) -> int:
        if future.cancelled():
            return 0
        return future.result()

    def _cancel_pending(self, pending: set[Future[int]]) -> None:
        for future in pending:
            future.cancel()
