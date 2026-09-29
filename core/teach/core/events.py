from functools import lru_cache
from weakref import ref
from dataclasses import dataclass, field
from typing import Any, Callable, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from logging import Logger


HandlerId = str | type
MessageHandler = Callable[[Any], None]
FallbackHandler = Callable[[HandlerId, Any], None]
LoggerLike = Union[str, "Logger", None]

_HandlersRegistry = dict[HandlerId, dict[type | None, list[MessageHandler]]]

NOT_GIVEN = object()
LOG_LEVELS = ("debug", "info", "warning", "error", "critical", "log")
COMMON_IDS = {*LOG_LEVELS, "show"}


@dataclass
class Config:
    log: set = field(default_factory=lambda: COMMON_IDS)
    log_all: bool = False


@dataclass
class Events:
    """
    Handle messages using signals and message handlers.
    """

    _handlers: _HandlersRegistry = field(default_factory=dict, init=False, repr=False)
    config: Config = field(default_factory=Config)
    fallback: FallbackHandler | None = field(default=None, repr=False)

    def __call__(self, id: HandlerId, msg: Any) -> None:
        return self.trigger(id, msg)

    @overload
    def register(
        self, id: HandlerId, /, handler: MessageHandler, *, cls: type | None = None
    ) -> "Handler":
        ...

    @overload
    def register(
        self, id: HandlerId, /, *, cls: type | None = None
    ) -> Callable[[MessageHandler], MessageHandler]:
        ...

    def register(self, id, handler=None, *, cls=None):
        """
        Register a message handler.
        """
        if handler is None:

            def decorator(fn: MessageHandler, /):
                handler = self.register(id, fn, cls=cls)
                try:
                    fn.unregister = handler.unregister  # type: ignore
                    fn.trigger = handler.trigger  # type: ignore
                except AttributeError:
                    pass
                return fn

            return decorator

        self._handlers.setdefault(id, {}).setdefault(cls, []).append(handler)
        return Handler(ref(self), handler, id, cls)

    @overload
    def unregister(
        self, handler: MessageHandler, *, id: HandlerId, cls: type | None = None
    ) -> None:
        ...

    @overload
    def unregister(self, handler: MessageHandler, *, cls: type | None = None) -> None:
        ...

    @overload
    def unregister(self, handler: "Handler") -> None:
        ...

    def unregister(self, handler, **kwargs):
        """
        Unregister a message handler.
        """

        if isinstance(handler, Handler):
            if kwargs:
                raise ValueError("Cannot unregister a handler with extra arguments")

            self.unregister(handler.func, id=handler.id, cls=handler.cls)
            handler._events = lambda: None  # type: ignore

        if not {"cls", "id"}.issuperset(kwargs):
            kwargs.pop("cls", None)
            kwargs.pop("id", None)
            raise TypeError(f"invalid keyword argument {next(iter(kwargs))}")

        elif "id" not in kwargs:
            for id in self._handlers:
                self.unregister(handler, id=id, **kwargs)
            return

        elif "cls" not in kwargs:
            id = kwargs.pop("id")

            for cls, fns in self._handlers[id].items():
                self.unregister(handler, id=id, cls=cls)

        else:
            id = kwargs.pop("id")
            cls = kwargs.pop("cls")
            fns = self._handlers.get(id, {}).get(cls, [])
            idx = 0
            while idx < len(fns):
                if fns[idx] is handler:
                    del fns[idx]
                else:
                    idx += 1

    def trigger(self, id: HandlerId, msg: Any) -> None:
        """
        Dispatch a message to all handlers.
        """
        trigger = False
        handlers = self._handlers.get(id, {})

        # Type-based dispatch
        # TODO: handle sub-classes
        cls = type(msg)
        for fn in handlers.get(cls, ()):
            try:
                trigger = True
                fn(msg)
            except Exception as exc:
                func = getattr(fn, "__name__", fn)
                self._log(id, f"[{cls}] {func}({msg}) raised {exc}")

        # Generic dispatch for message
        for fn in handlers.get(None, ()):
            try:
                trigger = True
                fn(msg)
            except Exception as exc:
                func = getattr(fn, "__name__", fn)
                self._log(id, f"{func}({msg}) raised {exc}")

        # Dispatch to fallback, but if no fallback is set, use the default
        # logger.
        if not trigger and self.fallback is not None:
            self.fallback(id, msg)
        else:
            self._log(id, msg)

    def _log(self, id: HandlerId, msg: Any) -> None:
        if id not in self.config.log and not self.config.log_all:
            return
        print(id, msg)

    #
    # Standard triggers.
    #
    def log(self, msg) -> None:
        "Log a runtime message. Usually something that a sysadmin will find useful."
        self("log", msg)

    def critical(self, msg) -> None:
        "Log a critical message."
        self("critical", msg)

    def error(self, msg) -> None:
        "Log an error message."
        self("error", msg)

    def warn(self, msg) -> None:
        "Log a warning message."
        self("warning", msg)

    def warning(self, msg) -> None:
        "Log a warning message."
        self("warning", msg)

    def info(self, msg) -> None:
        "Log an info message."
        self("info", msg)

    def debug(self, msg) -> None:
        "Log a debug message."
        self("debug", msg)

    def show(self, msg) -> None:
        "Trigger an message that will be shown to the user."
        self("show", msg)

    #
    # Utilities
    #
    def disable_fallback(self):
        """
        Disable the fallback handler.

        Same as self.fallback = None, but some people feel uncomfortable with
        dirrectly changing attributes.
        """
        self.fallback = None

    def set_strict_fallback(self, *, warn=False, log=False):
        """
        Raise a RuntimeError if no handlers are registered.

        >>> get_messages().set_strict_fallback()
        """

        def fn(id, _):
            exc = exception(f'No handlers registered for "{id}"')
            if log:
                self._log(id, exc)
            else:
                raise exc

        exception = RuntimeWarning if warn else RuntimeError
        self.fallback = fn

    def set_chain_fallback(self, broker: "Events"):
        """
        Chain messages to messages instance as a fallback.

        >>> get_messages().set_chain_fallback(get_messages())
        """
        end = broker
        while isinstance(broker.fallback, Events):
            end = broker.fallback
        if end is self:
            raise ValueError("Cannot chain to self")
        self.fallback = broker

    def set_log_fallback(self, level: str = "info", logger: LoggerLike = None):
        """
        As a fallback, log messages to logger.

        Show messages at least as severe as the provided level.

        Args:
            level:
                The level to log messages at.
            logger:
                A string used to fetch a logger from the logging module
                or a logger instance (or anything with the used logging
                methods).
        """
        from logging import getLogger

        if logger is None:
            logger = getLogger()
        if isinstance(logger, str):
            logger = getLogger(logger)

        level_idx = LOG_LEVELS.index(level)

        def fallback(id, msg):
            try:
                idx = LOG_LEVELS.index(id)
            except IndexError:
                return

            if idx >= level_idx:
                method = getattr(logger, id)
                method(msg)

        self.fallback = fallback


@dataclass(frozen=True)
class Handler:
    """
    Wraps an event handler function.

    The event handler can be called in response to an event
    """

    _events: ref[Events] = field(repr=False)
    func: MessageHandler
    id: HandlerId
    cls: type | None = None

    @property
    def events(self) -> Events | None:
        "The owning events instance."
        return self._events()

    def __post_init__(self):
        if not isinstance(self._events, ref):
            object.__setattr__(self, "_events", ref(self._events))

    def __getattr__(self, name):
        return getattr(self.func, name)

    def unregister(self) -> None:
        """
        Unregister the handler from message broker.
        """
        events = self._events()
        if events is not None:
            events.unregister(self)

    def trigger(self, id, msg, globally=False) -> None:
        """
        Trigger an event locally and run the handler if it passes conditions it was
        registered.

        If globally is True, the event is triggered to all handlers.
        """
        if globally and self.events is not None:
            self.events(id, msg)
        elif globally:
            pass
        elif self.id == id and (self.cls is None or isinstance(msg, self.cls)):
            self.run(msg)

    def run(self, msg) -> None:
        """
        Run the handler function unconditionally, independently of how it was
        registered on the event broker.

        Note:
            Use the ``.func`` attribute to direct access to the inner function.
        """
        return self.func(msg)  # type: ignore


@lru_cache(1)
def get_messages() -> "Events":
    """
    Get the global message broker.
    """
    return Events()
