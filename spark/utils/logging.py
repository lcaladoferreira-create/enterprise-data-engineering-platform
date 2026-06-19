import sys
from contextvars import ContextVar

from loguru import logger

# Context variable to store trace_id for structured logging
trace_id: ContextVar[str] = ContextVar("trace_id", default="undefined")


def get_logger(name: str):
    """
    Returns a configured loguru logger with JSON sink and trace_id.
    """
    # Remove default handler
    try:
        logger.remove(0)
    except Exception:
        pass

    # Add JSON sink to stdout
    logger.add(
        sys.stdout,
        format='{"time": "{time:YYYY-MM-DD HH:mm:ss.SSS}", "level": "{level}", "name": "{extra[name]}", "trace_id": "{extra[trace_id]}", "message": "{message}"}',
        level="INFO",
        filter=lambda record: "extra" in record and record["extra"].get("name") == name,
    )

    return logger.bind(name=name, trace_id=trace_id.get())
