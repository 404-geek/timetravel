"""
api/helpers.py — shared response helpers mirroring the Go api/helpers.go.
"""

import json
import logging

from flask import Response

logger = logging.getLogger(__name__)


def write_json(data: dict | list, status: int = 200) -> Response:
    """Return a JSON response with the correct Content-Type header."""
    body = json.dumps(data)
    return Response(body + "\n", status=status, mimetype="application/json; charset=utf-8")


def write_error(message: str, status: int) -> Response:
    """Return a JSON error response and log the message."""
    logger.error("response errored: %s", message)
    return write_json({"error": message}, status)
