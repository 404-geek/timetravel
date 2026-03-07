"""
server.py — entry point for the Python timetravel server.

Usage:
    python server.py              # listens on 127.0.0.1:8000
    DB_PATH=my.db python server.py
"""

import logging
import os
import sys

# Allow imports from the python/ directory without an install step.
sys.path.insert(0, os.path.dirname(__file__))

from flask import Flask

from api.v1 import create_v1_blueprint
from api.v2 import create_v2_blueprint
from service.record import SQLiteRecordService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_app(db_path: str = "records.db") -> Flask:
    """
    Create and configure the Flask application.

    Accepts an optional *db_path* so tests can pass ``":memory:"`` or a
    temporary file without touching the real database.
    """
    svc = SQLiteRecordService(db_path)

    app = Flask(__name__)
    app.register_blueprint(create_v1_blueprint(svc))
    app.register_blueprint(create_v2_blueprint(svc))

    return app


if __name__ == "__main__":
    db_path = os.environ.get("DB_PATH", "records.db")
    logger.info("using database: %s", db_path)

    app = create_app(db_path)
    logger.info("listening on 127.0.0.1:8000")
    app.run(host="127.0.0.1", port=8000)
