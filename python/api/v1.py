"""
api/v1.py — /api/v1 Blueprint mirroring the Go api package.

Routes:
    POST /api/v1/health
    GET  /api/v1/records/<id>
    POST /api/v1/records/<id>
"""

import logging

from flask import Blueprint, request

from api.helpers import write_error, write_json
from entity.record import Record
from service.record import (
    RecordDoesNotExistError,
    SQLiteRecordService,
)

logger = logging.getLogger(__name__)


def create_v1_blueprint(svc: SQLiteRecordService) -> Blueprint:
    bp = Blueprint("v1", __name__, url_prefix="/api/v1")

    @bp.route("/health", methods=["POST"])
    def health():
        return write_json({"ok": True})

    @bp.route("/records/<id_str>", methods=["GET"])
    def get_record(id_str: str):
        try:
            record_id = int(id_str)
        except ValueError:
            return write_error("invalid id; id must be a positive number", 400)

        if record_id <= 0:
            return write_error("invalid id; id must be a positive number", 400)

        try:
            record = svc.get_record(record_id)
        except RecordDoesNotExistError:
            return write_error(f"record of id {record_id} does not exist", 400)
        except Exception:
            logger.exception("unexpected error in GET /api/v1/records/%s", id_str)
            return write_error("internal error", 500)

        return write_json(record.to_dict())

    @bp.route("/records/<id_str>", methods=["POST"])
    def post_record(id_str: str):
        try:
            record_id = int(id_str)
        except ValueError:
            return write_error("invalid id; id must be a positive number", 400)

        if record_id <= 0:
            return write_error("invalid id; id must be a positive number", 400)

        body = request.get_json(silent=True, force=True)
        if body is None or not isinstance(body, dict):
            return write_error("invalid input; could not parse json", 400)

        # Validate that all values are strings or null
        for key, val in body.items():
            if val is not None and not isinstance(val, str):
                return write_error("invalid input; values must be strings or null", 400)

        try:
            svc.get_record(record_id)
            record_exists = True
        except RecordDoesNotExistError:
            record_exists = False

        try:
            if record_exists:
                record = svc.update_record(record_id, body)
            else:
                # Exclude null-value keys on initial creation
                record_map = {k: v for k, v in body.items() if v is not None}
                record = Record(id=record_id, data=record_map)
                svc.create_record(record)
        except Exception:
            logger.exception("unexpected error in POST /api/v1/records/%s", id_str)
            return write_error("internal error", 500)

        return write_json(record.to_dict())

    return bp
