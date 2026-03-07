"""
api/v2.py — /api/v2 Blueprint with time-travel versioning.

Routes:
    GET  /api/v2/records/<id>
    GET  /api/v2/records/<id>?version=N
    POST /api/v2/records/<id>
    GET  /api/v2/records/<id>/versions
"""

import logging

from flask import Blueprint, request

from api.helpers import write_error, write_json
from entity.record import Record
from service.record import (
    RecordDoesNotExistError,
    RecordVersionDoesNotExistError,
    SQLiteRecordService,
)

logger = logging.getLogger(__name__)


def create_v2_blueprint(svc: SQLiteRecordService) -> Blueprint:
    bp = Blueprint("v2", __name__, url_prefix="/api/v2")

    @bp.route("/records/<id_str>", methods=["GET"])
    def get_record(id_str: str):
        try:
            record_id = int(id_str)
        except ValueError:
            return write_error("invalid id; id must be a positive number", 400)

        if record_id <= 0:
            return write_error("invalid id; id must be a positive number", 400)

        version = 0
        version_str = request.args.get("version", "")
        if version_str:
            try:
                version = int(version_str)
            except ValueError:
                return write_error("invalid version; version must be a positive number", 400)
            if version <= 0:
                return write_error("invalid version; version must be a positive number", 400)

        try:
            vr = svc.get_versioned_record(record_id, version)
        except RecordDoesNotExistError:
            return write_error(f"record of id {record_id} does not exist", 404)
        except RecordVersionDoesNotExistError:
            return write_error(
                f"record of id {record_id} at version {version} does not exist", 404
            )
        except Exception:
            logger.exception("unexpected error in GET /api/v2/records/%s", id_str)
            return write_error("internal error", 500)

        return write_json(vr.to_dict())

    @bp.route("/records/<id_str>/versions", methods=["GET"])
    def get_record_versions(id_str: str):
        try:
            record_id = int(id_str)
        except ValueError:
            return write_error("invalid id; id must be a positive number", 400)

        if record_id <= 0:
            return write_error("invalid id; id must be a positive number", 400)

        try:
            versions = svc.list_record_versions(record_id)
        except RecordDoesNotExistError:
            return write_error(f"record of id {record_id} does not exist", 404)
        except Exception:
            logger.exception(
                "unexpected error in GET /api/v2/records/%s/versions", id_str
            )
            return write_error("internal error", 500)

        return write_json([v.to_dict() for v in versions])

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
                svc.update_record(record_id, body)
            else:
                record_map = {k: v for k, v in body.items() if v is not None}
                svc.create_record(Record(id=record_id, data=record_map))
        except Exception:
            logger.exception("unexpected error in POST /api/v2/records/%s", id_str)
            return write_error("internal error", 500)

        try:
            vr = svc.get_versioned_record(record_id, version=0)
        except Exception:
            logger.exception(
                "unexpected error fetching versioned record after POST /api/v2/records/%s",
                id_str,
            )
            return write_error("internal error", 500)

        return write_json(vr.to_dict())

    return bp
