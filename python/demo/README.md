# Record timeline demo (optional)

Small UI to showcase **constant-time seek**: load timeline (metadata only), then seek to a version. Cached versions render instantly.

## Run

1. Start the API from the `python/` directory: `uvicorn main:app --reload`
2. Open in browser: **http://127.0.0.1:8000/demo/**

## Usage

- Enter a **Record ID** (e.g. 1) and click **Load timeline** → fetches only version numbers + timestamps (small payload).
- Click a **version row** to seek → fetches that version’s data (or shows from cache if already loaded).
- **Prefetch all** → loads every version’s data into cache; subsequent seeks are instant (no request).

## Remove

To drop the demo later:

1. Delete the `demo/` folder.
2. In `main.py`, remove the `pathlib` import, the `StaticFiles` import, and the block that mounts `/demo` (the 4 lines under “Optional demo UI”).
