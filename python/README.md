# Python replica (same API as Go)

SQLite-backed records API. Uses same DB file as Go: `db/data/records.db`.

## Run

```bash
cd python
pip install -r requirements.txt
python main.py
```

## Endpoints

- `POST /api/v1/health` → `{"ok": true}`
- `GET /api/v1/records/{id}` → record or 400
- `POST /api/v1/records/{id}` → create/update (body: string keys, string or null)
