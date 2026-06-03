# AGENTS.md

## Cursor Cloud specific instructions

### Repository layout

Application code lives on branch `cursor/proscalper-terminal-670e` (tracked as `origin/cursor/proscalper-terminal-670e`). The default `main` branch is README-only. Check out the feature branch (or this repo’s dev-setup branch) before running services.

| Component | Path | Stack |
|-----------|------|--------|
| Backend | `main.py` | FastAPI + Uvicorn on port **8000** |
| Frontend | `App.jsx` + Vite scaffold (`package.json`, `src/main.jsx`) | React on port **5173** |

### Dependencies

- **Python**: `pip install -r requirements.txt` (installs to `~/.local/bin`; add `export PATH="$HOME/.local/bin:$PATH"` in the shell if `uvicorn` is not found).
- **Node**: `npm install` at repo root.

The VM **update script** runs only those install steps on startup.

### Environment variables

| Variable | Required? | Notes |
|----------|-----------|--------|
| `UPSTOX_ACCESS_TOKEN` | **Yes** for real broker data | Backend calls `authenticate()` on startup; without any token the process exits. A placeholder token lets the API start but broker validation stays in **SAFE MODE** (401 on profile). |
| `UPSTOX_API_KEY`, `UPSTOX_API_SECRET`, OAuth fields | Optional | Alternative auth flows (see `main.py`). |
| `VITE_API_URL` | Optional | Default `http://localhost:8000` |
| `VITE_WS_URL` | Optional | Default `ws://localhost:8000/ws/dashboard` |
| `PORT` | Optional | Backend port (default 8000) |
| `SQLITE_PATH` | Optional | Default `pro_scalper.db` in cwd |

### Running services (use tmux for long-lived processes)

```bash
# Backend
export PATH="$HOME/.local/bin:$PATH"
export UPSTOX_ACCESS_TOKEN="<your-token>"
python3 main.py

# Frontend (separate terminal)
export VITE_API_URL=http://localhost:8000
export VITE_WS_URL=ws://localhost:8000/ws/dashboard
npm run dev
```

Quick check: `curl http://localhost:8000/api/health` should return `"status":"ok"`.

### Lint / tests

No ESLint, pytest, or CI config in the repo. Validation is manual: health endpoint, `npm run build`, and loading the Vite UI.

### Gotchas

- **`GET /api/dashboard` may 500** without a valid Upstox session because the payload includes `collections.deque` objects that FastAPI/Pydantic cannot JSON-serialize. `/api/health` is the reliable smoke test when credentials are missing or invalid.
- Optional ML deps (`xgboost`, `lightgbm`) and `redis` / `psycopg2` are not in `requirements.txt`; the backend skips them if not installed.
- SQLite DB file `pro_scalper.db` is created in the working directory when the engine runs.
