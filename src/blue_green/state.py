import json
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_STATE_FILE = _PROJECT_ROOT / "blue_green_state.json"

_EMPTY_STATE = {"active": None, "staging": None, "last_switch": None}


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class StateManager:
    def __init__(self, state_file_path: str = None):
        self._path = Path(state_file_path) if state_file_path else _DEFAULT_STATE_FILE

    def read(self) -> dict:
        if not self._path.exists():
            self._write(_EMPTY_STATE.copy())
            return _EMPTY_STATE.copy()
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            print(f"[blue-green] state file corrompido — recriando: {self._path}")
            self._write(_EMPTY_STATE.copy())
            return _EMPTY_STATE.copy()

    def _write(self, state: dict) -> None:
        self._path.write_text(
            json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def update_staging_downloaded(self, source_month: str) -> None:
        state = self.read()
        state["staging"] = {
            "database": "receita_federal_staging",
            "source_month": source_month,
            "downloaded_at": _now_iso(),
            "processed_at": None,
        }
        self._write(state)

    def update_staging_processed(self) -> None:
        state = self.read()
        if state.get("staging"):
            state["staging"]["processed_at"] = _now_iso()
            self._write(state)

    def promote_staging(self) -> None:
        state = self.read()
        staging = state.get("staging") or {}
        now = _now_iso()
        state["active"] = {
            "database": "receita_federal",
            "source_month": staging.get("source_month"),
            "downloaded_at": staging.get("downloaded_at"),
            "processed_at": staging.get("processed_at"),
            "switched_at": now,
        }
        state["staging"] = None
        state["last_switch"] = now
        self._write(state)

    def get_active(self) -> dict | None:
        return self.read().get("active")

    def get_staging(self) -> dict | None:
        return self.read().get("staging")
