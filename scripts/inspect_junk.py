from pathlib import Path
from fitparse import FitFile, FitParseError
import sys

FIT_FOLDER = Path(r"C:\Users\סלע נץ\Documents\garmin")
JUNK = FIT_FOLDER / "_junk"
FAILED = FIT_FOLDER / "_failed"

folders = [(JUNK, "_junk"), (FAILED, "_failed")]

def extract_start(path: Path):
    try:
        fit = FitFile(str(path))
        # Try session/start_time -> activity -> record timestamp
        # Check session messages
        for m in fit.get_messages('session'):
            try:
                if hasattr(m, "get_value"):
                    v = getattr(m, "get_value")('start_time')  # dynamic access
                elif isinstance(m, dict):
                    v = m.get('start_time')
                else:
                    v = None
                if v:
                    return str(v)
            except Exception:
                continue
        # Check activity messages
        for m in fit.get_messages('activity'):
            try:
                if hasattr(m, "get_value"):
                    v = getattr(m, "get_value")('local_timestamp') or getattr(m, "get_value")('timestamp')
                elif isinstance(m, dict):
                    v = m.get('local_timestamp') or m.get('timestamp')
                else:
                    v = None
                if v:
                    return str(v)
            except Exception:
                continue
        # Fallback to first record timestamp
        for m in fit.get_messages('record'):
            try:
                if hasattr(m, "get_value"):
                    v = getattr(m, "get_value")('timestamp')
                elif isinstance(m, dict):
                    v = m.get('timestamp')
                else:
                    v = None
                if v:
                    return str(v)
            except Exception:
                continue
        return 'no-timestamp'
    except FitParseError as e:
        return f'parse-error:{e}'
    except Exception as e:
        return f'error:{e}'

if __name__ == '__main__':
    any_found = False
    for folder, name in folders:
        if not folder.exists():
            print(f"{name}: (folder not found)")
            continue
        fits = sorted(folder.glob('*.fit')) + sorted(folder.glob('*.FIT'))
        print(f"{name}: {len(fits)} .fit files")
        any_found = True
        # sample up to 20 files
        for p in fits[:20]:
            start = extract_start(p)
            print(f"{p.name}\t{start}")
    if not any_found:
        print("No _junk or _failed folders found.")
