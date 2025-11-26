from pathlib import Path
from fitparse import FitFile, FitParseError
from typing import Any
import datetime

FIT_FOLDER = Path(r"C:\Users\סלע נץ\Documents\garmin")
JUNK = FIT_FOLDER / "_junk"

if not JUNK.exists():
    print("_junk folder not found")
    raise SystemExit(0)

counts = {
    'total': 0,
    'with_ts': 0,
    'before_2024': 0,
}

examples_before = []
examples_after = []

cutoff = datetime.datetime(2024,1,1)

def extract_dt(path: Path):
    try:
        fit = FitFile(str(path))
        # Check session start_time
        for m in fit.get_messages('session'):
            try:
                if hasattr(m, "get_value"):
                    v = getattr(m, "get_value")('start_time')  # type: Any
                    if v:
                        return v
            except Exception:
                continue
        # activity
        for m in fit.get_messages('activity'):
            for key in ('local_timestamp', 'timestamp'):
                try:
                    if hasattr(m, "get_value"):
                        v = getattr(m, "get_value")(key)  # type: Any
                        if v:
                            return v
                except Exception:
                    continue
        # records
        for m in fit.get_messages('record'):
            try:
                if hasattr(m, "get_value"):
                    v = getattr(m, "get_value")('timestamp')  # type: Any
                    if v:
                        return v
            except Exception:
                continue
        return None
    except FitParseError:
        return None
    except Exception:
        return None

for p in JUNK.glob('*.fit'):
    counts['total'] += 1
    dt = extract_dt(p)
    if dt is None:
        continue
    counts['with_ts'] += 1
    if isinstance(dt, str):
        try:
            dt = datetime.datetime.fromisoformat(dt)
        except Exception:
            # try common format
            try:
                dt = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            except Exception:
                continue
    if dt < cutoff:
        counts['before_2024'] += 1
        if len(examples_before) < 10:
            examples_before.append((p.name, dt.isoformat()))
    else:
        if len(examples_after) < 10:
            examples_after.append((p.name, dt.isoformat()))

print(f"Total in _junk: {counts['total']}")
print(f"Files with parseable timestamps: {counts['with_ts']}")
print(f"Files before 2024: {counts['before_2024']}")
print('\nExamples before 2024:')
for n,d in examples_before:
    print(n, d)
print('\nExamples after 2024:')
for n,d in examples_after:
    print(n, d)
