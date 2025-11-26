from pathlib import Path
from fitparse import FitFile
from typing import Any
from dotenv import load_dotenv
from os import getenv

load_dotenv(encoding='utf-8')
FIT_FOLDER = Path(getenv('FIT_FOLDER') or Path.cwd())

counts = {}
missing = 0
total = 0

for p in sorted(FIT_FOLDER.iterdir()):
    if not p.is_file() or p.suffix.lower() != '.fit':
        continue
    if p.parent.name in ('_junk', '_failed'):
        continue
    total += 1
    try:
        fit = FitFile(str(p))
        msgs = list(fit.get_messages('file_id'))
        if not msgs:
            missing += 1
            continue
        m = msgs[0]
        try:
            # Guard access because some fitparse versions may yield dict-like
            # message objects in certain environments; use getattr or dict
            if hasattr(m, "get_value"):
                t = getattr(m, "get_value")('time_created')  # type: Any
            elif isinstance(m, dict):
                t = m.get('time_created')  # type: Any
            else:
                t = None
        except Exception:
            t = None
        if t is None:
            missing += 1
            continue
        year = t.year
        counts[year] = counts.get(year, 0) + 1
    except Exception:
        missing += 1

print(f"Top-level .fit total: {total}")
print('By year:')
for y in sorted(counts):
    print(y, counts[y])
print('Missing:', missing)
